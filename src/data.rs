//! Reading row data from files (Parquet / NDJSON) for ingest and update.
//!
//! Bulk ingest is **streamed**: instead of decoding a whole file into memory,
//! [`stream_parquet`] / [`stream_ndjson`] read record batches incrementally and
//! group them into windows of about `window_bytes`, invoking a sink once per
//! window. Each window is one `append` (one commit, one superfile), so peak
//! memory is roughly one window regardless of input size. [`read_rows`] (the
//! whole-file path) is kept for `update`, whose replacement set is small.

use std::{
    fs::File,
    io::{BufReader, Read, stdin},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use arrow::{
    array::RecordBatch, compute::concat_batches, datatypes::SchemaRef,
    json::ReaderBuilder as JsonReaderBuilder,
};
use clap::ValueEnum;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Row input encoding.
#[derive(Copy, Clone, ValueEnum)]
pub enum Format {
    Parquet,
    Ndjson,
}

/// Expand ingest inputs into a concrete, ordered list of files. Each entry may
/// be a file, a directory (all `*.parquet` inside, sorted), or a glob pattern
/// (`*`, `?`, `[…]`). Explicit entries keep their given order; expansions are
/// sorted for determinism. Errors if nothing resolves.
pub fn resolve_inputs(entries: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in entries {
        let text = entry.to_string_lossy();
        if text.contains(['*', '?', '[']) {
            let mut matched = Vec::new();
            for hit in glob::glob(&text).with_context(|| format!("bad glob pattern `{text}`"))? {
                matched.push(hit.with_context(|| format!("reading glob match for `{text}`"))?);
            }
            if matched.is_empty() {
                bail!("no files matched `{text}`");
            }
            matched.sort();
            files.extend(matched);
        } else if entry.is_dir() {
            let mut in_dir: Vec<PathBuf> = std::fs::read_dir(entry)
                .with_context(|| format!("reading directory {}", entry.display()))?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
                })
                .collect();
            if in_dir.is_empty() {
                bail!("no .parquet files in {}", entry.display());
            }
            in_dir.sort();
            files.extend(in_dir);
        } else {
            files.push(entry.clone());
        }
    }
    if files.is_empty() {
        bail!("no input files given");
    }
    Ok(files)
}

/// Read the Arrow schema of a Parquet file without decoding any rows.
pub fn parquet_schema(path: &Path) -> Result<SchemaRef> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading parquet {}", path.display()))?;
    Ok(builder.schema().clone())
}

/// Concatenate a window's batches into one, then hand it to `sink` and clear
/// the buffer. A single-batch window skips the copy.
fn flush_window<F>(window: &mut Vec<RecordBatch>, sink: &mut F) -> Result<()>
where
    F: FnMut(&RecordBatch) -> Result<()>,
{
    match window.len() {
        0 => Ok(()),
        1 => {
            let batch = window.pop().expect("len == 1");
            sink(&batch)
        }
        _ => {
            let schema = window[0].schema();
            let batch = concat_batches(&schema, window.iter()).context("concatenating window")?;
            window.clear();
            sink(&batch)
        }
    }
}

/// Stream record batches from one or more Parquet files, grouping them into
/// windows of about `window_bytes` (decoded, in-memory size) and calling `sink`
/// once per window. All files must share one schema. Returns the total rows
/// streamed. Peak memory is roughly one window, not the whole input.
/// Fraction of the window a single decoded batch is sized to fill, leaving room
/// for the row-width estimate to be a little low.
const BATCH_WINDOW_DIVISOR: usize = 2;

/// Rows per decoded batch when a file's width cannot be measured (no rows).
/// Matches the parquet reader's own default.
const FALLBACK_BATCH_ROWS: usize = 1024;

/// Rows decoded while measuring row width, before the real batch size is known.
const PROBE_BATCH_ROWS: usize = 256;

/// Ceiling on rows per decoded batch. Narrow rows would otherwise produce one
/// enormous batch: the byte bound would still hold, but the row count would be
/// unreasonable to materialise in a single Arrow allocation.
const MAX_BATCH_ROWS: usize = 1_000_000;

/// Rows to decode per batch so that one batch stays inside `window_bytes`.
///
/// The reader's default is a fixed row count, which says nothing about how wide
/// a row is: 1024 rows of a few bytes is trivial, 1024 rows carrying kilobytes
/// of text each is hundreds of megabytes. Without sizing from actual width, a
/// window smaller than one decoded batch cannot be honoured at all.
///
/// Width is measured by decoding one small batch rather than read from parquet's
/// own byte counts, because those describe the *encoded* form. A
/// dictionary-encoded column of repeated values occupies almost nothing on disk
/// and materialises to full width per row in Arrow, so metadata can understate
/// the decoded size by orders of magnitude.
fn rows_per_batch(path: &Path, window_bytes: usize) -> Result<usize> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut probe = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading parquet {}", path.display()))?
        .with_batch_size(PROBE_BATCH_ROWS)
        .build()
        .with_context(|| format!("opening parquet reader for {}", path.display()))?;

    let Some(batch) = probe.next().transpose()? else {
        return Ok(FALLBACK_BATCH_ROWS);
    };
    if batch.num_rows() == 0 {
        return Ok(FALLBACK_BATCH_ROWS);
    }
    let per_row = (batch.get_array_memory_size() / batch.num_rows()).max(1);
    let target = window_bytes / BATCH_WINDOW_DIVISOR;
    Ok((target / per_row).clamp(1, MAX_BATCH_ROWS))
}

pub fn stream_parquet<F>(paths: &[PathBuf], window_bytes: usize, mut sink: F) -> Result<u64>
where
    F: FnMut(&RecordBatch) -> Result<()>,
{
    let mut window: Vec<RecordBatch> = Vec::new();
    let mut window_bytes_acc = 0usize;
    let mut first_schema: Option<SchemaRef> = None;
    let mut total_rows = 0u64;

    for path in paths {
        let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let batch_rows = rows_per_batch(path, window_bytes)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("reading parquet {}", path.display()))?
            .with_batch_size(batch_rows)
            .build()
            .with_context(|| format!("opening parquet reader for {}", path.display()))?;
        for batch in reader {
            let batch =
                batch.with_context(|| format!("decoding parquet batch in {}", path.display()))?;
            match &first_schema {
                None => first_schema = Some(batch.schema()),
                Some(schema) if schema != &batch.schema() => bail!(
                    "parquet schema in {} does not match the first input; all files must share one schema",
                    path.display()
                ),
                _ => {}
            }
            if batch.num_rows() == 0 {
                continue;
            }
            total_rows += batch.num_rows() as u64;
            // Flush before adding this batch would cross the window, not after.
            // Adding first overshoots by a whole batch every time.
            let size = batch.get_array_memory_size();
            if !window.is_empty() && window_bytes_acc + size > window_bytes {
                flush_window(&mut window, &mut sink)?;
                window_bytes_acc = 0;
            }
            window_bytes_acc += size;
            window.push(batch);
        }
    }
    flush_window(&mut window, &mut sink)?;
    Ok(total_rows)
}

/// Stream NDJSON into `window_bytes` windows against a known schema, calling
/// `sink` per window. Reads each path in order, or stdin when `paths` is empty
/// (or an entry is `-`). Returns the total rows streamed.
pub fn stream_ndjson<F>(
    paths: &[PathBuf],
    schema: SchemaRef,
    window_bytes: usize,
    mut sink: F,
) -> Result<u64>
where
    F: FnMut(&RecordBatch) -> Result<()>,
{
    let mut window: Vec<RecordBatch> = Vec::new();
    let mut window_bytes_acc = 0usize;
    let mut total_rows = 0u64;

    // Empty -> stdin. Each path decodes against the caller's schema.
    let sources: Vec<&Path> = if paths.is_empty() {
        vec![Path::new("-")]
    } else {
        paths.iter().map(PathBuf::as_path).collect()
    };
    for source in sources {
        let reader = JsonReaderBuilder::new(schema.clone())
            .build(open_buf(source)?)
            .with_context(|| format!("starting NDJSON reader for {}", source.display()))?;
        for batch in reader {
            let batch = batch.context("decoding NDJSON batch")?;
            if batch.num_rows() == 0 {
                continue;
            }
            total_rows += batch.num_rows() as u64;
            window_bytes_acc += batch.get_array_memory_size();
            window.push(batch);
            if window_bytes_acc >= window_bytes {
                flush_window(&mut window, &mut sink)?;
                window_bytes_acc = 0;
            }
        }
    }
    flush_window(&mut window, &mut sink)?;
    Ok(total_rows)
}

/// Read all record batches from a Parquet file (whole-file; used by `update`).
pub fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading parquet {}", path.display()))?
        .build()?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .context("decoding parquet batches")
}

/// Read newline-delimited JSON into batches against a known schema (`-` = stdin).
pub fn read_ndjson(path: &Path, schema: SchemaRef) -> Result<Vec<RecordBatch>> {
    let reader = JsonReaderBuilder::new(schema)
        .build(open_buf(path)?)
        .context("starting NDJSON reader")?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .context("decoding NDJSON batches")
}

/// Read rows in the requested format (whole-file; used by `update`). Parquet
/// needs a file (random access); NDJSON accepts `-`/absent for stdin and needs
/// the target schema.
pub fn read_rows(
    path: Option<&Path>,
    format: Format,
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    match format {
        Format::Parquet => {
            let path = path.context("--file is required for parquet")?;
            read_parquet(path)
        }
        Format::Ndjson => read_ndjson(path.unwrap_or_else(|| Path::new("-")), schema),
    }
}

/// Collapse batches into one (the mutation API takes a single `RecordBatch`).
pub fn concat(batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    match batches.len() {
        0 => bail!("no rows provided"),
        1 => Ok(batches.into_iter().next().expect("len == 1")),
        _ => {
            let schema = batches[0].schema();
            Ok(concat_batches(&schema, &batches)?)
        }
    }
}

fn open_buf(path: &Path) -> Result<BufReader<Box<dyn Read>>> {
    let inner: Box<dyn Read> = if path.as_os_str() == "-" {
        Box::new(stdin())
    } else {
        Box::new(File::open(path).with_context(|| format!("opening {}", path.display()))?)
    };
    Ok(BufReader::new(inner))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::LargeStringArray;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;

    /// A parquet file of deliberately wide rows in one row group: the shape that
    /// exposed the window being unenforceable. Each row carries ~8 KiB of text,
    /// so the reader's default of 1024 rows per batch decodes to ~8 MiB
    /// whatever window the caller asked for.
    fn write_wide_rows(path: &Path, rows: usize) {
        const TEXT_BYTES: usize = 8 * 1024;
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "body",
            DataType::LargeUtf8,
            false,
        )]));
        let body = "x".repeat(TEXT_BYTES);
        let column = LargeStringArray::from(vec![body.as_str(); rows]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column)]).expect("batch");
        let file = File::create(path).expect("create parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    /// Unique text per row, so nothing dictionary-encodes: the profile of real
    /// document data, where the encoded and decoded sizes are closer together
    /// but a row is still far wider than the reader's fixed row count assumes.
    fn write_unique_wide_rows(path: &Path, rows: usize) {
        const TEXT_BYTES: usize = 8 * 1024;
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::LargeUtf8, false),
        ]));
        let bodies: Vec<String> = (0..rows)
            .map(|i| format!("{i:0>width$}", width = TEXT_BYTES))
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                Arc::new(LargeStringArray::from(
                    bodies.iter().map(String::as_str).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch");
        let file = File::create(path).expect("create parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    #[test]
    fn no_window_exceeds_the_requested_size_on_unique_rows() {
        const WINDOW: usize = 1024 * 1024;
        const ROWS: usize = 4_000;

        let dir = tmpdir("window-bound-unique");
        let path = dir.join("unique.parquet");
        write_unique_wide_rows(&path, ROWS);

        let mut oversized = Vec::new();
        let mut appends = 0usize;
        let total = stream_parquet(&[path], WINDOW, |batch| {
            appends += 1;
            let size = batch.get_array_memory_size();
            if size > WINDOW {
                oversized.push((batch.num_rows(), size));
            }
            Ok(())
        })
        .expect("stream");

        assert_eq!(total, ROWS as u64);
        assert!(
            oversized.is_empty(),
            "no append may exceed the {WINDOW} B window, got (rows, bytes) {oversized:?}"
        );
        assert!(
            appends > 1,
            "a {ROWS}-row file of 8 KiB rows must be split across windows, got {appends} append(s)"
        );
    }

    #[test]
    fn no_window_exceeds_the_requested_size() {
        // The window is the whole point of streaming ingest: it bounds peak
        // memory locally, and it keeps a hosted append inside the service's
        // per-request budget. Grouping whole decoded batches is not enough,
        // because one decoded batch can already be larger than the window, and
        // then the window can only be exceeded, never honoured.
        const WINDOW: usize = 1024 * 1024;
        const ROWS: usize = 8_000;

        let dir = tmpdir("window-bound");
        let path = dir.join("wide.parquet");
        write_wide_rows(&path, ROWS);

        let mut oversized = Vec::new();
        let mut seen_rows = 0u64;
        let total = stream_parquet(&[path], WINDOW, |batch| {
            seen_rows += batch.num_rows() as u64;
            let size = batch.get_array_memory_size();
            if size > WINDOW {
                oversized.push((batch.num_rows(), size));
            }
            Ok(())
        })
        .expect("stream");

        assert_eq!(total, ROWS as u64, "every row must be streamed");
        assert_eq!(seen_rows, ROWS as u64, "the sink must see every row");
        assert!(
            oversized.is_empty(),
            "no append may exceed the {WINDOW} B window, got (rows, bytes) {oversized:?}"
        );
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("infino-cli-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create tempdir");
        dir
    }

    fn id_body_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
        ]))
    }

    fn write_parquet(path: &Path, schema: SchemaRef, ids: &[i64]) {
        let bodies: Vec<String> = ids.iter().map(|i| format!("row {i}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(bodies)),
            ],
        )
        .expect("batch");
        let file = File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    #[test]
    fn resolve_expands_directory_sorted_parquet_only() {
        let dir = tmpdir("dir");
        write_parquet(&dir.join("b.parquet"), id_body_schema(), &[1]);
        write_parquet(&dir.join("a.parquet"), id_body_schema(), &[2]);
        std::fs::write(dir.join("notes.txt"), "ignore").expect("write txt");

        let files = resolve_inputs(std::slice::from_ref(&dir)).expect("resolve");
        assert_eq!(files.len(), 2, "only .parquet files");
        assert!(files[0].ends_with("a.parquet"), "sorted: {files:?}");
        assert!(files[1].ends_with("b.parquet"), "sorted: {files:?}");
    }

    #[test]
    fn resolve_errors_on_empty_directory() {
        let dir = tmpdir("empty");
        let err = resolve_inputs(&[dir]).expect_err("no parquet");
        assert!(err.to_string().contains("no .parquet"), "got {err}");
    }

    #[test]
    fn stream_parquet_counts_all_rows_across_files() {
        let dir = tmpdir("stream");
        write_parquet(
            &dir.join("f1.parquet"),
            id_body_schema(),
            &(0..100).collect::<Vec<_>>(),
        );
        write_parquet(
            &dir.join("f2.parquet"),
            id_body_schema(),
            &(100..250).collect::<Vec<_>>(),
        );
        let files = resolve_inputs(&[dir]).expect("resolve");

        // A 1-byte window forces a flush after every batch, so peak memory is
        // one batch and we see multiple windows.
        let mut windows = 0u32;
        let mut rows = 0u64;
        let total = stream_parquet(&files, 1, |b| {
            windows += 1;
            rows += b.num_rows() as u64;
            Ok(())
        })
        .expect("stream");
        assert_eq!(total, 250);
        assert_eq!(rows, 250);
        assert!(
            windows >= 2,
            "tiny window should flush per batch, got {windows}"
        );
    }

    #[test]
    fn stream_parquet_rejects_schema_mismatch() {
        let dir = tmpdir("mismatch");
        write_parquet(&dir.join("a.parquet"), id_body_schema(), &[1]);
        // A second file with a different schema.
        let other = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(other.clone(), vec![Arc::new(Int64Array::from(vec![9i64]))])
                .expect("batch");
        let file = File::create(dir.join("b.parquet")).expect("create");
        let mut writer = ArrowWriter::try_new(file, other, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        let files = resolve_inputs(&[dir]).expect("resolve");
        let err = stream_parquet(&files, 1 << 20, |_| Ok(())).expect_err("mismatch");
        assert!(err.to_string().contains("schema"), "got {err}");
    }
}
