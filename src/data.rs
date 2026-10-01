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
    array::{RecordBatch, UInt32Array},
    compute::{concat_batches, take},
    datatypes::SchemaRef,
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
/// Copy a batch into buffers of its own.
///
/// A slice is a view over its parent's buffers, so anything measuring capacity
/// downstream sees the parent. `concat_batches` will not do it: with a single
/// input it returns that input unchanged. Taking every row forces the copy.
fn compact(batch: &RecordBatch) -> Result<RecordBatch> {
    let indices = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None).context("copying a split window"))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns).context("rebuilding a split window")
}

fn flush_window<F>(window: &mut Vec<RecordBatch>, sink: &mut F) -> Result<()>
where
    F: FnMut(&RecordBatch) -> Result<()>,
{
    match window.len() {
        0 => Ok(()),
        1 => {
            let batch = window.pop().expect("len == 1");
            // A slice shares its parent's buffers, so everything downstream --
            // the engine's memory accounting locally, the IPC writer on the way
            // to a hosted table -- would see the whole parent instead of these
            // rows. Compact it. An unsliced batch passes through untouched, so
            // the copy is only paid where a split actually happened.
            if batch_bytes(&batch) < batch.get_array_memory_size() {
                sink(&compact(&batch)?)
            } else {
                sink(&batch)
            }
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
/// Rows decoded per batch. A fixed count is fine because the window is enforced
/// by splitting what comes back, not by predicting it: this only bounds how much
/// the parquet reader materialises at once.
const READER_BATCH_ROWS: usize = 8_192;

/// Bytes a batch occupies, counting only the rows it spans.
///
/// `get_array_memory_size` reports whole buffer capacity, so a slice reports its
/// parent and looks far larger than it is. Every size decision here is made on
/// slices, so it has to be the slice-aware measure.
fn batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| {
            c.to_data()
                .get_slice_memory_size()
                .unwrap_or_else(|_| c.get_array_memory_size())
        })
        .sum()
}

/// Split `batch` into consecutive pieces that each fit `window_bytes`.
///
/// Row width is taken from the batch itself rather than sampled from the front
/// of the file. Document corpora are heavily skewed -- a run of stubs followed
/// by full articles -- so a sample of the first rows can understate the real
/// width by orders of magnitude, and any batch size derived from it is wrong for
/// the rest of the file. Each piece is measured, and halved while it still does
/// not fit, so the bound holds even when rows vary inside one batch.
fn split_to_window(batch: &RecordBatch, window_bytes: usize) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    if rows == 0 {
        return Vec::new();
    }
    if batch_bytes(batch) <= window_bytes {
        return vec![batch.clone()];
    }

    let per_row = (batch_bytes(batch) / rows).max(1);
    let mut pieces = Vec::new();
    let mut offset = 0;
    while offset < rows {
        let mut take = (window_bytes / per_row).clamp(1, rows - offset);
        let mut piece = batch.slice(offset, take);
        while take > 1 && batch_bytes(&piece) > window_bytes {
            take /= 2;
            piece = batch.slice(offset, take);
        }
        offset += take;
        pieces.push(piece);
    }
    pieces
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
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("reading parquet {}", path.display()))?
            .with_batch_size(READER_BATCH_ROWS)
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
            for piece in split_to_window(&batch, window_bytes) {
                // Flush before adding this piece would cross the window, not
                // after. Adding first overshoots by a whole piece every time.
                let size = batch_bytes(&piece);
                if !window.is_empty() && window_bytes_acc + size > window_bytes {
                    flush_window(&mut window, &mut sink)?;
                    window_bytes_acc = 0;
                }
                window_bytes_acc += size;
                window.push(piece);
            }
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

    /// Leading narrow rows followed by wide ones: a document corpus of stubs and
    /// full articles. Any batch size sampled from the front of the file is far
    /// too large for the rest, which is how a 100 MiB window produced a
    /// multi-gigabyte append.
    fn write_skewed_rows(path: &Path, stubs: usize, wide: usize) {
        const WIDE_BYTES: usize = 128 * 1024;
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "body",
            DataType::LargeUtf8,
            false,
        )]));
        let wide_body = "w".repeat(WIDE_BYTES);
        let mut rows: Vec<&str> = vec!["stub"; stubs];
        rows.extend(std::iter::repeat_n(wide_body.as_str(), wide));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(LargeStringArray::from(rows))])
                .expect("batch");
        let file = File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    #[test]
    fn no_window_exceeds_the_requested_size_on_skewed_rows() {
        const WINDOW: usize = 4 * 1024 * 1024;
        const STUBS: usize = 600;
        const WIDE: usize = 400;

        let dir = tmpdir("window-bound-skew");
        let path = dir.join("skew.parquet");
        write_skewed_rows(&path, STUBS, WIDE);

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

        assert_eq!(total, (STUBS + WIDE) as u64, "every row must be streamed");
        assert!(
            oversized.is_empty(),
            "no append may exceed the {WINDOW} B window, got (rows, bytes) {oversized:?}"
        );
        assert!(appends > 1, "wide rows must be split across windows");
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
