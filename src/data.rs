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
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use arrow::{
    array::RecordBatch,
    compute::{cast, concat_batches},
    datatypes::{DataType, Field, Schema, SchemaRef},
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

/// Replace top-level `Utf8` fields with `LargeUtf8`.
///
/// Parquet strings decode to `Utf8`, but the engine's full-text index requires
/// `LargeUtf8` (the two differ only in offset width, `i32` vs `i64` — the same
/// strings). Widening the inferred schema lets `--fts` work on ordinary
/// Parquet without the user re-encoding the file.
pub fn widen_utf8(schema: &SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if f.data_type() == &DataType::Utf8 {
                Field::new(f.name(), DataType::LargeUtf8, f.is_nullable())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Cast a batch's columns to `target` where they differ only in string offset
/// width (`Utf8` vs `LargeUtf8`) — the mismatch Parquet introduces against a
/// `LargeUtf8` table schema. Other columns pass through unchanged, and a
/// genuine type or column-count mismatch surfaces at `append`.
fn coerce_string_width(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema().as_ref() == target.as_ref() {
        return Ok(batch.clone());
    }
    if batch.num_columns() != target.fields().len() {
        return Ok(batch.clone());
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, column) in batch.columns().iter().enumerate() {
        let want = target.field(i).data_type();
        let coerced = match (column.data_type(), want) {
            (DataType::Utf8, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8) => {
                cast(column, want).context("adjusting string column width")?
            }
            _ => Arc::clone(column),
        };
        columns.push(coerced);
    }
    RecordBatch::try_new(target.clone(), columns).context("coercing batch to the table schema")
}

/// Concatenate a window's batches into one, coerce it to `target` (string
/// offset width only), then hand it to `sink` and clear the buffer. A
/// single-batch window skips the concat copy.
fn flush_window<F>(window: &mut Vec<RecordBatch>, target: &SchemaRef, sink: &mut F) -> Result<()>
where
    F: FnMut(&RecordBatch) -> Result<()>,
{
    let batch = match window.len() {
        0 => return Ok(()),
        1 => window.pop().expect("len == 1"),
        _ => {
            let schema = window[0].schema();
            let batch = concat_batches(&schema, window.iter()).context("concatenating window")?;
            window.clear();
            batch
        }
    };
    let batch = coerce_string_width(&batch, target)?;
    sink(&batch)
}

/// Stream record batches from one or more Parquet files, grouping them into
/// windows of about `window_bytes` (decoded, in-memory size) and calling `sink`
/// once per window. All files must share one schema. Each window is coerced to
/// `target` for string offset width (Parquet's `Utf8` vs the table's
/// `LargeUtf8`) before `sink`. Returns the total rows streamed. Peak memory is
/// roughly one window, not the whole input.
pub fn stream_parquet<F>(
    paths: &[PathBuf],
    target: &SchemaRef,
    window_bytes: usize,
    mut sink: F,
) -> Result<u64>
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
            window_bytes_acc += batch.get_array_memory_size();
            window.push(batch);
            if window_bytes_acc >= window_bytes {
                flush_window(&mut window, target, &mut sink)?;
                window_bytes_acc = 0;
            }
        }
    }
    flush_window(&mut window, target, &mut sink)?;
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
                flush_window(&mut window, &schema, &mut sink)?;
                window_bytes_acc = 0;
            }
        }
    }
    flush_window(&mut window, &schema, &mut sink)?;
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

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use parquet::arrow::ArrowWriter;

    use super::*;

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
        let total = stream_parquet(&files, &id_body_schema(), 1, |b| {
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
    fn widen_utf8_promotes_only_utf8_fields() {
        let widened = widen_utf8(&id_body_schema());
        assert_eq!(widened.field(0).data_type(), &DataType::Int64, "id kept");
        assert_eq!(
            widened.field(1).data_type(),
            &DataType::LargeUtf8,
            "body widened"
        );
    }

    #[test]
    fn stream_parquet_coerces_utf8_to_large_utf8_target() {
        // Parquet stores `body` as Utf8; the table wants LargeUtf8 (as it would
        // for `--fts body`). Each streamed window must arrive as LargeUtf8.
        let dir = tmpdir("widen");
        write_parquet(
            &dir.join("f.parquet"),
            id_body_schema(),
            &(0..10).collect::<Vec<_>>(),
        );
        let files = resolve_inputs(&[dir]).expect("resolve");
        let target = widen_utf8(&id_body_schema());

        let mut seen = 0u32;
        let total = stream_parquet(&files, &target, 1 << 20, |b| {
            seen += 1;
            assert_eq!(
                b.schema().field(1).data_type(),
                &DataType::LargeUtf8,
                "body coerced to LargeUtf8"
            );
            Ok(())
        })
        .expect("stream");
        assert_eq!(total, 10);
        assert!(seen >= 1);
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
        let err =
            stream_parquet(&files, &id_body_schema(), 1 << 20, |_| Ok(())).expect_err("mismatch");
        assert!(err.to_string().contains("schema"), "got {err}");
    }
}
