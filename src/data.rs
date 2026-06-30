//! Reading row data from files (Parquet / NDJSON) for ingest and update.

use std::{
    fs::File,
    io::{BufReader, Read, stdin},
    path::Path,
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

/// Read all record batches from a Parquet file.
pub fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading parquet {}", path.display()))?
        .build()?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .context("decoding parquet batches")
}

/// Read the Arrow schema of a Parquet file without decoding any rows.
pub fn parquet_schema(path: &Path) -> Result<SchemaRef> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading parquet {}", path.display()))?;
    Ok(builder.schema().clone())
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

/// Read rows in the requested format. Parquet needs a file (random access);
/// NDJSON accepts `-`/absent for stdin and needs the target schema.
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
