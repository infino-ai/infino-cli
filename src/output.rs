//! Rendering Arrow rows in the format the user asked for.

use std::io::stdout;

use anyhow::Result;
use arrow::{
    array::RecordBatch, csv::WriterBuilder, json::LineDelimitedWriter,
    util::pretty::pretty_format_batches,
};
use clap::ValueEnum;

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum OutputFormat {
    /// Aligned, human-readable table.
    Table,
    /// One JSON object per row (newline-delimited).
    Json,
    /// Comma-separated values with a header row.
    Csv,
}

/// Print a result set in the chosen format.
pub fn render(format: OutputFormat, batches: &[RecordBatch]) -> Result<()> {
    match format {
        OutputFormat::Table => {
            if batches.iter().all(|b| b.num_rows() == 0) {
                println!("(no rows)");
            } else {
                println!("{}", pretty_format_batches(batches)?);
            }
        }
        OutputFormat::Json => {
            let mut writer = LineDelimitedWriter::new(stdout().lock());
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        OutputFormat::Csv => {
            let mut writer = WriterBuilder::new()
                .with_header(true)
                .build(stdout().lock());
            for batch in batches {
                writer.write(batch)?;
            }
        }
    }
    Ok(())
}
