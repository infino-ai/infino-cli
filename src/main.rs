//! infino — command-line interface for the infino retrieval engine.
//!
//! Each command opens a connection to a storage location (`--uri`) and calls
//! the public `infino` API, mirroring the Node/Python SDK surface. Read and
//! query commands render Arrow rows.

mod data;
mod output;
mod predicate;
mod schema;
mod search;
mod skills;

use std::{io::stderr, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use infino::{CompactionSettings, Connection, MutationStats, OptimizeOptions, Supertable, connect};
use tracing_subscriber::EnvFilter;

use crate::{
    data::Format,
    output::{OutputFormat, render},
    search::{Bm25Args, ExactMatchArgs, TokenMatchArgs, VectorArgs},
    skills::SkillsCommand,
};

/// Log filter when `RUST_LOG` is unset (quiet by default).
const DEFAULT_LOG_FILTER: &str = "warn";

#[derive(Parser)]
#[command(
    name = "infino",
    version,
    about = "SQL, full-text, and vector search over object storage"
)]
struct Cli {
    /// Storage location: `memory://`, `file://<path>`, `s3://<bucket>/<prefix>`,
    /// or `az://<container>/<prefix>`. May also be set via `INFINO_URI`.
    #[arg(long, global = true, env = "INFINO_URI")]
    uri: Option<String>,

    /// Output format for row-returning commands.
    #[arg(long, value_enum, global = true, default_value = "table")]
    output: OutputFormat,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List the tables at a storage location.
    Tables,
    /// Show a table's schema.
    Describe {
        /// Table name.
        table: String,
    },
    /// Run a SQL query and print the resulting rows.
    Query {
        /// SQL text, e.g. "SELECT * FROM docs LIMIT 10".
        sql: String,
    },
    /// Keyword (BM25) ranked search.
    Bm25Search(Bm25Args),
    /// Vector similarity (kNN) search.
    VectorSearch(VectorArgs),
    /// Unranked token match over an FTS column.
    TokenMatch(TokenMatchArgs),
    /// Unranked exact-value match.
    ExactMatch(ExactMatchArgs),
    /// Create a table and load its initial rows (a table is not durable until
    /// its first commit). Schema + data come from a Parquet file
    /// (`--from-parquet`), or from a YAML schema (`--schema`) plus `--file`.
    CreateTable {
        /// Table name.
        name: String,
        /// Parquet file: infers the schema AND loads it as the initial rows.
        #[arg(long, conflicts_with = "schema")]
        from_parquet: Option<PathBuf>,
        /// YAML schema (a list of `{name, type, nullable?}`); pair with `--file`.
        #[arg(long)]
        schema: Option<PathBuf>,
        /// Initial rows to load (required with `--schema`; `-`/omitted = stdin).
        #[arg(long)]
        file: Option<PathBuf>,
        /// Encoding of `--file`.
        #[arg(long, value_enum, default_value = "ndjson")]
        format: Format,
        /// Mark a column as full-text (BM25) indexed (repeatable).
        #[arg(long)]
        fts: Vec<String>,
        /// Mark a column as vector indexed (repeatable).
        #[arg(long, value_name = "COL:DIM:NCENT:METRIC")]
        vector: Vec<String>,
    },
    /// Append rows from a Parquet file (`--file`) or NDJSON (`--file` / stdin).
    Ingest {
        /// Table name.
        table: String,
        /// Input file (`-` or omitted = stdin, NDJSON only).
        #[arg(long)]
        file: Option<PathBuf>,
        /// Input encoding.
        #[arg(long, value_enum, default_value = "parquet")]
        format: Format,
    },
    /// Replace rows matching `--where` with the values in `--set-file`.
    Update {
        /// Table name.
        table: String,
        /// SQL predicate selecting rows to replace.
        #[arg(long = "where", value_name = "PREDICATE")]
        predicate: String,
        /// File holding the replacement values.
        #[arg(long)]
        set_file: PathBuf,
        /// Encoding of `--set-file`.
        #[arg(long, value_enum, default_value = "ndjson")]
        set_format: Format,
    },
    /// Delete rows matching `--where`.
    Delete {
        /// Table name.
        table: String,
        /// SQL predicate selecting rows to delete.
        #[arg(long = "where", value_name = "PREDICATE")]
        predicate: String,
    },
    /// Compact a table.
    Optimize {
        /// Table name.
        table: String,
        /// Build-time memory budget, in MB.
        #[arg(long)]
        max_memory_mb: Option<u64>,
        /// Only compact superfiles below this fill percent (0–100).
        #[arg(long)]
        min_fill_percent: Option<u8>,
        /// Target merged-superfile size, in MB.
        #[arg(long)]
        target_superfile_size_mb: Option<u64>,
    },
    /// Install or check the bundled agent skills (Claude Code / Cursor).
    Skills {
        #[command(subcommand)]
        command: SkillsCommand,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER)),
        )
        .with_writer(stderr)
        .init();

    run(Cli::parse())
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Tables => {
            let conn = open(&cli.uri)?;
            let names = conn.list_tables().context("failed to list tables")?;
            if names.is_empty() {
                println!("(no tables)");
            }
            for name in names {
                println!("{name}");
            }
        }
        Command::Describe { table } => {
            let handle = open_table(&cli.uri, &table)?;
            for field in handle.schema().fields() {
                println!("{}\t{}", field.name(), field.data_type());
            }
        }
        Command::Query { sql } => {
            let conn = open(&cli.uri)?;
            let batches = conn.query_sql(&sql).context("query failed")?;
            render(cli.output, &batches)?;
        }
        Command::Bm25Search(args) => {
            let table = open_table(&cli.uri, &args.table)?;
            render(cli.output, &search::bm25(&table, &args)?)?;
        }
        Command::VectorSearch(args) => {
            let table = open_table(&cli.uri, &args.table)?;
            render(cli.output, &search::vector(&table, &args)?)?;
        }
        Command::TokenMatch(args) => {
            let table = open_table(&cli.uri, &args.table)?;
            render(cli.output, &search::token_match(&table, &args)?)?;
        }
        Command::ExactMatch(args) => {
            let table = open_table(&cli.uri, &args.table)?;
            render(cli.output, &search::exact_match(&table, &args)?)?;
        }
        Command::CreateTable {
            name,
            from_parquet,
            schema: schema_path,
            file,
            format,
            fts,
            vector,
        } => {
            let conn = open(&cli.uri)?;
            let (table_schema, initial) = match (from_parquet, schema_path) {
                (Some(parquet), None) => {
                    let schema = data::parquet_schema(&parquet)?;
                    (schema, data::read_parquet(&parquet)?)
                }
                (None, Some(yaml)) => {
                    let schema = schema::schema_from_yaml(&yaml)?;
                    let path = file
                        .as_deref()
                        .context("--schema needs initial rows; pass --file <data>")?;
                    let rows = data::read_rows(Some(path), format, schema.clone())?;
                    (schema, rows)
                }
                (None, None) => bail!("provide --from-parquet <file> or --schema <yaml>"),
                (Some(_), Some(_)) => bail!("--from-parquet and --schema are mutually exclusive"),
            };
            if initial.iter().all(|batch| batch.num_rows() == 0) {
                bail!("create-table needs at least one row to persist the table");
            }
            let spec = schema::index_spec(&fts, &vector)?;
            let handle = conn
                .create_table(&name, table_schema, spec)
                .with_context(|| format!("creating table `{name}`"))?;
            let mut rows = 0;
            for batch in &initial {
                rows += batch.num_rows();
                handle
                    .append(batch)
                    .with_context(|| format!("loading initial rows into `{name}`"))?;
            }
            println!("created table `{name}` with {rows} rows");
        }
        Command::Ingest {
            table,
            file,
            format,
        } => {
            let handle = open_table(&cli.uri, &table)?;
            let batches = data::read_rows(file.as_deref(), format, handle.schema())?;
            let mut rows = 0;
            for batch in &batches {
                rows += batch.num_rows();
                handle
                    .append(batch)
                    .with_context(|| format!("appending to `{table}`"))?;
            }
            println!("ingested {rows} rows into `{table}`");
        }
        Command::Update {
            table,
            predicate,
            set_file,
            set_format,
        } => {
            let handle = open_table(&cli.uri, &table)?;
            let expr = predicate::parse(&handle, &predicate)?;
            let rows = data::read_rows(Some(&set_file), set_format, handle.schema())?;
            let batch = data::concat(rows)?;
            let stats = handle
                .update(expr, &batch)
                .with_context(|| format!("updating `{table}`"))?;
            print_stats(&stats);
        }
        Command::Delete { table, predicate } => {
            let handle = open_table(&cli.uri, &table)?;
            let expr = predicate::parse(&handle, &predicate)?;
            let stats = handle
                .delete(expr)
                .with_context(|| format!("deleting from `{table}`"))?;
            print_stats(&stats);
        }
        Command::Optimize {
            table,
            max_memory_mb,
            min_fill_percent,
            target_superfile_size_mb,
        } => {
            let handle = open_table(&cli.uri, &table)?;
            let mut settings = CompactionSettings::default();
            if let Some(value) = max_memory_mb {
                settings.max_memory_mb = value;
            }
            if let Some(value) = min_fill_percent {
                settings.min_fill_percent = value;
            }
            if let Some(value) = target_superfile_size_mb {
                settings.target_superfile_size_mb = value;
            }
            handle
                .optimize(&OptimizeOptions::compact(settings))
                .with_context(|| format!("optimizing `{table}`"))?;
            println!("optimized `{table}`");
        }
        Command::Skills { command } => skills::run(&command)?,
    }
    Ok(())
}

/// Open a connection to the storage location, or explain how to provide one.
fn open(uri: &Option<String>) -> Result<Connection> {
    let uri = uri
        .as_deref()
        .context("no storage location — pass --uri or set INFINO_URI (e.g. file://./data)")?;
    connect(uri).with_context(|| format!("could not open storage at `{uri}`"))
}

/// Open a table handle at the storage location.
fn open_table(uri: &Option<String>, table: &str) -> Result<Supertable> {
    open(uri)?
        .open_table(table)
        .with_context(|| format!("failed to open table `{table}`"))
}

/// Print the row counts a mutation reported.
fn print_stats(stats: &MutationStats) {
    println!(
        "matched {}, tombstoned {}, not found {}",
        stats.matched(),
        stats.n_tombstoned(),
        stats.n_not_found()
    );
}
