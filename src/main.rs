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

use std::{io::stderr, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use infino::{
    CompactionSettings, ConnectOptions, Connection, GcReport, MutationStats, OptimizeOptions,
    Supertable, connect_with,
};
use tracing_subscriber::EnvFilter;

use crate::{
    data::Format,
    output::{OutputFormat, render},
    search::{Bm25Args, CountArgs, ExactMatchArgs, HybridArgs, TokenMatchArgs, VectorArgs},
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
    /// `az://<container>/<prefix>`, `gs://<bucket>/<prefix>`, or a hosted
    /// `https://<host>/<database>`. May also be set via `INFINO_URI`.
    #[arg(long, global = true, env = "INFINO_URI")]
    uri: Option<String>,

    /// Storage credential/config as `KEY=VALUE` (repeatable). Keys are
    /// object_store's config strings (`aws_access_key_id`,
    /// `aws_secret_access_key`, `aws_region`, `aws_endpoint`, `aws_allow_http`,
    /// `azure_storage_account_name`, `google_service_account`, …). This mirrors
    /// the `storage_options` map on the Node and Python bindings. Omit for
    /// local backends or ambient cloud identity (IAM role / managed identity).
    #[arg(long = "storage-option", global = true, value_name = "KEY=VALUE")]
    storage_option: Vec<String>,

    /// API key for a hosted (`https://<host>/<database>`) connection, sent as a
    /// bearer credential. Ignored by local backends.
    #[arg(long, global = true, env = "INFINO_API_KEY", hide_env_values = true)]
    api_key: Option<String>,

    /// Probe the storage backend at connect so bad credentials or an
    /// unreachable endpoint fail immediately, not on the first query.
    #[arg(long, global = true)]
    validate: bool,

    /// Output format for row-returning commands.
    #[arg(long, value_enum, global = true, default_value = "table")]
    output: OutputFormat,

    /// Ingest window size in MiB. Bulk ingest (`create-table` / `ingest`) is
    /// streamed and committed in chunks of about this size, so peak memory is
    /// bounded regardless of input size.
    #[arg(long, global = true, default_value_t = 256)]
    batch_size_mb: u64,

    /// Local disk-cache directory for durable (non-`memory://`) backends.
    #[arg(long, global = true, env = "INFINO_CACHE_DIR")]
    cache_dir: Option<PathBuf>,

    /// Disable the local disk cache and hold every read superfile resident
    /// in memory for the connection's lifetime. Useful for short-lived
    /// queries against a small dataset; unbounded for large ingests.
    #[arg(long, global = true)]
    no_disk_cache: bool,

    /// Disk-cache byte budget in MiB (per table)
    #[arg(long, global = true)]
    cache_budget_mb: Option<u64>,

    #[command(subcommand)]
    command: Command,
}

fn default_cache_dir() -> PathBuf {
    std::env::temp_dir().join("infino-cli-cache")
}

#[derive(Subcommand)]
enum Command {
    /// Provision the database named in a hosted (`https://<host>/<database>`)
    /// URI. A no-op for local and object-storage backends, which create their
    /// catalog lazily on first write.
    CreateDatabase,
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
    /// Hybrid BM25 + vector search, fused with reciprocal-rank fusion.
    HybridSearch(HybridArgs),
    /// Unranked token match over an FTS column.
    TokenMatch(TokenMatchArgs),
    /// Unranked exact-value match.
    ExactMatch(ExactMatchArgs),
    /// Count rows matching a keyword query, without fetching them.
    Count(CountArgs),
    /// Create a table and load its initial rows (a table is not durable until
    /// its first commit). Schema + data come from a Parquet file
    /// (`--from-parquet`), or from a YAML schema (`--schema`) plus `--file`.
    CreateTable {
        /// Table name.
        name: String,
        /// Parquet input: infers the schema AND loads it as the initial rows.
        /// Repeatable; each value may be a file, a directory (all `*.parquet`
        /// inside), or a quoted glob (e.g. `'data/*.parquet'`).
        #[arg(long, num_args = 1.., conflicts_with = "schema")]
        from_parquet: Vec<PathBuf>,
        /// YAML schema (a list of `{name, type, nullable?}`); pair with `--file`.
        #[arg(long)]
        schema: Option<PathBuf>,
        /// Initial rows to load (required with `--schema`). Repeatable; `-` or
        /// omitted reads NDJSON from stdin.
        #[arg(long, num_args = 1..)]
        file: Vec<PathBuf>,
        /// Encoding of `--file`.
        #[arg(long, value_enum, default_value = "ndjson")]
        format: Format,
        /// Mark a column as full-text (BM25) indexed (repeatable).
        #[arg(long)]
        fts: Vec<String>,
        /// Mark a column as vector indexed (repeatable). IVF centroid count
        /// is derived from the data at build time, not declared here.
        #[arg(long, value_name = "COL:DIM:METRIC")]
        vector: Vec<String>,
    },
    /// Append rows from Parquet (`--file`) or NDJSON (`--file` / stdin).
    Ingest {
        /// Table name.
        table: String,
        /// Input. Repeatable; each value may be a file, a directory (all
        /// `*.parquet` inside), or a quoted glob. `-` or omitted reads NDJSON
        /// from stdin.
        #[arg(long, num_args = 1..)]
        file: Vec<PathBuf>,
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
    /// Reclaim orphaned storage objects left by compaction or interrupted
    /// writes. Requires durable storage.
    Gc {
        /// Table name.
        table: String,
        /// Only delete objects older than this many seconds — a safety window
        /// against racing readers or writers.
        #[arg(long, default_value_t = 0.0)]
        older_than_secs: f64,
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
    let opts = connect_options(
        &cli.storage_option,
        cli.api_key.as_deref(),
        cli.validate,
        cli.no_disk_cache,
        cli.cache_dir.as_deref(),
        cli.cache_budget_mb,
    )?;
    match cli.command {
        Command::CreateDatabase => {
            let conn = open(&opts, &cli.uri)?;
            conn.create_database()
                .context("failed to create database")?;
            println!("database ready");
        }
        Command::Tables => {
            let conn = open(&opts, &cli.uri)?;
            let names = conn.list_tables().context("failed to list tables")?;
            if names.is_empty() {
                println!("(no tables)");
            }
            for name in names {
                println!("{name}");
            }
        }
        Command::Describe { table } => {
            let handle = open_table(&opts, &cli.uri, &table)?;
            for field in handle.schema().fields() {
                println!("{}\t{}", field.name(), field.data_type());
            }
        }
        Command::Query { sql } => {
            let conn = open(&opts, &cli.uri)?;
            let batches = conn.query_sql(&sql).context("query failed")?;
            render(cli.output, &batches)?;
        }
        Command::Bm25Search(args) => {
            let table = open_table(&opts, &cli.uri, &args.table)?;
            render(cli.output, &search::bm25(&table, &args)?)?;
        }
        Command::VectorSearch(args) => {
            let table = open_table(&opts, &cli.uri, &args.table)?;
            render(cli.output, &search::vector(&table, &args)?)?;
        }
        Command::HybridSearch(args) => {
            let table = open_table(&opts, &cli.uri, &args.table)?;
            render(cli.output, &search::hybrid(&table, &args)?)?;
        }
        Command::TokenMatch(args) => {
            let table = open_table(&opts, &cli.uri, &args.table)?;
            render(cli.output, &search::token_match(&table, &args)?)?;
        }
        Command::ExactMatch(args) => {
            let table = open_table(&opts, &cli.uri, &args.table)?;
            render(cli.output, &search::exact_match(&table, &args)?)?;
        }
        Command::Count(args) => {
            let table = open_table(&opts, &cli.uri, &args.table)?;
            println!("{}", search::count(&table, &args)?);
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
            let conn = open(&opts, &cli.uri)?;
            let window = window_bytes(cli.batch_size_mb);
            let spec = schema::index_spec(&fts, &vector)?;

            // Stream rows into the new table in windows so a large input never
            // has to fit in memory. Each window is one append == one commit.
            let appended: u64 = if !from_parquet.is_empty() {
                let files = data::resolve_inputs(&from_parquet)?;
                let table_schema = data::parquet_schema(&files[0])?;
                let handle = conn
                    .create_table(&name, table_schema, spec)
                    .with_context(|| format!("creating table `{name}`"))?;
                data::stream_parquet(&files, window, |batch| {
                    handle
                        .append(batch)
                        .with_context(|| format!("loading initial rows into `{name}`"))
                })?
            } else if let Some(yaml) = schema_path {
                let table_schema = schema::schema_from_yaml(&yaml)?;
                let handle = conn
                    .create_table(&name, table_schema.clone(), spec)
                    .with_context(|| format!("creating table `{name}`"))?;
                match format {
                    Format::Parquet => {
                        let files = data::resolve_inputs(&file)?;
                        data::stream_parquet(&files, window, |batch| {
                            handle
                                .append(batch)
                                .with_context(|| format!("loading initial rows into `{name}`"))
                        })?
                    }
                    Format::Ndjson => data::stream_ndjson(&file, table_schema, window, |batch| {
                        handle
                            .append(batch)
                            .with_context(|| format!("loading initial rows into `{name}`"))
                    })?,
                }
            } else {
                bail!("provide --from-parquet <input> or --schema <yaml>");
            };

            if appended == 0 {
                bail!("create-table needs at least one row to persist the table");
            }
            println!("created table `{name}` with {appended} rows");
        }
        Command::Ingest {
            table,
            file,
            format,
        } => {
            let handle = open_table(&opts, &cli.uri, &table)?;
            let window = window_bytes(cli.batch_size_mb);
            let appended = match format {
                Format::Parquet => {
                    let files = data::resolve_inputs(&file)?;
                    data::stream_parquet(&files, window, |batch| {
                        handle
                            .append(batch)
                            .with_context(|| format!("appending to `{table}`"))
                    })?
                }
                Format::Ndjson => {
                    let schema = handle.schema();
                    data::stream_ndjson(&file, schema, window, |batch| {
                        handle
                            .append(batch)
                            .with_context(|| format!("appending to `{table}`"))
                    })?
                }
            };
            println!("ingested {appended} rows into `{table}`");
        }
        Command::Update {
            table,
            predicate,
            set_file,
            set_format,
        } => {
            let handle = open_table(&opts, &cli.uri, &table)?;
            let expr = predicate::parse(&handle, &predicate)?;
            let rows = data::read_rows(Some(&set_file), set_format, handle.schema())?;
            let batch = data::concat(rows)?;
            let stats = handle
                .update(expr, &batch)
                .with_context(|| format!("updating `{table}`"))?;
            print_stats(&stats);
        }
        Command::Delete { table, predicate } => {
            let handle = open_table(&opts, &cli.uri, &table)?;
            let expr = predicate::parse(&handle, &predicate)?;
            let stats = handle
                .delete(expr)
                .with_context(|| format!("deleting from `{table}`"))?;
            print_stats(&stats);
        }
        Command::Gc {
            table,
            older_than_secs,
        } => {
            let handle = open_table(&opts, &cli.uri, &table)?;
            let report = handle
                .gc(Duration::from_secs_f64(older_than_secs.max(0.0)))
                .with_context(|| format!("gc on `{table}`"))?;
            print_gc(&report);
        }
        Command::Optimize {
            table,
            max_memory_mb,
            min_fill_percent,
            target_superfile_size_mb,
        } => {
            let handle = open_table(&opts, &cli.uri, &table)?;
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
fn open(opts: &ConnectOptions, uri: &Option<String>) -> Result<Connection> {
    let uri = uri
        .as_deref()
        .context("no storage location — pass --uri or set INFINO_URI (e.g. file://./data)")?;
    connect_with(uri, opts.clone()).with_context(|| format!("could not open storage at `{uri}`"))
}

/// Open a table handle at the storage location.
fn open_table(opts: &ConnectOptions, uri: &Option<String>, table: &str) -> Result<Supertable> {
    open(opts, uri)?
        .open_table(table)
        .with_context(|| format!("failed to open table `{table}`"))
}

/// Build [`ConnectOptions`] from the `--storage-option KEY=VALUE` flags.
///
/// This is the CLI's analog of the `storage_options` map on the Node
/// (`connect(uri, { storageOptions })`) and Python
/// (`connect(uri, storage_options=...)`) bindings: credentials, region, and
/// endpoint are passed explicitly, keyed by `object_store`'s config strings
/// (`aws_*` / `azure_*` / `google_*`). Like the engine and both bindings, the
/// CLI reads no credentials from the environment; an omitted set uses the
/// backend's ambient identity (IAM instance role / managed identity / ADC).
/// An unknown or cross-backend key is rejected by the engine at `connect`.
///
/// The disk cache defaults **on** (`--no-disk-cache` opts out) so a durable
/// backend's read superfiles are bounded on local disk instead of held
/// resident in RAM for the connection's lifetime — `memory://` and hosted
/// (`https://`) backends ignore the cache dir regardless.
fn connect_options(
    overrides: &[String],
    api_key: Option<&str>,
    validate: bool,
    no_disk_cache: bool,
    cache_dir: Option<&std::path::Path>,
    cache_budget_mb: Option<u64>,
) -> Result<ConnectOptions> {
    let mut opts = ConnectOptions::new();
    for kv in overrides {
        let (key, value) = kv
            .split_once('=')
            .with_context(|| format!("--storage-option must be KEY=VALUE, got `{kv}`"))?;
        opts = opts.with_storage_option(key.trim(), value);
    }
    if let Some(key) = api_key {
        opts = opts.with_api_key(key);
    }
    if validate {
        opts = opts.with_validate(true);
    }
    if !no_disk_cache {
        let dir = cache_dir.map_or_else(default_cache_dir, PathBuf::from);
        opts = opts.with_cache_dir(dir);
        if let Some(mb) = cache_budget_mb {
            opts = opts.with_cache_budget_bytes(mb.saturating_mul(1024 * 1024));
        }
    }
    Ok(opts)
}

/// Ingest window size in bytes from the `--batch-size-mb` flag (floored at
/// 1 MiB so `0` can't disable windowing).
fn window_bytes(mb: u64) -> usize {
    (mb.max(1) as usize).saturating_mul(1024 * 1024)
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

/// Print the counts a gc sweep reported.
fn print_gc(report: &GcReport) {
    println!(
        "deleted {} objects, freed {} bytes (skipped {} live, {} too new, {} errors)",
        report.objects_deleted,
        report.bytes_freed,
        report.objects_skipped_live,
        report.objects_skipped_too_new,
        report.delete_errors,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_flags_yields_empty_options_with_default_cache() {
        // With no flags, connect uses ambient identity — no storage options,
        // no api key — but the disk cache still defaults on so a durable
        // backend's reads stay bounded on disk instead of resident in RAM.
        // `ConnectOptions` exposes no getter, so inspect `Debug`.
        let opts = connect_options(&[], None, false, false, None, None).expect("assemble");
        let dbg = format!("{opts:?}");
        assert!(
            dbg.contains("storage_options: {}"),
            "no flags should carry no storage options, got {dbg}"
        );
        assert!(
            dbg.contains("validate: false"),
            "validate should default off, got {dbg}"
        );
        assert!(
            dbg.contains("cache_dir: Some"),
            "disk cache should default on, got {dbg}"
        );
    }

    #[test]
    fn no_disk_cache_flag_disables_the_cache() {
        let opts = connect_options(&[], None, false, true, None, None).expect("assemble");
        let dbg = format!("{opts:?}");
        assert!(
            dbg.contains("cache_dir: None"),
            "--no-disk-cache should disable the cache, got {dbg}"
        );
    }

    #[test]
    fn explicit_cache_dir_and_budget_are_recorded() {
        let dir = std::path::Path::new("/tmp/infino-cache-test");
        let opts = connect_options(&[], None, false, false, Some(dir), Some(64)).expect("assemble");
        let dbg = format!("{opts:?}");
        assert!(
            dbg.contains("infino-cache-test"),
            "explicit cache dir should be recorded, got {dbg}"
        );
        assert!(
            dbg.contains("cache_budget_bytes: Some(67108864)"),
            "cache budget should convert MiB to bytes, got {dbg}"
        );
    }

    #[test]
    fn storage_option_flag_requires_key_value() {
        let err = connect_options(&["not-a-pair".to_string()], None, false, false, None, None)
            .expect_err("malformed --storage-option should error");
        assert!(err.to_string().contains("KEY=VALUE"));
    }

    #[test]
    fn storage_option_flags_are_recorded() {
        let opts = connect_options(
            &[
                "aws_endpoint=http://h:9000".to_string(),
                "aws_region=us-west-1".to_string(),
            ],
            None,
            false,
            false,
            None,
            None,
        )
        .expect("assemble");
        let dbg = format!("{opts:?}");
        assert!(
            dbg.contains("aws_endpoint")
                && dbg.contains("http://h:9000")
                && dbg.contains("us-west-1"),
            "flag values should land in storage options, got {dbg}"
        );
    }

    #[test]
    fn api_key_and_validate_are_recorded() {
        let opts =
            connect_options(&[], Some("sk-test"), true, false, None, None).expect("assemble");
        let dbg = format!("{opts:?}");
        assert!(
            dbg.contains("sk-test") && dbg.contains("validate: true"),
            "api key and validate should be set, got {dbg}"
        );
    }
}
