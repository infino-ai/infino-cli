//! Search commands — one per public search method, mirroring the Node/Python
//! SDK surface: `bm25-search`, `vector-search`, `token-match`, `exact-match`.

use std::{
    fs::read_to_string,
    io::{Read, stdin},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use clap::{Args, ValueEnum};
use infino::{BoolMode, Supertable, VectorFilter, VectorSearchOptions};

/// Default number of results when `-k` is not given.
const DEFAULT_K: usize = 10;

/// Token matching mode, matching the SDKs' `"or"` / `"and"`.
#[derive(Copy, Clone, ValueEnum)]
pub enum Mode {
    Or,
    And,
}

impl From<Mode> for BoolMode {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::Or => BoolMode::Or,
            Mode::And => BoolMode::And,
        }
    }
}

/// `bm25Search(column, query, k, mode, projection)`.
#[derive(Args)]
pub struct Bm25Args {
    /// Table name.
    pub table: String,
    /// FTS-indexed column to search.
    pub column: String,
    /// Query text.
    pub query: String,
    /// Number of results.
    #[arg(short = 'k', long, default_value_t = DEFAULT_K)]
    pub k: usize,
    /// Boolean mode for multi-term queries.
    #[arg(long, value_enum, default_value = "or")]
    pub mode: Mode,
    /// Columns to return, comma-separated (default: id + score).
    #[arg(long, value_delimiter = ',')]
    pub fields: Option<Vec<String>>,
}

/// `vectorSearch(column, query, k, nprobe, rerankMult, projection, filter)`.
#[derive(Args)]
pub struct VectorArgs {
    /// Table name.
    pub table: String,
    /// Vector-indexed column to search.
    pub column: String,
    /// Query vector: a file holding a JSON array of numbers, or `-` for stdin.
    #[arg(long)]
    pub vector_file: PathBuf,
    /// Number of results.
    #[arg(short = 'k', long, default_value_t = DEFAULT_K)]
    pub k: usize,
    /// IVF probe count (higher = more recall, slower).
    #[arg(long)]
    pub nprobe: Option<usize>,
    /// Rerank multiplier.
    #[arg(long)]
    pub rerank_mult: Option<usize>,
    /// Columns to return, comma-separated (default: id + score).
    #[arg(long, value_delimiter = ',')]
    pub fields: Option<Vec<String>>,
    /// Pushdown keyword pre-filter — FTS column (requires `--filter-query`).
    #[arg(long, requires = "filter_query")]
    pub filter_column: Option<String>,
    /// Pushdown keyword pre-filter — query (requires `--filter-column`).
    #[arg(long, requires = "filter_column")]
    pub filter_query: Option<String>,
    /// Boolean mode for the pushdown filter.
    #[arg(long, value_enum, default_value = "or")]
    pub filter_mode: Mode,
}

/// `tokenMatch(column, query, mode, projection)`.
#[derive(Args)]
pub struct TokenMatchArgs {
    /// Table name.
    pub table: String,
    /// FTS-indexed column.
    pub column: String,
    /// Query text.
    pub query: String,
    /// Boolean mode for multi-term queries.
    #[arg(long, value_enum, default_value = "or")]
    pub mode: Mode,
    /// Columns to return, comma-separated.
    #[arg(long, value_delimiter = ',')]
    pub fields: Option<Vec<String>>,
}

/// `exactMatch(column, value, projection)`.
#[derive(Args)]
pub struct ExactMatchArgs {
    /// Table name.
    pub table: String,
    /// Column.
    pub column: String,
    /// Exact value to match.
    pub value: String,
    /// Columns to return, comma-separated.
    #[arg(long, value_delimiter = ',')]
    pub fields: Option<Vec<String>>,
}

pub fn bm25(table: &Supertable, args: &Bm25Args) -> Result<Vec<RecordBatch>> {
    let fields = projection(&args.fields);
    Ok(table.bm25_search(
        &args.column,
        &args.query,
        args.k,
        args.mode.into(),
        fields.as_deref(),
    )?)
}

pub fn vector(table: &Supertable, args: &VectorArgs) -> Result<Vec<RecordBatch>> {
    let query = read_vector(&args.vector_file)?;
    let mut options = VectorSearchOptions::new();
    if let Some(nprobe) = args.nprobe {
        options = options.with_nprobe(nprobe);
    }
    if let Some(rerank_mult) = args.rerank_mult {
        options = options.with_rerank_mult(rerank_mult);
    }
    let filter = match (&args.filter_column, &args.filter_query) {
        (Some(column), Some(query)) => Some(VectorFilter {
            column,
            query,
            mode: args.filter_mode.into(),
        }),
        _ => None,
    };
    let fields = projection(&args.fields);
    Ok(table.vector_search(
        &args.column,
        &query,
        args.k,
        options,
        filter,
        fields.as_deref(),
    )?)
}

pub fn token_match(table: &Supertable, args: &TokenMatchArgs) -> Result<Vec<RecordBatch>> {
    let fields = projection(&args.fields);
    Ok(table.token_match(
        &args.column,
        &args.query,
        args.mode.into(),
        fields.as_deref(),
    )?)
}

pub fn exact_match(table: &Supertable, args: &ExactMatchArgs) -> Result<Vec<RecordBatch>> {
    let fields = projection(&args.fields);
    Ok(table.exact_match(&args.column, &args.value, fields.as_deref())?)
}

/// Turn the `--fields` list into a projection borrowing the argument strings.
fn projection(fields: &Option<Vec<String>>) -> Option<Vec<&str>> {
    fields
        .as_ref()
        .map(|names| names.iter().map(String::as_str).collect())
}

/// Read a query vector from a file holding a JSON array of numbers (`-` = stdin).
fn read_vector(path: &Path) -> Result<Vec<f32>> {
    let text = if path.as_os_str() == "-" {
        let mut buf = String::new();
        stdin()
            .read_to_string(&mut buf)
            .context("reading vector from stdin")?;
        buf
    } else {
        read_to_string(path).with_context(|| format!("reading vector file {}", path.display()))?
    };
    serde_json::from_str(&text).context("vector file must be a JSON array of numbers")
}
