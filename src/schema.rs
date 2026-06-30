//! Building a table's Arrow schema (from a YAML column list) and its index
//! spec (from `--fts` / `--vector` flags).

use std::{fs::read_to_string, path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use infino::{IndexSpec, Metric};
use serde::Deserialize;

#[derive(Deserialize)]
struct ColumnSpec {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(default)]
    nullable: bool,
}

/// Parse a YAML list of `{name, type, nullable?}` columns into an Arrow schema.
pub fn schema_from_yaml(path: &Path) -> Result<SchemaRef> {
    let text =
        read_to_string(path).with_context(|| format!("reading schema {}", path.display()))?;
    let columns: Vec<ColumnSpec> = serde_yaml::from_str(&text).context("parsing schema YAML")?;
    let fields = columns
        .iter()
        .map(|c| Ok(Field::new(&c.name, parse_type(&c.ty)?, c.nullable)))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Build an `IndexSpec` from repeated `--fts <col>` and
/// `--vector <col:dim:n_cent:metric>` flags.
pub fn index_spec(fts: &[String], vector: &[String]) -> Result<IndexSpec> {
    let mut spec = IndexSpec::new();
    for column in fts {
        spec = spec.fts(column.clone());
    }
    for entry in vector {
        let (column, dim, n_cent, metric) = parse_vector(entry)?;
        spec = spec.vector(column, dim, n_cent, metric);
    }
    Ok(spec)
}

/// Parse `column:dim:n_cent:metric`, e.g. `embedding:384:256:cosine`.
fn parse_vector(entry: &str) -> Result<(String, usize, usize, Metric)> {
    let parts: Vec<&str> = entry.split(':').collect();
    if parts.len() != 4 {
        bail!("--vector must be `column:dim:n_cent:metric`, got {entry:?}");
    }
    let dim = parts[1]
        .parse()
        .with_context(|| format!("vector dim in {entry:?}"))?;
    let n_cent = parts[2]
        .parse()
        .with_context(|| format!("vector n_cent in {entry:?}"))?;
    Ok((
        parts[0].to_string(),
        dim,
        n_cent,
        metric_from_str(parts[3])?,
    ))
}

fn metric_from_str(s: &str) -> Result<Metric> {
    match s.to_ascii_lowercase().as_str() {
        "cosine" => Ok(Metric::Cosine),
        "l2sq" | "l2" => Ok(Metric::L2Sq),
        "negdot" | "dot" => Ok(Metric::NegDot),
        other => bail!("unknown metric {other:?}; use cosine, l2sq, or negdot"),
    }
}

/// Map a type string to an Arrow `DataType`.
fn parse_type(ty: &str) -> Result<DataType> {
    Ok(match ty.trim().to_ascii_lowercase().as_str() {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" | "int" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" | "float" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "utf8" | "string" | "str" => DataType::Utf8,
        // infino's full-text index requires LargeUtf8, so FTS columns use this.
        "large_utf8" | "largeutf8" | "large_string" | "largestring" => DataType::LargeUtf8,
        "bool" | "boolean" => DataType::Boolean,
        "date32" => DataType::Date32,
        other => parse_fixed_size_list(other)
            .with_context(|| format!("unsupported column type {ty:?}"))?,
    })
}

/// Parse `fixed_size_list<float32,384>` — the vector column type.
fn parse_fixed_size_list(ty: &str) -> Result<DataType> {
    let inner = ty
        .strip_prefix("fixed_size_list<")
        .and_then(|s| s.strip_suffix('>'))
        .context("not a fixed_size_list")?;
    let (item, len) = inner
        .split_once(',')
        .context("fixed_size_list needs <type,len>")?;
    let item_type = parse_type(item)?;
    let len: i32 = len.trim().parse().context("fixed_size_list length")?;
    Ok(DataType::FixedSizeList(
        Arc::new(Field::new("item", item_type, false)),
        len,
    ))
}
