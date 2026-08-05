//! TEMPORARY hosted-transport shim.
//!
//! The hosted wire protocol currently carries only scalar / float-vector /
//! list-of-scalar column types, so a schema with structs, lists-of-structs,
//! maps, or timestamps is rejected at `create_table`. To let such data reach
//! the hosted service anyway, this module rewrites each unsupported column to
//! `LargeUtf8` holding the column's JSON text (one JSON value per row). The
//! encoding is lossless: the nested value round-trips through JSON, so it is
//! retrievable and full-text searchable, just not structured-queryable
//! server-side.
//!
//! Scoped to `https://` (hosted) targets only. Local and object-storage
//! backends handle these types natively and are left untouched. Remove this
//! whole module (and its call sites in `main.rs`) once the hosted API accepts
//! nested schemas directly.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, LargeStringArray, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
    json::writer::ArrayWriter,
};
use serde_json::Value;

/// Whether `uri` targets the hosted service, where the shim applies.
pub fn is_hosted(uri: Option<&str>) -> bool {
    uri.is_some_and(|u| u.starts_with("https://") || u.starts_with("http://"))
}

/// Scalar types the hosted wire carries directly.
fn is_scalar(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Column types the hosted wire carries as-is: scalars, a float vector
/// (`FixedSizeList<Float32>`), and a list of scalars. Everything else (structs,
/// lists of structs, maps, timestamps, ...) is encoded as JSON text.
fn wire_supported(dt: &DataType) -> bool {
    if is_scalar(dt) {
        return true;
    }
    match dt {
        DataType::FixedSizeList(item, _) => item.data_type() == &DataType::Float32,
        DataType::List(item) => is_scalar(item.data_type()),
        _ => false,
    }
}

/// Rewrite `schema` so every column the hosted wire can't carry becomes
/// `LargeUtf8` (it will hold JSON text). Returns the rewritten schema and the
/// indices of the rewritten columns.
pub fn jsonify_schema(schema: &SchemaRef) -> (SchemaRef, Vec<usize>) {
    let mut converted = Vec::new();
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            if wire_supported(f.data_type()) {
                f.as_ref().clone()
            } else {
                converted.push(i);
                Field::new(f.name(), DataType::LargeUtf8, true)
            }
        })
        .collect();
    (Arc::new(Schema::new(fields)), converted)
}

/// Rebuild `batch` so the `convert` columns hold JSON text, producing a batch
/// that matches `target` (the schema from [`jsonify_schema`]). Empty `convert`
/// returns the batch unchanged.
pub fn jsonify_batch(
    batch: &RecordBatch,
    convert: &[usize],
    target: &SchemaRef,
) -> Result<RecordBatch> {
    if convert.is_empty() {
        return Ok(batch.clone());
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        if convert.contains(&i) {
            columns.push(Arc::new(column_to_json(batch, i)?));
        } else {
            columns.push(batch.column(i).clone());
        }
    }
    RecordBatch::try_new(target.clone(), columns)
        .context("rebuilding batch with JSON-encoded nested columns")
}

/// Serialize one column's values to a `LargeUtf8` array of JSON text, one JSON
/// value per row (a null value stays null).
fn column_to_json(batch: &RecordBatch, col: usize) -> Result<LargeStringArray> {
    let field = batch.schema().field(col).as_ref().clone();
    let key = field.name().clone();
    let single = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![batch.column(col).clone()],
    )
    .context("isolating nested column")?;

    let mut buf = Vec::new();
    {
        let mut writer = ArrayWriter::new(&mut buf);
        writer
            .write(&single)
            .with_context(|| format!("JSON-encoding column `{key}`"))?;
        writer.finish().context("finishing JSON encoder")?;
    }
    // ArrayWriter emits `[{"<key>": <value>}, ...]`; pull out each row's value.
    let rows: Vec<Value> = serde_json::from_slice(&buf).context("parsing encoded JSON")?;
    Ok(rows
        .into_iter()
        .map(|mut row| match row.get_mut(key.as_str()) {
            Some(v) if !v.is_null() => Some(v.take().to_string()),
            _ => None,
        })
        .collect::<LargeStringArray>())
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StructArray};
    use arrow::datatypes::Fields;

    use super::*;

    #[test]
    fn is_hosted_only_for_http_uris() {
        assert!(is_hosted(Some("https://api.example/db")));
        assert!(!is_hosted(Some("s3://bucket/prefix")));
        assert!(!is_hosted(Some("file://./data")));
        assert!(!is_hosted(None));
    }

    #[test]
    fn jsonify_schema_converts_only_unsupported() {
        let inner = Fields::from(vec![Field::new("x", DataType::Int64, true)]);
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("meta", DataType::Struct(inner), true),
        ]));
        let (out, convert) = jsonify_schema(&schema);
        assert_eq!(convert, vec![2], "only the struct column");
        assert_eq!(out.field(0).data_type(), &DataType::Int64);
        assert_eq!(out.field(1).data_type(), &DataType::Utf8);
        assert_eq!(out.field(2).data_type(), &DataType::LargeUtf8);
    }

    #[test]
    fn jsonify_batch_encodes_struct_as_json_text() {
        let inner = Fields::from(vec![Field::new("x", DataType::Int64, true)]);
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("meta", DataType::Struct(inner.clone()), true),
        ]));
        let meta = StructArray::new(
            inner,
            vec![Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef],
            None,
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(meta)],
        )
        .expect("batch");

        let (target, convert) = jsonify_schema(&schema);
        let out = jsonify_batch(&batch, &convert, &target).expect("jsonify");
        assert_eq!(out.schema().field(1).data_type(), &DataType::LargeUtf8);
        let json = out
            .column(1)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("large utf8");
        assert_eq!(json.value(0), "{\"x\":7}");
        assert_eq!(json.value(1), "{\"x\":8}");
    }
}
