//! Rendering Arrow rows in the format the user asked for.

use std::{
    fmt,
    io::{Write, stdout},
};

use anyhow::{Context, Result};
use arrow::{
    array::RecordBatch, csv::WriterBuilder, json::LineDelimitedWriter,
    util::pretty::pretty_format_batches,
};
use clap::ValueEnum;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{MapAccess, SeqAccess, Visitor},
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::{Number, value::RawValue};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Aligned, human-readable table.
    Table,
    /// One JSON object per row (newline-delimited).
    Json,
    /// Comma-separated values with a header row.
    Csv,
}

/// Print a result set in the chosen format.
///
/// With `expand_json`, a JSON row's string values that hold a JSON object or
/// array are written as that object or array rather than as an escaped
/// string. Only the JSON format has nested values to write them into.
pub fn render(format: OutputFormat, expand_json: bool, batches: &[RecordBatch]) -> Result<()> {
    match format {
        OutputFormat::Table => {
            if batches.iter().all(|b| b.num_rows() == 0) {
                println!("(no rows)");
            } else {
                println!("{}", pretty_format_batches(batches)?);
            }
        }
        OutputFormat::Json if expand_json => {
            let mut rows = Vec::new();
            write_ndjson(&mut rows, batches)?;
            write_expanded(&rows, &mut stdout().lock())?;
        }
        OutputFormat::Json => write_ndjson(stdout().lock(), batches)?,
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

/// Write `batches` as newline-delimited JSON, one object per row.
fn write_ndjson<W: Write>(out: W, batches: &[RecordBatch]) -> Result<()> {
    let mut writer = LineDelimitedWriter::new(out);
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(())
}

/// Rewrite newline-delimited JSON `rows` to `out`, each string column whose
/// value holds a JSON object or array written as that value, expanded at
/// every depth.
///
/// Every other column is copied as the exact text Arrow wrote. Parsing it
/// would round a number wider than a float, such as a 128-bit `_id`.
fn write_expanded<W: Write>(rows: &[u8], out: &mut W) -> Result<()> {
    for line in rows.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
        let row: Row = serde_json::from_slice(line).context("re-reading a JSON row")?;
        out.write_all(b"{")?;
        for (i, (column, value)) in row.0.iter().enumerate() {
            if i > 0 {
                out.write_all(b",")?;
            }
            serde_json::to_writer(&mut *out, column)?;
            out.write_all(b":")?;
            match expand_column(value)? {
                Some(document) => serde_json::to_writer(&mut *out, &document)?,
                None => out.write_all(value.get().as_bytes())?,
            }
        }
        out.write_all(b"}\n")?;
    }
    Ok(())
}

/// A column's value expanded, when it is a string holding a JSON object or
/// array; `None` to keep the value as written.
fn expand_column(value: &RawValue) -> Result<Option<Node>> {
    if !value.get().starts_with('"') {
        return Ok(None);
    }
    let text: String = serde_json::from_str(value.get()).context("re-reading a string column")?;
    Ok(parse_json_text(&text).map(Node::expand))
}

/// One JSON row: its columns in the order written, each value as raw text.
struct Row(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(RowVisitor)
    }
}

/// Builds a [`Row`] from a JSON object.
struct RowVisitor;

impl<'de> Visitor<'de> for RowVisitor {
    type Value = Row;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Row, A::Error> {
        let mut columns = Vec::new();
        while let Some(entry) = map.next_entry()? {
            columns.push(entry);
        }
        Ok(Row(columns))
    }
}

/// A JSON value that keeps its object keys in the order they were written.
///
/// `serde_json::Value` sorts keys unless a crate-wide feature is turned on,
/// which would reorder every key of an expanded document.
#[derive(Debug, Clone, PartialEq)]
enum Node {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Node>),
    Object(Vec<(String, Node)>),
}

impl Node {
    /// This value with every string inside it that holds a JSON object or
    /// array replaced by the parsed value, itself expanded. Each parse yields
    /// strictly shorter strings than the one it came from, so this ends.
    fn expand(self) -> Node {
        match self {
            Node::String(text) => match parse_json_text(&text) {
                Some(parsed) => parsed.expand(),
                None => Node::String(text),
            },
            Node::Array(items) => Node::Array(items.into_iter().map(Node::expand).collect()),
            Node::Object(fields) => Node::Object(
                fields
                    .into_iter()
                    .map(|(key, value)| (key, value.expand()))
                    .collect(),
            ),
            other => other,
        }
    }
}

/// `text` parsed, when it holds a JSON object or array; `None` for any other
/// string, including one that only starts like JSON.
fn parse_json_text(text: &str) -> Option<Node> {
    let trimmed = text.trim();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return None;
    }
    serde_json::from_str(trimmed).ok()
}

impl Serialize for Node {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Node::Null => serializer.serialize_unit(),
            Node::Bool(b) => serializer.serialize_bool(*b),
            Node::Number(n) => n.serialize(serializer),
            Node::String(s) => serializer.serialize_str(s),
            Node::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Node::Object(fields) => {
                let mut map = serializer.serialize_map(Some(fields.len()))?;
                for (key, value) in fields {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(NodeVisitor)
    }
}

/// Builds a [`Node`], keeping object keys in input order.
struct NodeVisitor;

impl<'de> Visitor<'de> for NodeVisitor {
    type Value = Node;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Node, E> {
        Ok(Node::Null)
    }

    fn visit_none<E>(self) -> Result<Node, E> {
        Ok(Node::Null)
    }

    fn visit_bool<E>(self, b: bool) -> Result<Node, E> {
        Ok(Node::Bool(b))
    }

    fn visit_i64<E>(self, n: i64) -> Result<Node, E> {
        Ok(Node::Number(n.into()))
    }

    fn visit_u64<E>(self, n: u64) -> Result<Node, E> {
        Ok(Node::Number(n.into()))
    }

    fn visit_f64<E>(self, n: f64) -> Result<Node, E> {
        // JSON has no NaN or infinity, so a parsed float always has a Number.
        Ok(Number::from_f64(n).map_or(Node::Null, Node::Number))
    }

    fn visit_str<E>(self, s: &str) -> Result<Node, E> {
        Ok(Node::String(s.to_owned()))
    }

    fn visit_string<E>(self, s: String) -> Result<Node, E> {
        Ok(Node::String(s))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Node, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Node::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Node, A::Error> {
        let mut fields = Vec::new();
        while let Some((key, value)) = map.next_entry()? {
            fields.push((key, value));
        }
        Ok(Node::Object(fields))
    }
}

#[cfg(test)]
mod tests {
    use std::{slice::from_ref, sync::Arc};

    use arrow::{
        array::{Decimal128Array, Float64Array, Int64Array, LargeStringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    /// A 128-bit `_id`, wider than any JSON integer a float could carry exactly.
    const WIDE_ID: i128 = 33_044_227_701_279_365_841_616_791_142_402;

    /// Precision and scale of the engine's `_id` column.
    const ID_PRECISION: u8 = 38;

    /// The rows of `batch` as expanded newline-delimited JSON.
    fn expanded(batch: &RecordBatch) -> String {
        let mut rows = Vec::new();
        write_ndjson(&mut rows, from_ref(batch)).expect("ndjson");
        let mut out = Vec::new();
        write_expanded(&rows, &mut out).expect("expand");
        String::from_utf8(out).expect("utf-8")
    }

    fn log_row(resource: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_id", DataType::Decimal128(ID_PRECISION, 0), false),
            Field::new("body", DataType::LargeUtf8, false),
            Field::new("resource", DataType::LargeUtf8, true),
            Field::new("n", DataType::Int64, false),
            Field::new("x", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    Decimal128Array::from(vec![WIDE_ID])
                        .with_precision_and_scale(ID_PRECISION, 0)
                        .expect("decimal"),
                ),
                Arc::new(LargeStringArray::from(vec!["{not json"])),
                Arc::new(LargeStringArray::from(vec![resource])),
                Arc::new(Int64Array::from(vec![i64::MIN])),
                Arc::new(Float64Array::from(vec![0.1])),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn json_held_as_a_string_is_written_as_json_at_every_depth() {
        let network = "[{\n    \"name\": \"cbr0\",\n    \"ips\": [\"10.244.0.177\"]\n}]";
        let resource = serde_json::json!({
            "app": "beeper",
            "k8s.v1.cni.cncf.io/network-status": network,
        })
        .to_string();
        let out = expanded(&log_row(&resource));
        assert!(!out.contains("\\\""), "no escaped quotes left: {out}");
        let row: serde_json::Value = serde_json::from_str(&out).expect("one JSON row");
        assert_eq!(
            row["resource"]["k8s.v1.cni.cncf.io/network-status"][0]["ips"][0],
            "10.244.0.177"
        );
        assert_eq!(row["resource"]["app"], "beeper");
        assert_eq!(
            row["body"], "{not json",
            "a string that only looks like JSON stays"
        );
    }

    #[test]
    fn columns_keys_and_numbers_come_out_as_written() {
        let out = expanded(&log_row(r#"{"z":1,"a":2,"m":[3,{"y":4,"b":5}]}"#));
        let mut plain = Vec::new();
        write_ndjson(&mut plain, &[log_row("{}")]).expect("ndjson");
        let plain = String::from_utf8(plain).expect("utf-8");
        // Every column other than the expanded one is byte-for-byte what the
        // plain JSON format writes, in the same order.
        let prefix = &plain[..plain.find(r#""resource""#).expect("resource key")];
        assert!(out.starts_with(prefix), "{out}\nvs\n{plain}");
        assert!(
            out.contains(r#""resource":{"z":1,"a":2,"m":[3,{"y":4,"b":5}]}"#),
            "{out}"
        );
        assert!(out.contains(&format!(r#""n":{}"#, i64::MIN)), "{out}");
        assert!(out.contains(r#""x":0.1"#), "{out}");
    }

    #[test]
    fn rows_without_json_text_are_unchanged() {
        let batch = log_row("plain text");
        let mut plain = Vec::new();
        write_ndjson(&mut plain, from_ref(&batch)).expect("ndjson");
        assert_eq!(expanded(&batch), String::from_utf8(plain).expect("utf-8"));
    }

    #[test]
    fn an_empty_result_writes_nothing() {
        let mut out = Vec::new();
        write_expanded(b"", &mut out).expect("expand");
        assert!(out.is_empty());
    }
}
