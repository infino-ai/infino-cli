//! Parse a SQL predicate string into a DataFusion `Expr`, resolved against the
//! table's schema — the same model the Node and Python bindings use.

use anyhow::{Context, Result};
use datafusion::{common::DFSchema, execution::context::SessionContext, logical_expr::Expr};
use infino::Supertable;

/// Resolve a predicate like `"status = 'spam'"` against the table schema.
pub fn parse(table: &Supertable, predicate: &str) -> Result<Expr> {
    let df_schema =
        DFSchema::try_from(table.schema().as_ref().clone()).context("building predicate schema")?;
    SessionContext::new()
        .parse_sql_expr(predicate, &df_schema)
        .with_context(|| format!("invalid predicate {predicate:?}"))
}
