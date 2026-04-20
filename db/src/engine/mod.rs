use std::fmt;

use fallible_iterator::FallibleIterator;
use thiserror::Error;

use crate::{
    FileId, TransactionId, Value,
    binder::{Bound, bind},
    buffer_pool::page_store::PageStoreError,
    catalog::{CatalogError, manager::Catalog},
    execution::unary::BooleanExpression,
    heap::file::{HeapError, HeapFile},
    parser::statements::{Statement, WhereCondition},
    primitives::RecordId,
    transaction::{Transaction, TransactionError, TransactionManager},
    tuple::{Tuple, TupleSchema},
};

mod ddl;
mod dml;
mod query;

#[derive(Debug)]
pub enum StatementResult {
    TableCreated {
        name: String,
        file_id: FileId,
        already_exists: bool,
    },
    TableDropped {
        name: String,
    },
    Inserted {
        table: String,
        rows: usize,
    },
    Deleted {
        table: String,
        rows: usize,
    },
    Updated {
        table: String,
        rows: usize,
    },
    Selected {
        table: String,
        rows: Vec<Tuple>,
    },
}

impl StatementResult {
    pub(super) fn table_created(
        name: impl Into<String>,
        file_id: FileId,
        already_exists: bool,
    ) -> Self {
        Self::TableCreated {
            name: name.into(),
            file_id,
            already_exists,
        }
    }

    pub(super) fn table_dropped(name: impl Into<String>) -> Self {
        Self::TableDropped { name: name.into() }
    }

    pub(super) fn inserted(table: impl Into<String>, rows: usize) -> Self {
        Self::Inserted {
            table: table.into(),
            rows,
        }
    }

    pub(super) fn deleted(table: impl Into<String>, rows: usize) -> Self {
        Self::Deleted {
            table: table.into(),
            rows,
        }
    }

    pub(super) fn updated(table: impl Into<String>, rows: usize) -> Self {
        Self::Updated {
            table: table.into(),
            rows,
        }
    }
}

impl fmt::Display for StatementResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatementResult::TableCreated {
                name,
                file_id,
                already_exists,
            } => {
                if *already_exists {
                    write!(
                        f,
                        "CREATE TABLE completed: table '{name}' already exists (IF NOT EXISTS); using existing file {file_id}"
                    )
                } else {
                    write!(
                        f,
                        "CREATE TABLE completed: registered '{name}' in the catalog with backing file {file_id}"
                    )
                }
            }
            StatementResult::TableDropped { name } => write!(
                f,
                "DROP TABLE completed: removed '{name}' from the catalog and released its heap file"
            ),
            StatementResult::Inserted { table, rows } => {
                let row_word = if *rows == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "INSERT completed: wrote {rows} {row_word} into heap table '{table}'",
                )
            }
            StatementResult::Deleted { table, rows } => {
                let row_word = if *rows == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "DELETE completed: removed {rows} {row_word} from heap table '{table}'",
                )
            }
            StatementResult::Updated { table, rows } => {
                let row_word = if *rows == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "UPDATE completed: modified {rows} {row_word} in heap table '{table}'",
                )
            }
            StatementResult::Selected { table, rows } => {
                let row_word = if rows.len() == 1 { "row" } else { "rows" };
                write!(
                    f,
                    "SELECT completed: returned {} {row_word} from '{table}'",
                    rows.len()
                )
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("unsupported statement: {0}")]
    Unsupported(String),

    #[error(transparent)]
    Catalog(#[from] CatalogError),

    #[error(transparent)]
    Transaction(#[from] TransactionError),

    #[error(transparent)]
    Storage(#[from] HeapError),

    #[error(transparent)]
    BufferPool(#[from] PageStoreError),

    #[error(transparent)]
    Bind(#[from] crate::binder::BindError),

    #[error("table '{0}' not found")]
    TableNotFound(String),

    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("column '{column}' not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },

    #[error("column '{column}' appears more than once in INSERT into '{table}'")]
    DuplicateInsertColumn { table: String, column: String },

    #[error("wrong number of values for table '{table}': expected {expected}, got {got}")]
    WrongColumnCount {
        table: String,
        expected: usize,
        got: usize,
    },

    #[error("null value in NOT NULL column '{column}' in table '{table}'")]
    NullViolation { table: String, column: String },

    #[error("type mismatch for column '{column}': expected {expected}, got {got}")]
    TypeMismatch {
        column: String,
        expected: String,
        got: String,
    },

    #[error("type error: {0}")]
    TypeError(String),
}

impl EngineError {
    pub(super) fn column_not_found(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self::ColumnNotFound {
            table: table.into(),
            column: column.into(),
        }
    }

    pub(super) fn column_already_exists(
        table: impl Into<String>,
        column: impl Into<String>,
    ) -> Self {
        Self::DuplicateInsertColumn {
            table: table.into(),
            column: column.into(),
        }
    }

    pub(super) fn type_error(message: impl Into<String>) -> Self {
        Self::TypeError(message.into())
    }
}

pub(super) fn with_txn<T, F>(f: F, txn_manager: &TransactionManager) -> Result<T, EngineError>
where
    F: FnOnce(&Transaction<'_>) -> Result<T, EngineError>,
{
    let txn = txn_manager.begin()?;
    let out = f(&txn)?;
    txn.commit()?;
    Ok(out)
}

/// Binds `stmt` inside a fresh transaction and hands the resulting [`Bound`]
/// to `run` for execution. The transaction commits on success.
///
/// This is the shared shell used by statement executors that all follow the
/// same shape: begin a txn, bind the AST against the catalog, then dispatch
/// on the bound variant. Callers supply `run` to do the variant-specific work.
pub(super) fn bind_and_execute<F>(
    catalog: &Catalog,
    txn_manager: &TransactionManager,
    stmt: Statement,
    run: F,
) -> Result<StatementResult, EngineError>
where
    F: FnOnce(&Catalog, Bound, &Transaction<'_>) -> Result<StatementResult, EngineError>,
{
    with_txn(
        |txn| {
            let bound = bind(stmt, catalog, txn)?;
            run(catalog, bound, txn)
        },
        txn_manager,
    )
}

/// Collects every heap row visible to `transaction_id`, optionally filtered by `predicate`.
///
/// Rows with no predicate are included. When `predicate` is `Some`, a row is kept only if the
/// expression evaluates to true for that row's tuple.
///
/// # Errors
///
/// Returns [`EngineError`] if the heap scan cannot advance.
///
/// # Examples
///
/// Collect all rows without filtering:
///
/// ```ignore
/// let rows = collect_matching(&heap, txn_id, None)?;
/// // rows contains every (RecordId, Tuple) pair visible to the transaction
/// ```
///
/// Collect only rows where the first column equals `42`:
///
/// ```ignore
/// use storemy::{execution::unary::BooleanExpression, primitives::Predicate, types::Value};
///
/// let predicate = BooleanExpression::Leaf {
///     col_index: 0,
///     op: Predicate::Equals,
///     operand: Value::Int32(42),
/// };
/// let rows = collect_matching(&heap, txn_id, Some(&predicate))?;
/// // rows contains only tuples whose first column is 42
/// ```
pub(super) fn collect_matching(
    heap: &HeapFile,
    transaction_id: TransactionId,
    predicate: Option<&BooleanExpression>,
) -> Result<Vec<(RecordId, Tuple)>, EngineError> {
    let mut scan = heap.scan(transaction_id)?;
    let mut out = Vec::new();
    while let Some((rid, tuple)) = FallibleIterator::next(&mut scan)? {
        if predicate.is_none_or(|p| p.eval(&tuple)) {
            out.push((rid, tuple));
        }
    }
    Ok(out)
}

/// Builds a [`BooleanExpression`] from a parsed [`WhereCondition`].
///
/// Leaf predicates resolve the column name to an index, coerce the literal to the column's
/// [`crate::types::Type`], and store the comparison operator. `AND` and `OR` nodes recurse.
///
/// For unknown columns in a leaf, the error's table name is the literal `"?"` (the AST node does
/// not carry the table name).
///
/// # Errors
///
/// Returns [`EngineError`] when a column name does not exist in `schema`, or when a literal
/// cannot be converted to the column's type.
///
/// # Examples
///
/// Resolve a simple leaf predicate (`age > 30`):
///
/// ```ignore
/// use storemy::{
///     parser::statements::WhereCondition,
///     primitives::Predicate,
///     tuple::{Field, TupleSchema},
///     types::{Type, Value},
/// };
///
/// let schema = TupleSchema::new(vec![
///     Field::new("name", Type::Varchar),
///     Field::new("age", Type::Int32),
/// ]);
/// let condition = WhereCondition::predicate("age", Predicate::GreaterThan, Value::Int32(30));
/// let expr = resolve_where_clause(&condition, &schema)?;
/// // expr is BooleanExpression::Leaf { col_index: 1, op: GreaterThan, operand: Int32(30) }
/// ```
///
/// Resolve a compound `AND` condition (`age > 30 AND name = 'Alice'`):
///
/// ```ignore
/// use storemy::{
///     parser::statements::WhereCondition,
///     primitives::Predicate,
///     tuple::{Field, TupleSchema},
///     types::{Type, Value},
/// };
///
/// let schema = TupleSchema::new(vec![
///     Field::new("name", Type::Varchar),
///     Field::new("age", Type::Int32),
/// ]);
/// let condition = WhereCondition::And(
///     Box::new(WhereCondition::predicate("age", Predicate::GreaterThan, Value::Int32(30))),
///     Box::new(WhereCondition::predicate("name", Predicate::Equals, Value::String("Alice".into()))),
/// );
/// let expr = resolve_where_clause(&condition, &schema)?;
/// // expr is BooleanExpression::And(Leaf { col_index: 1, … }, Leaf { col_index: 0, … })
/// ```
///
/// Resolving an unknown column returns an error:
///
/// ```ignore
/// let bad = WhereCondition::predicate("missing", Predicate::Equals, Value::Int32(1));
/// assert!(resolve_where_clause(&bad, &schema).is_err());
/// ```
pub(super) fn resolve_where_clause(
    w: &WhereCondition,
    schema: &TupleSchema,
) -> Result<BooleanExpression, EngineError> {
    match w {
        WhereCondition::Predicate { field, op, value } => {
            let (idx, field_def) = schema
                .field_by_name(&field.name)
                .ok_or_else(|| EngineError::column_not_found("?", field.name.clone()))?;
            let operand = Value::try_from((value, field_def.field_type))
                .map_err(|e| EngineError::type_error(e.to_string()))?;
            Ok(BooleanExpression::Leaf {
                col_index: idx,
                op: *op,
                operand,
            })
        }

        WhereCondition::And(left, right) => {
            let left = resolve_where_clause(left, schema)?;
            let right = resolve_where_clause(right, schema)?;
            Ok(BooleanExpression::And(Box::new(left), Box::new(right)))
        }

        WhereCondition::Or(left, right) => {
            let left = resolve_where_clause(left, schema)?;
            let right = resolve_where_clause(right, schema)?;
            Ok(BooleanExpression::Or(Box::new(left), Box::new(right)))
        }
    }
}

pub fn execute_statement(
    catalog: &Catalog,
    stmt: Statement,
    txn_manager: &TransactionManager,
) -> Result<StatementResult, EngineError> {
    match stmt {
        Statement::CreateTable(_) => ddl::create_table::execute(catalog, txn_manager, stmt),
        Statement::Drop(s) => ddl::drop_table::execute(catalog, txn_manager, s),
        Statement::Insert(_) => dml::insert::execute(catalog, txn_manager, stmt),
        Statement::Delete(_) => dml::delete::execute(catalog, txn_manager, stmt),
        Statement::Update(s) => dml::update::execute(catalog, txn_manager, s),
        _ => Err(EngineError::Unsupported(stmt.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{primitives::Predicate, tuple::Field, types::Type};

    fn schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Int32),
            Field::new("name", Type::String),
        ])
    }

    fn leaf(field: &str, value: Value) -> WhereCondition {
        WhereCondition::Predicate {
            field: field.into(),
            op: Predicate::Equals,
            value,
        }
    }

    // a leaf predicate resolves to BooleanExpression::Leaf with the right index/op
    #[test]
    fn test_resolve_where_predicate_leaf() {
        let s = schema();
        let w = leaf("id", Value::Int64(5));
        match resolve_where_clause(&w, &s).unwrap() {
            BooleanExpression::Leaf {
                col_index,
                op,
                operand,
            } => {
                assert_eq!(col_index, 0);
                assert_eq!(op, Predicate::Equals);
                assert!(matches!(operand, Value::Int32(5)));
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    // And recurses into both sides
    #[test]
    fn test_resolve_where_and_recurses() {
        let s = schema();
        let w = WhereCondition::And(
            Box::new(leaf("id", Value::Int64(1))),
            Box::new(leaf("name", Value::String("x".into()))),
        );
        assert!(matches!(
            resolve_where_clause(&w, &s).unwrap(),
            BooleanExpression::And(_, _)
        ));
    }

    // Or recurses into both sides
    #[test]
    fn test_resolve_where_or_recurses() {
        let s = schema();
        let w = WhereCondition::Or(
            Box::new(leaf("id", Value::Int64(1))),
            Box::new(leaf("name", Value::String("x".into()))),
        );
        assert!(matches!(
            resolve_where_clause(&w, &s).unwrap(),
            BooleanExpression::Or(_, _)
        ));
    }

    // nested And inside Or fully resolves (no early return)
    #[test]
    fn test_resolve_where_nested() {
        let s = schema();
        let inner = WhereCondition::And(
            Box::new(leaf("id", Value::Int64(1))),
            Box::new(leaf("name", Value::String("x".into()))),
        );
        let outer = WhereCondition::Or(Box::new(inner), Box::new(leaf("id", Value::Int64(2))));
        assert!(matches!(
            resolve_where_clause(&outer, &s).unwrap(),
            BooleanExpression::Or(_, _)
        ));
    }

    // unknown field in a leaf predicate -> ColumnNotFound (table is the "?" placeholder)
    #[test]
    fn test_resolve_where_unknown_field() {
        let s = schema();
        let w = leaf("ghost", Value::Int64(1));
        match resolve_where_clause(&w, &s).unwrap_err() {
            EngineError::ColumnNotFound { table, column } => {
                assert_eq!(table, "?");
                assert_eq!(column, "ghost");
            }
            other => panic!("expected ColumnNotFound, got {other:?}"),
        }
    }

    // type-incompatible literal -> TypeError from Value::try_from
    #[test]
    fn test_resolve_where_type_mismatch() {
        let s = schema();
        let w = leaf("id", Value::String("not a number".into()));
        let err = resolve_where_clause(&w, &s).unwrap_err();
        assert!(matches!(err, EngineError::TypeError(_)));
    }

    // failure in the LEFT branch of And short-circuits before evaluating RIGHT
    #[test]
    fn test_resolve_where_and_left_error_short_circuits() {
        let s = schema();
        let bad = leaf("ghost", Value::Int64(1));
        let ok = leaf("id", Value::Int64(2));
        assert!(
            resolve_where_clause(&WhereCondition::And(Box::new(bad), Box::new(ok)), &s).is_err()
        );
    }

    // failure in the RIGHT branch of Or still surfaces an error
    #[test]
    fn test_resolve_where_or_right_error_propagates() {
        let s = schema();
        let ok = leaf("id", Value::Int64(1));
        let bad = leaf("ghost", Value::Int64(2));
        assert!(
            resolve_where_clause(&WhereCondition::Or(Box::new(ok), Box::new(bad)), &s).is_err()
        );
    }
}
