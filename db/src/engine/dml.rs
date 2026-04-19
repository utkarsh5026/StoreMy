use fallible_iterator::FallibleIterator;

use crate::{
    TransactionId, Value,
    catalog::manager::{Catalog, TableInfo},
    engine::{EngineError, StatementResult},
    execution::unary::BooleanExpression,
    heap::file::HeapFile,
    parser::statements::{
        Assignment, DeleteStatement, InsertStatement, UpdateStatement, WhereCondition,
    },
    primitives::RecordId,
    transaction::{Transaction, TransactionManager},
    tuple::{Tuple, TupleSchema},
};

pub(super) fn insert(
    catalog: &Catalog,
    txn_manager: &TransactionManager,
    statement: InsertStatement,
) -> Result<super::StatementResult, super::EngineError> {
    let table_name = statement.table_name;
    let cols = statement.columns;
    let values = statement.values;

    let f = |txn: &Transaction<'_>| {
        let table_info = catalog.get_table_info(txn, &table_name)?;
        let schema = table_info.schema;

        let field_mapping = if let Some(cols) = cols {
            let mut field_mapping = Vec::with_capacity(schema.num_fields());
            for col in &cols {
                let Some((index, _)) = schema.field_by_name(col) else {
                    return Err(EngineError::column_not_found(table_name, col.clone()));
                };
                field_mapping.push(index);
            }
            field_mapping
        } else {
            (0..schema.num_fields()).collect()
        };

        if field_mapping.len() != schema.num_fields() {
            return Err(EngineError::wrong_column_count(
                table_name,
                schema.num_fields(),
                field_mapping.len(),
            ));
        }

        let tuples = values
            .into_iter()
            .map(|value| Tuple::new(value).project(&field_mapping))
            .collect::<Vec<Tuple>>();

        let heap = catalog.get_table_heap(table_info.file_id)?;
        let record_ids = heap.bulk_insert(txn.transaction_id(), tuples)?;
        Ok(StatementResult::inserted(table_name, record_ids.len()))
    };

    super::with_txn(f, txn_manager)
}

pub(super) fn delete(
    catalog: &Catalog,
    txn_manager: &TransactionManager,
    statement: DeleteStatement,
) -> Result<super::StatementResult, super::EngineError> {
    let table_name = statement.table_name;

    let f = |txn: &Transaction<'_>| -> Result<StatementResult, EngineError> {
        let table_info = catalog.get_table_info(txn, &table_name)?;
        let heap = catalog.get_table_heap(table_info.file_id)?;

        let predicate = statement
            .where_clause
            .map(|w| resolve_where_clause(&w, &table_info.schema))
            .transpose()?;

        let txn_id = txn.transaction_id();
        let rids = collect_matching(&heap, txn_id, predicate.as_ref())?
            .into_iter()
            .map(|(rid, _)| rid)
            .collect::<Vec<RecordId>>();

        let deleted = rids.len();
        for rid in rids {
            heap.delete_tuple(txn_id, rid)?;
        }

        Ok(StatementResult::deleted(table_name, deleted))
    };

    super::with_txn(f, txn_manager)
}

fn collect_matching(
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

pub(super) fn update(
    catalog: &Catalog,
    txn_manager: &TransactionManager,
    statement: UpdateStatement,
) -> Result<super::StatementResult, super::EngineError> {
    let UpdateStatement {
        table_name,
        assignments,
        where_clause,
        ..
    } = statement;

    let f = |txn: &Transaction<'_>| -> Result<StatementResult, EngineError> {
        let TableInfo {
            schema, file_id, ..
        } = catalog.get_table_info(txn, &table_name)?;
        let heap_file = catalog.get_table_heap(file_id)?;

        let resolved_assignments =
            resolve_assignments(assignments.as_slice(), &schema, &table_name)?;

        let predicate = where_clause
            .map(|w| resolve_where_clause(&w, &schema))
            .transpose()?;

        let txn_id = txn.transaction_id();
        let rows = collect_matching(&heap_file, txn_id, predicate.as_ref())?;
        let updated = rows.len();

        for (rid, mut tuple) in rows {
            for (idx, value) in &resolved_assignments {
                tuple
                    .set_field(*idx, value.clone(), &schema)
                    .map_err(|e| EngineError::type_error(e.to_string()))?;
            }
            heap_file.update_tuple(txn_id, rid, &tuple)?;
        }

        Ok(StatementResult::updated(table_name, updated))
    };

    super::with_txn(f, txn_manager)
}

fn resolve_assignments(
    assignments: &[Assignment],
    schema: &TupleSchema,
    table_name: &str,
) -> Result<Vec<(usize, Value)>, EngineError> {
    let mut field_mapping = Vec::with_capacity(assignments.len());
    for Assignment { column, value } in assignments {
        let (idx, _) = schema
            .field_by_name(column)
            .ok_or_else(|| EngineError::column_not_found(table_name, column))?;
        field_mapping.push((idx, value.clone()));
    }
    Ok(field_mapping)
}

fn resolve_where_clause(
    w: &WhereCondition,
    schema: &TupleSchema,
) -> Result<BooleanExpression, EngineError> {
    match w {
        WhereCondition::Predicate { field, op, value } => {
            let (idx, field_def) = schema
                .field_by_name(field)
                .ok_or_else(|| EngineError::column_not_found("?", field.clone()))?;
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
