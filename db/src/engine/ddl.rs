use crate::{
    catalog::manager::Catalog,
    engine::{EngineError, StatementResult},
    parser::statements::{ColumnDef, CreateTableStatement, DropStatement},
    transaction::TransactionManager,
    tuple::TupleSchema,
};

pub(super) fn create_table(
    catalog: &Catalog,
    txn_manager: &TransactionManager,
    statement: CreateTableStatement,
) -> Result<StatementResult, EngineError> {
    let table_name = statement.table_name;

    if statement.if_not_exists && catalog.table_exists(&table_name) {
        let table_info = catalog.get_table_info(&txn_manager.begin()?, &table_name)?;
        return Ok(StatementResult::table_created(
            table_name,
            table_info.file_id,
            true,
        ));
    }

    let schema = TupleSchema::from(statement.columns.iter().collect::<Vec<&ColumnDef>>());

    let pk_col_name = statement
        .columns
        .iter()
        .find(|c| c.primary_key)
        .map(|c| c.name.clone())
        .or(statement.primary_key.clone());

    let primary_key = pk_col_name
        .as_deref()
        .and_then(|name| schema.field_by_name(name).map(|(i, _)| vec![i]));

    let file_id = super::with_txn(
        |txn| {
            catalog
                .create_table(txn, &table_name, schema, primary_key)
                .map_err(Into::into)
        },
        txn_manager,
    )?;

    Ok(StatementResult::table_created(table_name, file_id, false))
}

pub(super) fn drop_table(
    catalog: &Catalog,
    txn_manager: &TransactionManager,
    statement: DropStatement,
) -> Result<StatementResult, EngineError> {
    let table_name = statement.table_name;

    if statement.if_exists && !catalog.table_exists(&table_name) {
        return Ok(StatementResult::table_dropped(table_name));
    }

    super::with_txn(
        |txn| catalog.drop_table(txn, &table_name).map_err(Into::into),
        txn_manager,
    )?;

    Ok(StatementResult::table_dropped(table_name))
}
