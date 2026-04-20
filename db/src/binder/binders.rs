pub(super) mod ddl {
    use std::collections::HashSet;

    use crate::{
        binder::{BindError, BoundCreateTable, BoundDrop},
        catalog::{
            CatalogError,
            manager::{Catalog, TableInfo},
        },
        parser::statements::{ColumnDef, CreateTableStatement, DropStatement},
        transaction::Transaction,
        tuple::TupleSchema,
    };

    pub(in crate::binder) fn bind_drop(
        stmt: &DropStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<BoundDrop, BindError> {
        let table_info = catalog.get_table_info(txn, &stmt.table_name);

        match table_info {
            Ok(TableInfo { name, file_id, .. }) => Ok(BoundDrop::Drop {
                name: name.clone(),
                file_id,
            }),
            Err(CatalogError::TableNotFound { .. }) if stmt.if_exists => Ok(BoundDrop::NoOp {
                name: stmt.table_name.clone(),
            }),
            Err(CatalogError::TableNotFound { table_name }) => {
                Err(BindError::unknown_table(table_name))
            }
            Err(other) => Err(other.into()),
        }
    }

    pub(in crate::binder) fn bind_create_table(
        stmt: &CreateTableStatement,
        catalog: &Catalog,
        txn: &Transaction<'_>,
    ) -> Result<BoundCreateTable, BindError> {
        let CreateTableStatement {
            table_name,
            columns,
            ..
        } = stmt;

        if catalog.table_exists(table_name) {
            if stmt.if_not_exists {
                let info = catalog.get_table_info(txn, table_name)?;
                return Ok(BoundCreateTable::AlreadyExists {
                    name: table_name.clone(),
                    file_id: info.file_id,
                });
            }
            return Err(BindError::table_already_exists(table_name));
        }

        let mut seen = HashSet::with_capacity(columns.len());
        columns
            .iter()
            .try_for_each(|col| -> Result<(), BindError> {
                if seen.insert(col.name.as_str()) {
                    Ok(())
                } else {
                    Err(BindError::duplicate_column(col.name.as_str()))
                }
            })?;

        let schema = TupleSchema::from(columns.iter().collect::<Vec<&ColumnDef>>());
        let pk_name = stmt
            .columns
            .iter()
            .find(|c| c.primary_key)
            .map(|c| c.name.as_str())
            .or(stmt.primary_key.as_deref());

        let primary_key = match pk_name {
            None => None,
            Some(name) => match schema.field_by_name(name) {
                Some((idx, _)) => Some(vec![idx]),
                None => return Err(BindError::PrimaryKeyNotInColumns(name.to_string())),
            },
        };

        Ok(BoundCreateTable::New {
            name: table_name.clone(),
            schema,
            primary_key,
        })
    }
}
