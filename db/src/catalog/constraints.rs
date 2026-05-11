//! Constraint management for the system catalog.
//!
//! Extends [`Catalog`] with operations to add, drop, and reload UNIQUE and
//! FOREIGN KEY constraints. Mirrors the layered approach used for primary
//! keys in `table.rs`:
//!
//! 1. Mutate the system tables (`CATALOG_CONSTRAINTS`, `CATALOG_CONSTRAINT_COLUMNS`,
//!    `CATALOG_FK_CONSTRAINTS`) inside the supplied transaction.
//! 2. Refresh the in-memory `TableInfo` so the change is visible without a cache reload.
//!
//! See `db/src/catalog/systable.rs` for the row layouts and the multi-row
//! pattern used to encode composite constraints.

use std::collections::HashMap;

use crate::{
    FileId,
    catalog::{
        CatalogError, ForeignKey, TableInfo, UniqueConstraint,
        manager::Catalog,
        systable::{
            ConstraintColumnRow, ConstraintKind, ConstraintRow, FkAction, FkConstraintRow, IndexRow,
        },
    },
    primitives::{ColumnId, IndexId, NonEmptyString},
    transaction::Transaction,
};

impl Catalog {
    /// Adds a UNIQUE constraint to a table.
    ///
    /// Writes one `ConstraintRow` header (kind = `Unique`) and one
    /// `ConstraintColumnRow` per participating column, then refreshes
    /// the cached [`TableInfo::unique_constraints`].
    ///
    /// `columns` order is preserved as `ordinal` in the catalog — `(a, b)` and
    /// `(b, a)` are different UNIQUE constraints because index-prefix lookups
    /// depend on order.
    pub fn add_unique_constraint(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        name: &str,
        columns: &[ColumnId],
        backing_index_id: Option<IndexId>,
    ) -> Result<(), CatalogError> {
        if columns.is_empty() {
            return Err(CatalogError::invalid_catalog_row(
                "UNIQUE constraint must list at least one column",
            ));
        }

        let mut table = self.get_table_info_by_id(txn, table_id)?;
        validate_columns_in_schema(&table, columns, "UNIQUE")?;
        self.reject_duplicate_constraint_name(txn, table_id, name)?;

        let constraint_name = NonEmptyString::try_from(name)?;

        self.insert_systable_tuple(txn, &ConstraintRow {
            constraint_name: constraint_name.clone(),
            table_id,
            constraint_kind: ConstraintKind::Unique,
            expr: None,
            backing_index_id,
        })?;

        for (ordinal, &col_id) in columns.iter().enumerate() {
            let ordinal = i32_ordinal(ordinal)?;
            self.insert_systable_tuple(txn, &ConstraintColumnRow {
                constraint_name: constraint_name.clone(),
                table_id,
                column_id: col_id,
                ordinal,
            })?;
        }

        table.unique_constraints.push(UniqueConstraint {
            name: constraint_name,
            columns: columns.to_vec(),
            backing_index_id,
        });
        self.refresh_cached_table(table);
        Ok(())
    }

    /// Adds an outgoing FOREIGN KEY constraint to a table.
    ///
    /// `local_columns[i]` is constrained to reference `ref_columns[i]` in the
    /// parent table. Both vectors must have the same length. The referenced
    /// columns must form a UNIQUE or PRIMARY KEY in the parent — without that,
    /// the FK has no guarantee that its lookup target is unique.
    #[allow(clippy::too_many_arguments)]
    pub fn add_foreign_key(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        name: &str,
        local_columns: &[ColumnId],
        ref_table_id: FileId,
        ref_columns: &[ColumnId],
        on_delete: Option<FkAction>,
        on_update: Option<FkAction>,
    ) -> Result<(), CatalogError> {
        if local_columns.is_empty() {
            return Err(CatalogError::invalid_catalog_row(
                "FOREIGN KEY must list at least one column",
            ));
        }
        if local_columns.len() != ref_columns.len() {
            return Err(CatalogError::invalid_catalog_row(format!(
                "FOREIGN KEY column count mismatch: {} local vs {} referenced",
                local_columns.len(),
                ref_columns.len()
            )));
        }

        let mut table = self.get_table_info_by_id(txn, table_id)?;
        validate_columns_in_schema(&table, local_columns, "FOREIGN KEY")?;
        self.reject_duplicate_constraint_name(txn, table_id, name)?;

        let ref_table = self.get_table_info_by_id(txn, ref_table_id)?;
        validate_columns_in_schema(&ref_table, ref_columns, "FOREIGN KEY referenced")?;
        if !references_unique_or_pk(&ref_table, ref_columns) {
            return Err(CatalogError::invalid_catalog_row(format!(
                "FOREIGN KEY referenced columns in table {} are not a PRIMARY KEY or UNIQUE",
                ref_table.name.as_str()
            )));
        }

        let constraint_name = NonEmptyString::try_from(name)?;
        self.insert_systable_tuple(txn, &ConstraintRow {
            constraint_name: constraint_name.clone(),
            table_id,
            constraint_kind: ConstraintKind::ForeignKey,
            expr: None,
            backing_index_id: None,
        })?;

        for (ordinal, (&local, &referenced)) in
            local_columns.iter().zip(ref_columns.iter()).enumerate()
        {
            let ordinal = i32_ordinal(ordinal)?;
            self.insert_systable_tuple(txn, &FkConstraintRow {
                constraint_name: constraint_name.clone(),
                table_id,
                local_column_id: local,
                ordinal,
                ref_table_id,
                ref_column_id: referenced,
                on_delete,
                on_update,
            })?;
        }

        table.foreign_keys.push(ForeignKey {
            name: constraint_name,
            local_columns: local_columns.to_vec(),
            ref_table_id,
            ref_columns: ref_columns.to_vec(),
            on_delete,
            on_update,
        });
        self.refresh_cached_table(table);
        Ok(())
    }

    /// Drops a constraint (UNIQUE or FK) by name.
    ///
    /// PRIMARY KEY is dropped via [`Catalog::drop_primary_key`] (it lives in
    /// its own system table for now). NOT NULL is a column attribute on
    /// `CATALOG_COLUMNS`, not a row in `CATALOG_CONSTRAINTS`.
    pub fn drop_constraint(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        name: &str,
    ) -> Result<(), CatalogError> {
        let mut table = self.get_table_info_by_id(txn, table_id)?;

        let header = self
            .scan_system_table_where::<ConstraintRow, _>(txn, |r| {
                r.table_id == table_id && r.constraint_name.as_str() == name
            })?
            .pop()
            .ok_or_else(|| CatalogError::ConstraintNotFound {
                table: table.name.as_str().to_owned(),
                constraint: name.to_owned(),
            })?;

        let backing_index_name = if header.constraint_kind == ConstraintKind::Unique {
            if let Some(idx_id) = header.backing_index_id {
                let rows =
                    self.scan_system_table_where::<IndexRow, _>(txn, |r| r.index_id == idx_id)?;
                rows.into_iter()
                    .next()
                    .map(|r| r.index_name.as_str().to_owned())
            } else {
                None
            }
        } else {
            None
        };

        match header.constraint_kind {
            ConstraintKind::Unique => {
                self.delete_systable_rows::<ConstraintColumnRow, _>(txn, |r| {
                    r.table_id == table_id && r.constraint_name.as_str() == name
                })?;
                table.unique_constraints.retain(|u| u.name.as_str() != name);
            }
            ConstraintKind::ForeignKey => {
                self.delete_systable_rows::<FkConstraintRow, _>(txn, |r| {
                    r.table_id == table_id && r.constraint_name.as_str() == name
                })?;
                table.foreign_keys.retain(|f| f.name.as_str() != name);
            }
            ConstraintKind::Check => {
                // Predicate lives on the header row; nothing else to delete.
            }
            ConstraintKind::PrimaryKey => {
                return Err(CatalogError::invalid_catalog_row(
                    "use drop_primary_key to remove a PRIMARY KEY",
                ));
            }
            ConstraintKind::NotNull => {
                return Err(CatalogError::invalid_catalog_row(
                    "NOT NULL is a column attribute; alter the column instead",
                ));
            }
        }

        self.delete_systable_rows::<ConstraintRow, _>(txn, |r| {
            r.table_id == table_id && r.constraint_name.as_str() == name
        })?;

        if let Some(index_name) = backing_index_name {
            self.drop_index(txn, &index_name)?;
        }

        self.refresh_cached_table(table);
        Ok(())
    }

    /// Reconstructs UNIQUE and FK metadata from already-fetched system rows.
    ///
    /// Designed to be called from `install_table` (in `table.rs`) once that
    /// path is updated to scan the constraint system tables alongside columns
    /// and primary-key columns. Pulled out as a free helper so the cache-miss
    /// load path and the eager `replay_tables` startup path can share the same
    /// reduction logic.
    pub(super) fn build_constraints_for_table(
        table: &mut TableInfo,
        headers: Vec<ConstraintRow>,
        column_rows: Vec<ConstraintColumnRow>,
        fk_rows: Vec<FkConstraintRow>,
    ) -> Result<(), CatalogError> {
        let mut cols_by_name: HashMap<String, Vec<ConstraintColumnRow>> = HashMap::new();
        for r in column_rows {
            cols_by_name
                .entry(r.constraint_name.as_str().to_owned())
                .or_default()
                .push(r);
        }

        let mut fks_by_name: HashMap<String, Vec<FkConstraintRow>> = HashMap::new();
        for r in fk_rows {
            fks_by_name
                .entry(r.constraint_name.as_str().to_owned())
                .or_default()
                .push(r);
        }

        for header in headers {
            match header.constraint_kind {
                ConstraintKind::Unique => {
                    let mut rows = cols_by_name
                        .remove(header.constraint_name.as_str())
                        .unwrap_or_default();
                    rows.sort_by_key(|r| r.ordinal);
                    table.unique_constraints.push(UniqueConstraint {
                        name: header.constraint_name,
                        columns: rows.into_iter().map(|r| r.column_id).collect(),
                        backing_index_id: header.backing_index_id,
                    });
                }
                ConstraintKind::ForeignKey => {
                    let mut rows = fks_by_name
                        .remove(header.constraint_name.as_str())
                        .unwrap_or_default();
                    rows.sort_by_key(|r| r.ordinal);
                    if rows.is_empty() {
                        return Err(CatalogError::invalid_catalog_row(format!(
                            "FOREIGN KEY {} has header but no column rows",
                            header.constraint_name.as_str()
                        )));
                    }
                    let ref_table_id = rows[0].ref_table_id;
                    let on_delete = rows[0].on_delete;
                    let on_update = rows[0].on_update;
                    let local_columns = rows.iter().map(|r| r.local_column_id).collect();
                    let ref_columns = rows.iter().map(|r| r.ref_column_id).collect();
                    table.foreign_keys.push(ForeignKey {
                        name: header.constraint_name,
                        local_columns,
                        ref_table_id,
                        ref_columns,
                        on_delete,
                        on_update,
                    });
                }
                // PrimaryKey is reconstructed from CATALOG_PRIMARY_KEY_COLUMNS,
                // not from these rows. Check has no detail rows. NotNull is a
                // column attribute. Drop them silently here.
                ConstraintKind::PrimaryKey | ConstraintKind::Check | ConstraintKind::NotNull => {}
            }
        }

        Ok(())
    }

    /// Replaces the cache entry for `table` under its current name.
    ///
    /// Mirrors the pattern at the bottom of `set_primary_key` / `drop_primary_key`:
    /// because `name` is the cache key, a remove+insert keeps the map coherent
    /// even if the caller mutated `table.name` (which today they don't, but
    /// the pattern is intentionally defensive).
    fn refresh_cached_table(&self, table: TableInfo) {
        let mut cache = self.user_tables.write();
        cache.remove(table.name.as_str());
        cache.insert(table.name.clone(), table);
    }

    /// Errors if a constraint named `name` already exists on `table_id`.
    fn reject_duplicate_constraint_name(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        name: &str,
    ) -> Result<(), CatalogError> {
        let existing = self.scan_system_table_where::<ConstraintRow, _>(txn, |r| {
            r.table_id == table_id && r.constraint_name.as_str() == name
        })?;
        if !existing.is_empty() {
            return Err(CatalogError::invalid_catalog_row(format!(
                "constraint {name} already exists on table {table_id}"
            )));
        }
        Ok(())
    }
}

/// Returns true if `columns` (as a set) equals the table's PK or any UNIQUE
/// constraint. FKs require the referenced column list to be the *full* key of
/// some uniqueness constraint — a prefix is not enough.
fn references_unique_or_pk(table: &TableInfo, columns: &[ColumnId]) -> bool {
    let target: std::collections::HashSet<ColumnId> = columns.iter().copied().collect();

    if let Some(pk) = &table.primary_key
        && pk.len() == target.len()
        && pk.iter().copied().collect::<std::collections::HashSet<_>>() == target
    {
        return true;
    }

    table.unique_constraints.iter().any(|u| {
        u.columns.len() == target.len()
            && u.columns
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                == target
    })
}

/// Bounds-check every column id against the table schema.
fn validate_columns_in_schema(
    table: &TableInfo,
    columns: &[ColumnId],
    label: &str,
) -> Result<(), CatalogError> {
    for &col in columns {
        if table.schema.field(usize::from(col)).is_none() {
            return Err(CatalogError::invalid_catalog_row(format!(
                "{label} column index {} out of bounds for table {}",
                usize::from(col),
                table.name.as_str()
            )));
        }
    }
    Ok(())
}

/// Converts a `usize` ordinal to the on-disk `i32`, matching the pattern in
/// `insert_primary_key_columns`.
fn i32_ordinal(ordinal: usize) -> Result<i32, CatalogError> {
    i32::try_from(ordinal)
        .map_err(|_| CatalogError::invalid_catalog_row("constraint ordinal does not fit in i32"))
}
