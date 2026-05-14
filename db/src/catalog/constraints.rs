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

use std::collections::{HashMap, HashSet};

use crate::{
    FileId,
    catalog::{
        CachedCheckConstraint, CatalogError, ForeignKey, ReferencingFk, TableInfo,
        UniqueConstraint,
        manager::Catalog,
        systable::{
            ConstraintColumnRow, ConstraintKind, ConstraintRow, FkAction, FkConstraintRow, IndexRow,
        },
    },
    parser::Parser,
    primitives::{ColumnId, IndexId, NonEmptyString},
    transaction::Transaction,
};

/// A constraint definition passed to [`Catalog::add_constraint`].
///
/// Each variant carries the fields specific to that constraint kind. Use this
/// instead of calling the per-kind methods directly — `add_constraint` is the
/// single public entry point for writing any constraint to the catalog.
pub enum ConstraintDef {
    Unique {
        name: String,
        columns: Vec<ColumnId>,
        backing_index_id: Option<IndexId>,
    },
    ForeignKey {
        name: String,
        local_columns: Vec<ColumnId>,
        ref_table_id: FileId,
        ref_columns: Vec<ColumnId>,
        on_delete: Option<FkAction>,
        on_update: Option<FkAction>,
    },
    /// Registers a CHECK expression in the catalog. Enforcement is not yet
    /// implemented — the expression is stored but never evaluated at DML time.
    Check { name: String, expr: String },
    /// Sets the primary key via the dedicated PK system table. Unlike the other
    /// variants there is no constraint name — PKs are identified by table only.
    PrimaryKey { columns: Vec<ColumnId> },
}

impl Catalog {
    /// Single entry point for adding any constraint to a table.
    pub fn add_constraint(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        def: ConstraintDef,
    ) -> Result<(), CatalogError> {
        match def {
            ConstraintDef::Unique {
                name,
                columns,
                backing_index_id,
            } => self.add_unique_constraint(txn, table_id, &name, &columns, backing_index_id),
            ConstraintDef::ForeignKey {
                name,
                local_columns,
                ref_table_id,
                ref_columns,
                on_delete,
                on_update,
            } => self.add_foreign_key(
                txn,
                table_id,
                &name,
                &local_columns,
                ref_table_id,
                &ref_columns,
                on_delete,
                on_update,
            ),
            ConstraintDef::Check { name, expr } => {
                let constraint_name = NonEmptyString::try_from(name.as_str())?;
                self.reject_duplicate_constraint_name(txn, table_id, &name)?;
                self.insert_systable_tuple(txn, &ConstraintRow {
                    constraint_name,
                    table_id,
                    constraint_kind: ConstraintKind::Check,
                    expr: Some(NonEmptyString::try_from(expr.as_str())?),
                    backing_index_id: None,
                })?;
                Ok(())
            }
            ConstraintDef::PrimaryKey { columns } => self.set_primary_key(txn, table_id, columns),
        }
    }

    /// Adds a UNIQUE constraint to a table.
    ///
    /// Writes one header row to `CATALOG_CONSTRAINTS` (kind UNIQUE, optional
    /// `backing_index_id` when the engine already created an enforcing index) and one
    /// `CATALOG_CONSTRAINT_COLUMNS` row per listed column. `columns` order is stored as
    /// `ordinal` and is semantically significant: `(a, b)` and `(b, a)` are different
    /// constraints for prefix-based uniqueness checks.
    ///
    /// Updates the in-memory [`TableInfo::unique_constraints`] cache for `table_id`.
    /// Prefer [`Catalog::add_constraint`] with [`ConstraintDef::Unique`] at call sites;
    /// this method is the internal implementation shared from that entry point.
    ///
    /// # Errors
    ///
    /// - Empty `columns`, unknown columns for the table, or a duplicate constraint name.
    /// - Name validation failures when `name` is not a valid [`NonEmptyString`].
    /// - `CatalogError` if an ordinal does not fit in `i32` or system-table inserts / cache refresh
    ///   fail.
    fn add_unique_constraint(
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

        Self::validate_columns_in_schema(&table, columns, "UNIQUE")?;
        self.reject_duplicate_constraint_name(txn, table_id, name)?;

        let constraint_name = NonEmptyString::try_from(name)?;
        let constraint_row = ConstraintRow {
            constraint_name: constraint_name.clone(),
            table_id,
            constraint_kind: ConstraintKind::Unique,
            expr: None,
            backing_index_id,
        };
        self.insert_systable_tuple(txn, &constraint_row)?;

        for (ordinal, &column_id) in columns.iter().enumerate() {
            let constraint_column_row = ConstraintColumnRow {
                constraint_name: constraint_name.clone(),
                table_id,
                column_id,
                ordinal: i32::try_from(ordinal).map_err(|_| {
                    CatalogError::invalid_catalog_row("constraint ordinal does not fit in i32")
                })?,
            };
            self.insert_systable_tuple(txn, &constraint_column_row)?;
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
    fn add_foreign_key(
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
        Self::validate_columns_in_schema(&table, local_columns, "FOREIGN KEY")?;

        let ref_table = self.get_table_info_by_id(txn, ref_table_id)?;
        Self::validate_columns_in_schema(&ref_table, ref_columns, "FOREIGN KEY referenced")?;

        self.reject_duplicate_constraint_name(txn, table_id, name)?;
        if !Self::references_unique_or_pk(&ref_table, ref_columns) {
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
            let ordinal = i32::try_from(ordinal).map_err(|_| {
                CatalogError::invalid_catalog_row("constraint ordinal does not fit in i32")
            })?;
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

    /// Returns whether `columns` names the same set of columns as an enforced unique key on
    /// `table`.
    ///
    /// Used when validating a FOREIGN KEY: the referenced column list must target a PRIMARY KEY
    /// or a UNIQUE constraint on the parent [`TableInfo`]. Matching is by **set** equality of
    /// [`ColumnId`] values (order in `columns` vs order in the PK / UNIQUE definition does not
    /// matter for this predicate).
    fn references_unique_or_pk(table: &TableInfo, columns: &[ColumnId]) -> bool {
        let target: HashSet<ColumnId> = columns.iter().copied().collect();

        if let Some(pk) = &table.primary_key
            && pk.len() == target.len()
            && pk.iter().copied().collect::<HashSet<_>>() == target
        {
            return true;
        }

        table.unique_constraints.iter().any(|u| {
            u.columns.len() == target.len()
                && u.columns.iter().copied().collect::<HashSet<_>>() == target
        })
    }

    /// Lists every foreign key in the catalog whose referenced table is `ref_table_id`.
    ///
    /// Scans the foreign-key constraint system table for rows where `ref_table_id` matches. A
    /// single logical FK is stored as one row per column pair; this method groups rows by
    /// `constraint_name`, sorts each group by `ordinal`, and folds them into one
    /// [`ReferencingFk`] with `column_pairs` in declaration order. `on_delete` / `on_update`
    /// are read from the first row of each group (they are identical across ordinals when the
    /// constraint was written).
    ///
    /// Callers use this to implement parent-side rules (for example blocking a delete or
    /// update on the referenced table while child rows still point at the key).
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] if the system table scan fails.
    pub fn find_referencing_fks(
        &self,
        txn: &Transaction<'_>,
        ref_table_id: FileId,
    ) -> Result<Vec<ReferencingFk>, CatalogError> {
        let fk_rows = self.scan_system_table_where::<FkConstraintRow, _>(txn, |r| {
            r.ref_table_id == ref_table_id
        })?;

        let mut by_name: HashMap<NonEmptyString, Vec<FkConstraintRow>> = HashMap::new();
        for row in fk_rows {
            if let Some(vec) = by_name.get_mut(row.constraint_name.as_str()) {
                vec.push(row);
            } else {
                let key = row.constraint_name.clone();
                by_name.insert(key, vec![row]);
            }
        }

        Ok(by_name
            .into_iter()
            .map(|(name, mut rows)| {
                rows.sort_by_key(|r| r.ordinal);
                let first = &rows[0];

                ReferencingFk {
                    constraint_name: name.into_inner(),
                    child_table_id: first.table_id,
                    on_delete: first.on_delete,
                    on_update: first.on_update,
                    column_pairs: rows
                        .into_iter()
                        .map(|r| (r.local_column_id, r.ref_column_id))
                        .collect(),
                }
            })
            .collect())
    }

    /// Drops a named constraint on `table_id` inside `txn`.
    ///
    /// Loads the constraint header row from `CATALOG_CONSTRAINTS` to learn the kind, then deletes
    /// the appropriate companion rows (`ConstraintColumnRow` for UNIQUE,
    /// `FkConstraintRow` for FOREIGN KEY), removes the header from
    /// `CATALOG_CONSTRAINTS`, drops any UNIQUE backing index, and refreshes the
    /// in-memory [`TableInfo`] for that table.
    ///
    /// PRIMARY KEY and NOT NULL are rejected here so callers use the dedicated
    /// catalog paths (`drop_primary_key`, column alter) instead of treating them
    /// as generic constraint rows.
    ///
    /// # Errors
    ///
    /// - [`CatalogError::ConstraintNotFound`] — no constraint named `name` on that table.
    /// - `CatalogError` from `invalid_catalog_row` when the constraint is PRIMARY KEY or NOT NULL
    ///   (those use other catalog APIs, not generic constraint drop).
    /// - Other [`CatalogError`] variants from system-table deletes, index drop, or cache refresh.
    pub fn drop_constraint(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        name: &str,
    ) -> Result<(), CatalogError> {
        let mut table = self.get_table_info_by_id(txn, table_id)?;
        let header = self.fetch_constraint_header(txn, table_id, name, table.name.as_str())?;

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
            ConstraintKind::PrimaryKey => unreachable!(
                "set_primary_key never writes a ConstraintRow; \
                 use drop_primary_key instead"
            ),
            ConstraintKind::NotNull => {
                unreachable!("NOT NULL is a column attribute, never written as a ConstraintRow")
            }
        }

        self.delete_systable_rows::<ConstraintRow, _>(txn, |r| {
            r.table_id == table_id && r.constraint_name.as_str() == name
        })?;

        self.delete_constraint_indexes(txn, &header)?;
        self.refresh_cached_table(table);
        Ok(())
    }

    /// Looks up the `CATALOG_CONSTRAINTS` header row for `constraint_name` on `table_id`.
    ///
    /// `table_name` is only used to populate [`CatalogError::ConstraintNotFound`]; it must
    /// match the human-readable table name for that `table_id` (callers typically pass
    /// [`TableInfo::name`]).
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::ConstraintNotFound`] when no matching header exists, or other
    /// [`CatalogError`] values if the system table scan fails.
    fn fetch_constraint_header(
        &self,
        txn: &Transaction<'_>,
        table_id: FileId,
        constraint_name: &str,
        table_name: &str,
    ) -> Result<ConstraintRow, CatalogError> {
        self.scan_system_table_where::<ConstraintRow, _>(txn, |r| {
            r.table_id == table_id && r.constraint_name.as_str() == constraint_name
        })?
        .pop()
        .ok_or_else(|| CatalogError::ConstraintNotFound {
            table: table_name.to_owned(),
            constraint: constraint_name.to_owned(),
        })
    }

    /// Drops the secondary index backing a UNIQUE constraint, if any.
    ///
    /// UNIQUE constraints may record a backing index id on the header row. When present, this
    /// resolves the index name from `CATALOG_INDEXES` and calls [`Catalog::drop_index`]. Other
    /// constraint kinds are no-ops.
    ///
    /// # Errors
    ///
    /// Propagates errors from index metadata lookup or [`Catalog::drop_index`].
    fn delete_constraint_indexes(
        &self,
        txn: &Transaction<'_>,
        header: &ConstraintRow,
    ) -> Result<(), CatalogError> {
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

        if let Some(index_name) = backing_index_name {
            self.drop_index(txn, &index_name)?;
        }
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
                ConstraintKind::Check => {
                    let expr_str = header.expr.ok_or_else(|| {
                        CatalogError::invalid_catalog_row(format!(
                            "CHECK constraint '{}' has no expression stored",
                            header.constraint_name.as_str()
                        ))
                    })?;
                    let expr = Parser::parse_expr(expr_str.as_str()).map_err(|e| {
                        CatalogError::invalid_catalog_row(format!(
                            "CHECK constraint '{}' has unparseable expression: {e}",
                            header.constraint_name.as_str()
                        ))
                    })?;
                    table.check_constraints.push(CachedCheckConstraint {
                        name: header.constraint_name.as_str().to_owned(),
                        expr,
                    });
                }
                // PrimaryKey is reconstructed from CATALOG_PRIMARY_KEY_COLUMNS,
                // not from these rows. NotNull is a column attribute.
                ConstraintKind::PrimaryKey | ConstraintKind::NotNull => {}
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
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        Type,
        buffer_pool::page_store::PageStore,
        transaction::TransactionManager,
        tuple::{Field, TupleSchema},
        wal::writer::Wal,
    };

    fn make_infra(dir: &Path) -> (Arc<Wal>, Arc<PageStore>) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("WAL creation failed"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        (wal, bp)
    }

    fn make_catalog_and_txn(dir: &Path) -> (Catalog, TransactionManager) {
        let (wal, bp) = make_infra(dir);
        let catalog = Catalog::initialize(&bp, &wal, dir).expect("catalog init failed");
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    fn field(name: &str, ty: Type) -> Field {
        Field::new(name, ty).unwrap()
    }

    /// Three-column schema: id (Uint64), email (String), name (String).
    fn three_col_schema() -> TupleSchema {
        TupleSchema::new(vec![
            field("id", Type::Uint64).not_null(),
            field("email", Type::String).not_null(),
            field("name", Type::String).not_null(),
        ])
    }

    fn col(id: usize) -> ColumnId {
        ColumnId::try_from(id).unwrap()
    }

    #[test]
    fn add_unique_single_column_visible_immediately() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_unique_constraint(&txn, file_id, "users_unique_email", &[col(1)], None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "users").unwrap();
        txn2.commit().unwrap();

        assert_eq!(info.unique_constraints.len(), 1);
        assert_eq!(
            info.unique_constraints[0].name.as_str(),
            "users_unique_email"
        );
        assert_eq!(info.unique_constraints[0].columns, vec![col(1)]);
    }

    #[test]
    fn add_unique_composite_columns_preserved_in_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_unique_constraint(
                &txn,
                file_id,
                "users_unique_email_name",
                &[col(1), col(2)],
                None,
            )
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "users").unwrap();
        txn2.commit().unwrap();

        assert_eq!(info.unique_constraints[0].columns, vec![col(1), col(2)]);
    }

    #[test]
    fn add_unique_constraint_survives_cache_evict_and_reload() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_unique_constraint(&txn, file_id, "users_unique_email", &[col(1)], None)
            .unwrap();
        txn.commit().unwrap();

        catalog.user_tables.write().remove("users");

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "users").unwrap();
        txn2.commit().unwrap();

        assert_eq!(info.unique_constraints.len(), 1);
        assert_eq!(
            info.unique_constraints[0].name.as_str(),
            "users_unique_email"
        );
    }

    #[test]
    fn add_multiple_unique_constraints_all_round_trip() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_unique_constraint(&txn, file_id, "users_unique_email", &[col(1)], None)
            .unwrap();
        catalog
            .add_unique_constraint(&txn, file_id, "users_unique_name", &[col(2)], None)
            .unwrap();
        txn.commit().unwrap();

        catalog.user_tables.write().remove("users");

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "users").unwrap();
        txn2.commit().unwrap();

        assert_eq!(info.unique_constraints.len(), 2);
        let names: Vec<_> = info
            .unique_constraints
            .iter()
            .map(|u| u.name.as_str())
            .collect();
        assert!(names.contains(&"users_unique_email"));
        assert!(names.contains(&"users_unique_name"));
    }

    // ── add_unique_constraint: error paths ────────────────────────────────

    #[test]
    fn add_unique_empty_column_list_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();

        let result = catalog.add_unique_constraint(&txn, file_id, "bad", &[], None);

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow for empty column list, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    #[test]
    fn add_unique_out_of_bounds_column_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();

        let result = catalog.add_unique_constraint(&txn, file_id, "bad", &[col(99)], None);

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow for OOB column, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    #[test]
    fn add_unique_duplicate_name_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_unique_constraint(&txn, file_id, "my_unique", &[col(1)], None)
            .unwrap();

        let result = catalog.add_unique_constraint(&txn, file_id, "my_unique", &[col(2)], None);

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected error for duplicate constraint name, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    // ── drop_constraint ───────────────────────────────────────────────────

    #[test]
    fn drop_constraint_removes_from_unique_constraints() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_unique_constraint(&txn, file_id, "users_unique_email", &[col(1)], None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .drop_constraint(&txn2, file_id, "users_unique_email")
            .unwrap();
        let info = catalog.get_table_info(&txn2, "users").unwrap();
        txn2.commit().unwrap();

        assert!(info.unique_constraints.is_empty());
    }

    #[test]
    fn drop_constraint_survives_cache_evict_and_reload() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_unique_constraint(&txn, file_id, "users_unique_email", &[col(1)], None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .drop_constraint(&txn2, file_id, "users_unique_email")
            .unwrap();
        txn2.commit().unwrap();

        catalog.user_tables.write().remove("users");

        let txn3 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn3, "users").unwrap();
        txn3.commit().unwrap();

        assert!(
            info.unique_constraints.is_empty(),
            "dropped constraint must not reappear after reload"
        );
    }

    #[test]
    fn drop_constraint_nonexistent_returns_not_found() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();

        let result = catalog.drop_constraint(&txn, file_id, "ghost");

        assert!(
            matches!(result, Err(CatalogError::ConstraintNotFound { .. })),
            "expected ConstraintNotFound, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    #[test]
    fn drop_one_constraint_leaves_others_intact() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_unique_constraint(&txn, file_id, "users_unique_email", &[col(1)], None)
            .unwrap();
        catalog
            .add_unique_constraint(&txn, file_id, "users_unique_name", &[col(2)], None)
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .drop_constraint(&txn2, file_id, "users_unique_email")
            .unwrap();
        let info = catalog.get_table_info(&txn2, "users").unwrap();
        txn2.commit().unwrap();

        assert_eq!(info.unique_constraints.len(), 1);
        assert_eq!(
            info.unique_constraints[0].name.as_str(),
            "users_unique_name"
        );
    }

    #[test]
    fn add_foreign_key_single_column_visible_immediately() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(0)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
                name: "items_fk_order".to_owned(),
                local_columns: vec![col(0)],
                ref_table_id: parent_id,
                ref_columns: vec![col(0)],
                on_delete: None,
                on_update: None,
            })
            .unwrap();

        let info = catalog.get_table_info(&txn, "items").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.foreign_keys.len(), 1);
        assert_eq!(info.foreign_keys[0].name.as_str(), "items_fk_order");
        assert_eq!(info.foreign_keys[0].local_columns, vec![col(0)]);
        assert_eq!(info.foreign_keys[0].ref_table_id, parent_id);
        assert_eq!(info.foreign_keys[0].ref_columns, vec![col(0)]);
    }

    #[test]
    fn add_foreign_key_references_unique_constraint() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        // No PK — reference a UNIQUE constraint instead.
        catalog
            .add_unique_constraint(&txn, parent_id, "users_unique_email", &[col(1)], None)
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "profiles", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
                name: "profiles_fk_email".to_owned(),
                local_columns: vec![col(1)],
                ref_table_id: parent_id,
                ref_columns: vec![col(1)],
                on_delete: None,
                on_update: None,
            })
            .unwrap();

        let info = catalog.get_table_info(&txn, "profiles").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.foreign_keys.len(), 1);
        assert_eq!(info.foreign_keys[0].ref_columns, vec![col(1)]);
    }

    #[test]
    fn add_foreign_key_composite_columns_preserved_in_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(1), col(2)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
                name: "items_fk_composite".to_owned(),
                local_columns: vec![col(1), col(2)],
                ref_table_id: parent_id,
                ref_columns: vec![col(1), col(2)],
                on_delete: None,
                on_update: None,
            })
            .unwrap();

        let info = catalog.get_table_info(&txn, "items").unwrap();
        txn.commit().unwrap();

        assert_eq!(info.foreign_keys[0].local_columns, vec![col(1), col(2)]);
        assert_eq!(info.foreign_keys[0].ref_columns, vec![col(1), col(2)]);
    }

    #[test]
    fn add_foreign_key_survives_cache_evict_and_reload() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(0)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
                name: "items_fk_order".to_owned(),
                local_columns: vec![col(0)],
                ref_table_id: parent_id,
                ref_columns: vec![col(0)],
                on_delete: None,
                on_update: None,
            })
            .unwrap();
        txn.commit().unwrap();

        catalog.user_tables.write().remove("items");

        let txn2 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn2, "items").unwrap();
        txn2.commit().unwrap();

        assert_eq!(info.foreign_keys.len(), 1);
        assert_eq!(info.foreign_keys[0].name.as_str(), "items_fk_order");
    }

    #[test]
    fn add_foreign_key_empty_columns_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(0)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();

        let result = catalog.add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
            name: "bad".to_owned(),
            local_columns: vec![],
            ref_table_id: parent_id,
            ref_columns: vec![],
            on_delete: None,
            on_update: None,
        });

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow for empty column list, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    #[test]
    fn add_foreign_key_column_count_mismatch_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(0)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();

        let result = catalog.add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
            name: "bad".to_owned(),
            local_columns: vec![col(0), col(1)],
            ref_table_id: parent_id,
            ref_columns: vec![col(0)],
            on_delete: None,
            on_update: None,
        });

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow for column count mismatch, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    #[test]
    fn add_foreign_key_referenced_columns_not_unique_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        // col(1) on orders has no PK or UNIQUE — FK must be rejected.
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();

        let result = catalog.add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
            name: "bad".to_owned(),
            local_columns: vec![col(1)],
            ref_table_id: parent_id,
            ref_columns: vec![col(1)],
            on_delete: None,
            on_update: None,
        });

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow when referenced columns are not PK/UNIQUE, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    #[test]
    fn add_foreign_key_duplicate_name_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(0)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
                name: "my_fk".to_owned(),
                local_columns: vec![col(0)],
                ref_table_id: parent_id,
                ref_columns: vec![col(0)],
                on_delete: None,
                on_update: None,
            })
            .unwrap();

        let result = catalog.add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
            name: "my_fk".to_owned(),
            local_columns: vec![col(1)],
            ref_table_id: parent_id,
            ref_columns: vec![col(0)],
            on_delete: None,
            on_update: None,
        });

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow for duplicate FK name, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    #[test]
    fn find_referencing_fks_returns_child_fk() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(0)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
                name: "items_fk_order".to_owned(),
                local_columns: vec![col(0)],
                ref_table_id: parent_id,
                ref_columns: vec![col(0)],
                on_delete: None,
                on_update: None,
            })
            .unwrap();

        let refs = catalog.find_referencing_fks(&txn, parent_id).unwrap();
        txn.commit().unwrap();

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].constraint_name, "items_fk_order");
        assert_eq!(refs[0].child_table_id, child_id);
        assert_eq!(refs[0].column_pairs, vec![(col(0), col(0))]);
    }

    #[test]
    fn find_referencing_fks_empty_when_no_references() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let table_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();

        let refs = catalog.find_referencing_fks(&txn, table_id).unwrap();
        txn.commit().unwrap();

        assert!(refs.is_empty());
    }

    #[test]
    fn find_referencing_fks_groups_composite_columns_into_single_entry() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(1), col(2)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
                name: "items_fk_composite".to_owned(),
                local_columns: vec![col(1), col(2)],
                ref_table_id: parent_id,
                ref_columns: vec![col(1), col(2)],
                on_delete: None,
                on_update: None,
            })
            .unwrap();

        let refs = catalog.find_referencing_fks(&txn, parent_id).unwrap();
        txn.commit().unwrap();

        assert_eq!(
            refs.len(),
            1,
            "two FK rows must fold into one ReferencingFk"
        );
        assert_eq!(refs[0].column_pairs, vec![
            (col(1), col(1)),
            (col(2), col(2))
        ]);
    }

    // ── drop_constraint (FK) ──────────────────────────────────────────────

    #[test]
    fn drop_fk_constraint_removes_from_foreign_keys() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(0)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
                name: "items_fk_order".to_owned(),
                local_columns: vec![col(0)],
                ref_table_id: parent_id,
                ref_columns: vec![col(0)],
                on_delete: None,
                on_update: None,
            })
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .drop_constraint(&txn2, child_id, "items_fk_order")
            .unwrap();
        let info = catalog.get_table_info(&txn2, "items").unwrap();
        txn2.commit().unwrap();

        assert!(info.foreign_keys.is_empty());
    }

    #[test]
    fn drop_fk_constraint_survives_cache_evict_and_reload() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let parent_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, parent_id, ConstraintDef::PrimaryKey {
                columns: vec![col(0)],
            })
            .unwrap();
        let child_id = catalog
            .create_table(&txn, "items", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, child_id, ConstraintDef::ForeignKey {
                name: "items_fk_order".to_owned(),
                local_columns: vec![col(0)],
                ref_table_id: parent_id,
                ref_columns: vec![col(0)],
                on_delete: None,
                on_update: None,
            })
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog
            .drop_constraint(&txn2, child_id, "items_fk_order")
            .unwrap();
        txn2.commit().unwrap();

        catalog.user_tables.write().remove("items");

        let txn3 = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn3, "items").unwrap();
        txn3.commit().unwrap();

        assert!(
            info.foreign_keys.is_empty(),
            "dropped FK must not reappear after reload"
        );
    }

    // ── add_constraint: Check dispatch ────────────────────────────────────

    #[test]
    fn add_constraint_check_stored_without_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();

        let result = catalog.add_constraint(&txn, file_id, ConstraintDef::Check {
            name: "age_positive".to_owned(),
            expr: "age > 0".to_owned(),
        });
        txn.commit().unwrap();

        assert!(
            result.is_ok(),
            "add_constraint Check should succeed, got: {result:?}"
        );
    }

    #[test]
    fn add_constraint_check_duplicate_name_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "users", three_col_schema(), vec![])
            .unwrap();
        catalog
            .add_constraint(&txn, file_id, ConstraintDef::Check {
                name: "chk".to_owned(),
                expr: "id > 0".to_owned(),
            })
            .unwrap();

        let result = catalog.add_constraint(&txn, file_id, ConstraintDef::Check {
            name: "chk".to_owned(),
            expr: "id > 1".to_owned(),
        });

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected error for duplicate Check name, got: {result:?}"
        );
        txn.commit().unwrap();
    }

    #[test]
    fn build_constraints_fk_header_with_no_detail_rows_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(&txn, "t", three_col_schema(), vec![])
            .unwrap();
        let mut table = catalog.get_table_info(&txn, "t").unwrap();
        txn.commit().unwrap();

        let orphan_header = ConstraintRow {
            constraint_name: NonEmptyString::try_from("orphan_fk").unwrap(),
            table_id: file_id,
            constraint_kind: ConstraintKind::ForeignKey,
            expr: None,
            backing_index_id: None,
        };

        let result =
            Catalog::build_constraints_for_table(&mut table, vec![orphan_header], vec![], vec![]);

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected error for FK header with no detail rows, got: {result:?}"
        );
    }

    // ── CHECK constraint caching ──────────────────────────────────────────────

    fn check_row(name: &str, expr: &str) -> ConstraintRow {
        ConstraintRow {
            constraint_name: NonEmptyString::try_from(name).unwrap(),
            table_id: FileId::from(0u64),
            constraint_kind: ConstraintKind::Check,
            expr: Some(NonEmptyString::try_from(expr).unwrap()),
            backing_index_id: None,
        }
    }

    #[test]
    fn build_constraints_check_row_is_parsed_and_cached() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let _file_id = catalog
            .create_table(&txn, "products", three_col_schema(), vec![])
            .unwrap();
        let mut info = catalog.get_table_info(&txn, "products").unwrap();
        txn.commit().unwrap();

        Catalog::build_constraints_for_table(
            &mut info,
            vec![check_row("price_positive", "id > 0")],
            vec![],
            vec![],
        )
        .unwrap();

        assert_eq!(info.check_constraints.len(), 1);
        assert_eq!(info.check_constraints[0].name, "price_positive");
        assert!(
            matches!(
                &info.check_constraints[0].expr,
                crate::parser::statements::Expr::BinaryOp {
                    op: crate::parser::statements::BinOp::Gt,
                    ..
                }
            ),
            "expected Gt binary op, got: {:?}",
            info.check_constraints[0].expr
        );
    }

    #[test]
    fn build_constraints_multiple_check_rows_all_cached() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let _file_id = catalog
            .create_table(&txn, "orders", three_col_schema(), vec![])
            .unwrap();
        let mut info = catalog.get_table_info(&txn, "orders").unwrap();
        txn.commit().unwrap();

        Catalog::build_constraints_for_table(
            &mut info,
            vec![
                check_row("chk_a", "id > 0"),
                check_row("chk_b", "id < 1000"),
            ],
            vec![],
            vec![],
        )
        .unwrap();

        assert_eq!(info.check_constraints.len(), 2);
        let names: Vec<&str> = info
            .check_constraints
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert!(names.contains(&"chk_a"));
        assert!(names.contains(&"chk_b"));
    }

    #[test]
    fn build_constraints_check_with_null_expr_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let _file_id = catalog
            .create_table(&txn, "t", three_col_schema(), vec![])
            .unwrap();
        let mut info = catalog.get_table_info(&txn, "t").unwrap();
        txn.commit().unwrap();

        let bad_row = ConstraintRow {
            constraint_name: NonEmptyString::try_from("chk_missing").unwrap(),
            table_id: FileId::from(0u64),
            constraint_kind: ConstraintKind::Check,
            expr: None,
            backing_index_id: None,
        };

        let result = Catalog::build_constraints_for_table(&mut info, vec![bad_row], vec![], vec![]);

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow for missing expr, got: {result:?}"
        );
    }

    #[test]
    fn build_constraints_check_with_unparseable_expr_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_catalog_and_txn(dir.path());
        let txn = txn_mgr.begin().unwrap();
        let _file_id = catalog
            .create_table(&txn, "t", three_col_schema(), vec![])
            .unwrap();
        let mut info = catalog.get_table_info(&txn, "t").unwrap();
        txn.commit().unwrap();

        // "age >" is incomplete — parser cannot produce an Expr from it.
        let result = Catalog::build_constraints_for_table(
            &mut info,
            vec![check_row("chk_bad", "age >")],
            vec![],
            vec![],
        );

        assert!(
            matches!(result, Err(CatalogError::InvalidCatalogRow { .. })),
            "expected InvalidCatalogRow for malformed expr, got: {result:?}"
        );
    }
}
