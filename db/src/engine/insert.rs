//! Execution of `INSERT` statements.
//!
//! The entry point is [`Engine::exec_insert`], which resolves the target table,
//! translates each `VALUES` row from user-supplied [`Value`]s into a
//! physical [`Tuple`], and writes them to the heap.
//!
//! ## Logical vs physical columns
//!
//! A table's schema tracks two column counts:
//!
//! - **Logical** — columns visible to SQL (dropped columns excluded). This is what the user names
//!   in `INSERT INTO t (a, b)` and what a bare `VALUES` row must satisfy.
//! - **Physical** — every slot ever allocated on disk, including columns removed with `ALTER TABLE
//!   … DROP COLUMN`. Storage always writes a value into every physical slot, so dropped slots are
//!   backfilled with `NULL` or the column's stored default.
//!
//! [`build_projection`](Engine::build_projection) bridges the two worlds by
//! building a permutation array of length `physical_num_fields` where each
//! entry is either `Some(user_value_index)` or `None` (dropped slot).
//! [`bind_row`](Engine::bind_row) uses that permutation to produce the final
//! `Vec<Value>` that maps 1-to-1 with physical slots.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    FileId, IndexId, TransactionId, Value,
    catalog::{LiveIndex, manager::Catalog},
    engine::{ConstraintViolation, Engine, EngineError, StatementResult},
    parser::statements::{InsertSource, InsertStatement},
    primitives::{ColumnId, NonEmptyString},
    transaction::Transaction,
    tuple::{Tuple, TupleSchema},
};

impl Engine<'_> {
    /// Execute an `INSERT` statement and return the number of rows written.
    ///
    /// Resolves the target table from the catalog, builds a column projection,
    /// converts every `VALUES` row into a [`Tuple`] aligned with the physical
    /// schema, and hands the tuples off to [`Self::insert_rows_and_indexes`].
    ///
    /// Only `INSERT … VALUES (…)` is supported today. `INSERT … DEFAULT VALUES`
    /// and `INSERT … SELECT` return [`EngineError::Unsupported`].
    ///
    /// # Errors
    ///
    /// - [`EngineError::TableNotFound`] if `stmt.table_name` does not exist.
    /// - [`EngineError::UnknownColumn`] if any named column is not in the table or was dropped.
    /// - [`EngineError::DuplicateInsertColumn`] if a column appears more than once in the column
    ///   list.
    /// - [`EngineError::WrongColumnCount`] if the number of `VALUES` in a row does not match the
    ///   number of logical columns.
    /// - [`EngineError::TypeMismatch`] if a value cannot be coerced to the declared column type.
    /// - Constraint errors (FK, UNIQUE) propagated from [`Self::insert_rows_and_indexes`].
    pub(super) fn exec_insert(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        stmt: InsertStatement,
    ) -> Result<StatementResult, EngineError> {
        let info = catalog.get_table_info(txn, &stmt.table_name)?;
        let name = info.name.as_str().to_owned();
        let file_id = info.file_id;
        let schema = info.schema.clone();

        if let Some(ai_col_id) = info.auto_increment_column {
            Self::reject_auto_increment_in_insert(
                stmt.columns.as_deref(),
                &schema,
                &name,
                ai_col_id,
            )?;
        }

        let projection = Self::build_projection(stmt.columns.as_deref(), &schema, &name)?;

        let values = match stmt.source {
            InsertSource::Values(rows) => rows,
            InsertSource::DefaultValues => {
                return Err(EngineError::Unsupported(
                    "INSERT … DEFAULT VALUES is not yet supported".into(),
                ));
            }
            InsertSource::Select(_) => {
                return Err(EngineError::Unsupported(
                    "INSERT … SELECT is not yet supported".into(),
                ));
            }
        };

        let tuples = values
            .into_iter()
            .map(|row| {
                let fields = Self::bind_row(&row, &schema, &projection, &name)?;
                Ok(Tuple::new(fields))
            })
            .collect::<Result<Vec<_>, EngineError>>()?;

        let count = Self::insert_rows_and_indexes(catalog, txn, file_id, tuples)?;
        Ok(StatementResult::inserted(name, count))
    }

    /// Write `tuples` into the heap for `file_id` and keep all indexes in sync.
    ///
    /// ## Fast path
    ///
    /// When the table has no indexes and no foreign-key constraints, all tuples
    /// are written in one [`HeapFile::bulk_insert`] call. This avoids per-row
    /// overhead when constraints cannot be violated.
    ///
    /// ## Checked path
    ///
    /// When indexes or FK constraints are present, each tuple is processed
    /// individually:
    ///
    /// 1. Every parent-side FK reference is checked — the referenced value must already exist in
    ///    the parent table.
    /// 2. Every UNIQUE constraint backed by an index is probed; a non-empty result means the new
    ///    value is already present.
    /// 3. The tuple is appended to the heap, obtaining a [`RowId`].
    /// 4. Every index entry for that tuple is inserted using the new [`RowId`].
    ///
    /// The function stops at the first constraint violation and returns an error
    /// without rolling back rows already written — callers are responsible for
    /// transaction-level rollback.
    ///
    /// # Errors
    ///
    /// - [`EngineError::TableNotFound`] / storage errors if the heap or an index cannot be
    ///   accessed.
    /// - [`EngineError::ConstraintViolation`] wrapping [`ConstraintViolation::ForeignKeyViolation`]
    ///   or [`ConstraintViolation::UniqueViolation`] when a constraint is breached.
    pub(super) fn insert_rows_and_indexes(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
        tuples: Vec<Tuple>,
    ) -> Result<usize, EngineError> {
        let tid = txn.transaction_id();
        let heap = catalog.get_table_heap(file_id)?;
        let indexes = catalog.indexes_for(file_id);
        let fk_checks = Self::prepare_outbound_ref_checks(catalog, txn, file_id)?;

        // If there are no indexes or FK constraints, we can bulk insert the tuples.
        // This is the fast path.
        if indexes.is_empty() && fk_checks.is_empty() {
            return heap
                .bulk_insert(tid, tuples)
                .map(|rids| rids.len())
                .map_err(EngineError::from);
        }

        // If there are indexes or FK constraints, we need to check each tuple individually.
        // We collect the unique index checks first to avoid making multiple catalog round-trips.
        let unique_checks = Self::collect_unique_index_checks(&indexes, catalog, txn, file_id)?;

        let mut count = 0;
        for tuple in tuples {
            Self::check_fk_constraints(&fk_checks, &tuple, tid)?;
            Self::check_unique_constraints(&unique_checks, &tuple, tid)?;
            let rid = heap.insert_tuple(tid, &tuple)?;
            for index in &indexes {
                index.insert(tid, &tuple, rid)?;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Build a permutation that maps physical column slots to user-supplied value positions.
    ///
    /// Returns a `Vec<Option<usize>>` of length `physical_num_fields`. Each entry at
    /// index `phys_i` is:
    ///
    /// - `Some(user_col_idx)` — the value for this physical slot comes from
    ///   `VALUES_row[user_col_idx]`.
    /// - `None` — this physical slot is a dropped column; [`bind_row`] will fill it with the
    ///   column's stored default or `NULL`.
    ///
    /// ## Unnamed INSERT (`INSERT INTO t VALUES (…)`)
    ///
    /// When `cols` is `None`, logical columns are mapped positionally: the first
    /// non-dropped physical slot gets user index 0, the second gets 1, and so on.
    ///
    /// ## Named INSERT (`INSERT INTO t (a, c) VALUES (…)`)
    ///
    /// When `cols` is `Some`, each non-dropped physical slot is matched by name
    /// against the caller-supplied column list after
    /// [`validate_named_insert_columns`] confirms the list is complete and
    /// duplicate-free.
    ///
    /// # Errors
    ///
    /// - [`EngineError::UnknownColumn`] if any name in `cols` does not exist or refers to a dropped
    ///   column.
    /// - [`EngineError::DuplicateInsertColumn`] if a name appears more than once.
    /// - [`EngineError::WrongColumnCount`] if `cols` does not cover every logical column.
    ///
    /// # Panics
    ///
    /// Panics if a non-dropped physical column has no matching entry in `cols`
    /// after validation passes — this would indicate a bug in
    /// [`validate_named_insert_columns`].
    fn build_projection(
        cols: Option<&[NonEmptyString]>,
        schema: &TupleSchema,
        table: &str,
    ) -> Result<Vec<Option<usize>>, EngineError> {
        let logical_n = schema.logical_num_fields();
        let physical_n = schema.physical_num_fields();

        // If no column list is provided (i.e., positional/unnamed INSERT), build a mapping of
        // physical column indices to value positions by assigning user values to the first N
        // non-dropped logical columns in schema order.
        //
        // For every physical column slot, if it is not dropped, we assign the next available
        // user-supplied value index to that slot. Dropped columns are mapped to None.
        let Some(cols) = cols else {
            let mut perm = vec![None; physical_n];
            let mut logical_idx = 0usize;
            for (phys_i, field) in schema.physical_iter().enumerate() {
                if !field.is_dropped {
                    perm[phys_i] = Some(logical_idx);
                    logical_idx += 1;
                }
            }
            return Ok(perm);
        };

        Self::validate_named_insert_columns(cols, schema, table, logical_n)?;

        // If a column list is provided (i.e., named INSERT), build a mapping of physical column
        // indices to value positions by mapping each physical column to the user-supplied column
        // name in the provided column list.
        let mut perm = vec![None; physical_n];
        for (phys_i, field) in schema.physical_iter().enumerate() {
            if !field.is_dropped {
                let pos = cols
                    .iter()
                    .position(|c| c.as_str() == field.name.as_str())
                    .expect("all logical columns are covered by the column list");
                perm[phys_i] = Some(pos);
            }
        }
        Ok(perm)
    }

    /// Check that a named-column INSERT list is valid for the given schema.
    ///
    /// Enforces three rules in one pass over `cols`:
    ///
    /// 1. Every name must exist in `schema` and must not refer to a dropped column.
    /// 2. No name may appear more than once.
    /// 3. The list must cover all `logical_n` columns — partial column lists are not supported
    ///    (callers that need defaults should supply them explicitly).
    ///
    /// # Errors
    ///
    /// - [`EngineError::UnknownColumn`] if a name is absent from the schema or names a dropped
    ///   column.
    /// - [`EngineError::DuplicateInsertColumn`] if a name appears more than once in `cols`.
    /// - [`EngineError::WrongColumnCount`] if `cols.len()` differs from `logical_n` after
    ///   deduplication.
    fn validate_named_insert_columns(
        cols: &[NonEmptyString],
        schema: &TupleSchema,
        table: &str,
        logical_n: usize,
    ) -> Result<(), EngineError> {
        let table = table.to_string();
        let mut seen: HashSet<&str> = HashSet::with_capacity(cols.len());

        for c in cols {
            let column = c.as_str();
            if !seen.insert(column) {
                return Err(EngineError::DuplicateInsertColumn {
                    table,
                    column: column.to_string(),
                });
            }

            // Check that the column exists in the schema and is not dropped.
            let field = schema.field_by_name(c);
            if field.is_none() || field.unwrap().1.is_dropped {
                return Err(EngineError::UnknownColumn {
                    table,
                    column: column.to_string(),
                });
            }
        }

        if seen.len() != logical_n {
            return Err(EngineError::WrongColumnCount {
                table,
                expected: logical_n,
                got: seen.len(),
            });
        }
        Ok(())
    }

    /// Convert a single `VALUES` row into a physical-length `Vec<Value>`.
    ///
    /// `row` contains one [`Value`] per *logical* column — exactly what the user
    /// wrote in `VALUES (…)`. This function stretches it to cover every
    /// *physical* slot using `projection`:
    ///
    /// - `Some(user_idx)` → coerce `row[user_idx]` to the field's declared type via
    ///   [`bind_value_for`].
    /// - `None` → the physical slot is a dropped column; fill it with the field's
    ///   `missing_default_value` or [`Value::Null`].
    ///
    /// The returned `Vec` is ready to be wrapped in a [`Tuple`] and written to
    /// the heap.
    ///
    /// # Errors
    ///
    /// - [`EngineError::WrongColumnCount`] if `row.len()` does not equal
    ///   `schema.logical_num_fields()`.
    /// - [`EngineError::TypeMismatch`] (or similar) from [`bind_value_for`] if a value cannot be
    ///   coerced to the column's declared type.
    fn bind_row(
        row: &[Value],
        schema: &TupleSchema,
        projection: &[Option<usize>],
        table: &str,
    ) -> Result<Vec<Value>, EngineError> {
        let logical_n = schema.logical_num_fields();
        if row.len() != logical_n {
            return Err(EngineError::WrongColumnCount {
                table: table.into(),
                expected: logical_n,
                got: row.len(),
            });
        }

        // For each physical column, if it is not dropped, we bind the value from the user-supplied
        // value position using `bind_value_for`. If it is dropped, we fill it with the column's
        // stored default or `NULL`.
        schema
            .physical_iter()
            .zip(projection.iter())
            .map(|(field, proj)| {
                if let Some(user_idx) = proj {
                    let value = &row[*user_idx];
                    Self::bind_value_for(value, field, table)
                } else {
                    Ok(field.missing_default_value.clone().unwrap_or(Value::Null))
                }
            })
            .collect()
    }

    /// Returns `(constraint_name, index)` pairs for every UNIQUE constraint
    /// that is backed by one of `indexes`.
    ///
    /// Returns an empty vec immediately when `indexes` is empty, avoiding an
    /// unnecessary catalog round-trip.
    fn collect_unique_index_checks(
        indexes: &[Arc<LiveIndex>],
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
    ) -> Result<Vec<(String, Arc<LiveIndex>)>, EngineError> {
        if indexes.is_empty() {
            return Ok(Vec::new());
        }

        let table = catalog.get_table_info_by_id(txn, file_id)?;

        // Collect the unique constraint names and their backing index IDs.
        // For each unique constraint, if it has a backing index, we add the constraint name and
        // the index ID to the map.
        let backing_ids = table
            .unique_constraints
            .into_iter()
            .filter_map(|uc| uc.backing_index_id.map(|id| (id, uc.name.into_inner())))
            .collect::<HashMap<IndexId, String>>();

        // For each index, if it has a backing unique constraint, we add the constraint name and
        // the index ID to the map.
        Ok(indexes
            .iter()
            .filter_map(|live| {
                backing_ids
                    .get(&live.index_id)
                    .map(|constraint_name| (constraint_name.clone(), Arc::clone(live)))
            })
            .collect())
    }

    /// Verifies that every outbound FK constraint is satisfied by `tuple`.
    ///
    /// Walks each [`ParentFkCheck`](super::fk::ParentFkCheck) prepared for this
    /// table and calls [`Self::referenced_row_exists`] to confirm the parent row
    /// is present.  The check uses an index point-lookup when a covering index
    /// exists on the parent, falling back to a heap scan otherwise.
    ///
    /// A NULL in any FK column short-circuits that constraint to `true` — SQL
    /// semantics treat a NULL foreign key as "no reference asserted", so it is
    /// always allowed regardless of what the parent table contains.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Constraint`] →
    /// [`ConstraintViolation::ForeignKeyViolation`] (with the constraint name)
    /// for the first FK whose referenced parent row cannot be found.
    fn check_fk_constraints(
        fk_checks: &[super::fk::ParentFkCheck],
        tuple: &Tuple,
        tid: TransactionId,
    ) -> Result<(), EngineError> {
        for fk in fk_checks {
            if !Self::referenced_row_exists(fk, tuple, tid)? {
                return Err(ConstraintViolation::ForeignKeyViolation {
                    constraint: fk.name.clone(),
                }
                .into());
            }
        }
        Ok(())
    }

    /// Verifies that `tuple` does not violate any UNIQUE constraint.
    ///
    /// For each `(constraint_name, index)` pair in `unique_checks`, projects
    /// `tuple` into the index's key format and probes the index.  A non-empty
    /// result means a row with that key already exists — inserting `tuple`
    /// would create a duplicate, so the insert is rejected immediately.
    ///
    /// `unique_checks` is pre-filtered to only the indexes that actually back a
    /// UNIQUE constraint, so non-unique indexes are never probed here.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::Constraint`] →
    /// [`ConstraintViolation::UniqueViolation`] (with the constraint name) for
    /// the first constraint whose key is already present in the index.
    fn check_unique_constraints(
        unique_checks: &[(String, Arc<LiveIndex>)],
        tuple: &Tuple,
        tid: TransactionId,
    ) -> Result<(), EngineError> {
        for (constraint, live) in unique_checks {
            let key = live.create_index_key(tuple)?;
            let hits = live.access.search(tid, &key)?;
            if hits.is_empty() {
                continue;
            }
            return Err(ConstraintViolation::UniqueViolation {
                constraint: constraint.clone(),
            }
            .into());
        }
        Ok(())
    }

    /// Rejects an INSERT that explicitly names an `AUTO_INCREMENT` column.
    ///
    /// For a **named INSERT** (`INSERT INTO t (a, b) VALUES …`), the column list
    /// is checked for the auto-increment column by name. For a **positional
    /// INSERT** (`INSERT INTO t VALUES …`) with no column list, the user has no
    /// way to omit the auto-increment slot, so positional inserts are rejected
    /// entirely when the table has an auto-increment column.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::InsertIntoAutoIncrementColumn`] when the column
    /// would be set by the INSERT.
    fn reject_auto_increment_in_insert(
        cols: Option<&[NonEmptyString]>,
        schema: &TupleSchema,
        table: &str,
        ai_col_id: ColumnId,
    ) -> Result<(), EngineError> {
        let ai_name = schema
            .field(usize::from(ai_col_id))
            .map(|f| f.name.as_str().to_owned())
            .unwrap_or_default();

        match cols {
            // Named INSERT: error only if the auto-increment column is listed.
            Some(named) => {
                if named.iter().any(|c| c.as_str() == ai_name) {
                    return Err(EngineError::InsertIntoAutoIncrementColumn {
                        table: table.to_owned(),
                        column: ai_name,
                    });
                }
            }
            // Positional INSERT: all logical columns must be supplied, so the
            // auto-increment column cannot be omitted — reject unconditionally.
            None => {
                return Err(EngineError::InsertIntoAutoIncrementColumn {
                    table: table.to_owned(),
                    column: ai_name,
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        Type, Value,
        buffer_pool::page_store::PageStore,
        catalog::manager::Catalog,
        engine::{ConstraintViolation, Engine, EngineError, StatementResult},
        index::{CompositeKey, IndexKind},
        parser::Parser,
        primitives::{ColumnId, NonEmptyString},
        transaction::TransactionManager,
        tuple::{Field, Tuple, TupleSchema},
        wal::writer::Wal,
    };

    fn make_infra(dir: &Path) -> (Catalog, TransactionManager) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).unwrap());
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).unwrap();
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    fn col_id(i: usize) -> ColumnId {
        ColumnId::try_from(i).unwrap()
    }

    fn run(engine: &Engine<'_>, sql: &str) {
        let stmt = Parser::new(sql).parse().expect("parse");
        engine.execute_statement(stmt).expect("execute");
    }

    fn try_run(engine: &Engine<'_>, sql: &str) -> Result<StatementResult, EngineError> {
        let stmt = Parser::new(sql).parse().expect("parse");
        engine.execute_statement(stmt)
    }

    fn scan_rows(catalog: &Catalog, txn_mgr: &TransactionManager, table: &str) -> Vec<Tuple> {
        let txn = txn_mgr.begin().unwrap();
        let info = catalog.get_table_info(&txn, table).unwrap();
        let heap = catalog.get_table_heap(info.file_id).unwrap();
        let mut rows = Vec::new();
        for (_, tuple) in heap.scan(txn.transaction_id()).unwrap() {
            rows.push(tuple);
        }
        txn.commit().unwrap();
        rows
    }

    fn field(name: &str, col_type: Type) -> Field {
        Field::new_non_empty(NonEmptyString::new(name).unwrap(), col_type)
    }

    #[test]
    fn insert_updates_single_column_hash_index() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("email", Type::String).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "users_email_idx",
                "users",
                table_file_id,
                &[col_id(1)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(
            &engine,
            "INSERT INTO users (id, email) VALUES (1, 'a@b.com'), (2, 'c@d.com');",
        );

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        let key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "exactly one rid for unique email");
        assert_eq!(hits[0].file_id, table_file_id);
    }

    #[test]
    fn insert_updates_composite_hash_index_in_declaration_order() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![
                    field("a", Type::Int64).not_null(),
                    field("b", Type::Int64).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "t_ba_idx",
                "t",
                table_file_id,
                &[col_id(1), col_id(0)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO t (a, b) VALUES (10, 20);");

        let live = catalog.get_index_by_name("t_ba_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();

        let key_ok = CompositeKey::new(vec![Value::Int64(20), Value::Int64(10)]);
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key_ok)
            .unwrap();
        assert_eq!(hits.len(), 1);

        let key_swapped = CompositeKey::new(vec![Value::Int64(10), Value::Int64(20)]);
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &key_swapped)
            .unwrap();
        probe_txn.commit().unwrap();
        assert!(miss.is_empty(), "swapped key must not match");
    }

    #[test]
    fn insert_into_table_without_indexes_still_works() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "noidx",
                TupleSchema::new(vec![field("x", Type::Int64).not_null()]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO noidx (x) VALUES (1), (2), (3);");
    }

    #[test]
    fn positional_insert_maps_values_to_correct_slots() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![
                    field("x", Type::Int64).not_null(),
                    field("y", Type::Int64).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        // Index on y (slot 1) lets us verify which slot received which value.
        catalog
            .create_index(&txn, "t_y_idx", "t", file_id, &[col_id(1)], IndexKind::Hash)
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO t VALUES (10, 20);");

        let live = catalog.get_index_by_name("t_y_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        let tid = probe_txn.transaction_id();

        // 20 is in y — must hit.
        let hits = live
            .access
            .search(tid, &CompositeKey::single(Value::Int64(20)))
            .unwrap();
        assert_eq!(hits.len(), 1, "value 20 must land in the y slot");

        // 10 is in x, not indexed — must miss.
        let miss = live
            .access
            .search(tid, &CompositeKey::single(Value::Int64(10)))
            .unwrap();
        assert!(miss.is_empty(), "value 10 is in x, not y");

        probe_txn.commit().unwrap();
    }

    #[test]
    fn named_insert_unknown_column_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("name", Type::String).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = try_run(&engine, "INSERT INTO t (id, ghost) VALUES (1, 'x')").unwrap_err();

        assert!(
            matches!(err, EngineError::UnknownColumn { ref column, .. } if column == "ghost"),
            "expected UnknownColumn for 'ghost', got {err:?}"
        );
    }

    #[test]
    fn named_insert_duplicate_column_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("name", Type::String).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        let err = try_run(&engine, "INSERT INTO t (id, id) VALUES (1, 2)").unwrap_err();

        assert!(
            matches!(err, EngineError::DuplicateInsertColumn { ref column, .. } if column == "id"),
            "expected DuplicateInsertColumn for 'id', got {err:?}"
        );
    }

    #[test]
    fn named_insert_column_list_too_short_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("name", Type::String).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        // Column list names only 1 of the 2 logical columns.
        let engine = Engine::new(&catalog, &txn_mgr);
        let err = try_run(&engine, "INSERT INTO t (id) VALUES (1)").unwrap_err();

        assert!(
            matches!(err, EngineError::WrongColumnCount {
                expected: 2,
                got: 1,
                ..
            }),
            "expected WrongColumnCount {{expected:2, got:1}}, got {err:?}"
        );
    }

    #[test]
    fn values_row_shorter_than_column_list_returns_error() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("name", Type::String).not_null(),
                ]),
                vec![],
            )
            .unwrap();
        txn.commit().unwrap();

        // Column list covers both columns; VALUES row supplies only one value.
        let engine = Engine::new(&catalog, &txn_mgr);
        let err = try_run(&engine, "INSERT INTO t (id, name) VALUES (1)").unwrap_err();

        assert!(
            matches!(err, EngineError::WrongColumnCount {
                expected: 2,
                got: 1,
                ..
            }),
            "expected WrongColumnCount {{expected:2, got:1}}, got {err:?}"
        );
    }

    #[test]
    fn dropped_column_physical_slot_is_backfilled_with_null() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        // Three-column table: id | tag | score (physical slots 0, 1, 2).
        let engine = Engine::new(&catalog, &txn_mgr);
        run(
            &engine,
            "CREATE TABLE t (id INT NOT NULL, tag STRING NOT NULL, score INT NOT NULL)",
        );
        // Drop the middle column — logical columns are now [id, score], physical still [id, tag,
        // score].
        run(&engine, "ALTER TABLE t DROP COLUMN tag");
        // Insert only the two surviving logical columns.
        run(&engine, "INSERT INTO t (id, score) VALUES (42, 99)");

        let rows = scan_rows(&catalog, &txn_mgr, "t");
        assert_eq!(rows.len(), 1);

        let tuple = &rows[0];
        // Physical slot 0: id = 42
        assert_eq!(
            tuple.get(0),
            Some(&Value::Int64(42)),
            "slot 0 must be id=42"
        );
        // Physical slot 1: dropped tag column — must be NULL
        assert_eq!(
            tuple.get(1),
            Some(&Value::Null),
            "slot 1 (dropped tag) must be NULL"
        );
        // Physical slot 2: score = 99
        assert_eq!(
            tuple.get(2),
            Some(&Value::Int64(99)),
            "slot 2 must be score=99"
        );
    }

    // ── FK enforcement ────────────────────────────────────────────────────────

    #[test]
    fn fk_violation_on_insert_is_rejected() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "CREATE TABLE parent (id INT NOT NULL UNIQUE)");
        run(
            &engine,
            "CREATE TABLE child (id INT NOT NULL, parent_id INT NOT NULL, \
             FOREIGN KEY (parent_id) REFERENCES parent(id))",
        );
        run(&engine, "INSERT INTO parent VALUES (1)");

        // 999 does not exist in parent.id.
        let err = try_run(&engine, "INSERT INTO child VALUES (1, 999)").unwrap_err();

        assert!(
            matches!(
                err,
                EngineError::Constraint(ConstraintViolation::ForeignKeyViolation { .. })
            ),
            "expected ForeignKeyViolation, got {err:?}"
        );
    }

    #[test]
    fn fk_insert_with_valid_parent_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "CREATE TABLE parent (id INT NOT NULL UNIQUE)");
        run(
            &engine,
            "CREATE TABLE child (id INT NOT NULL, parent_id INT NOT NULL, \
             FOREIGN KEY (parent_id) REFERENCES parent(id))",
        );
        run(&engine, "INSERT INTO parent VALUES (1)");
        // parent_id=1 exists in parent.
        run(&engine, "INSERT INTO child VALUES (1, 1)");

        assert_eq!(scan_rows(&catalog, &txn_mgr, "child").len(), 1);
    }
}
