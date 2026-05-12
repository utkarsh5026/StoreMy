//! Row-changing SQL (`INSERT`, `DELETE`, `UPDATE`).
//!
//! This module is the SQL → storage bridge for DML statements. The binder produces a
//! fully-resolved `Bound*` shape (column indices, coerced values, optional `WHERE` predicate),
//! and these executor entry points apply the requested changes inside a short transaction
//! callback, maintaining any registered secondary indexes.
//!
//! ## Shape
//!
//! - [`Engine::exec_insert`] — `INSERT INTO … VALUES …`
//! - [`Engine::exec_delete`] — `DELETE FROM … [WHERE …]`
//! - [`Engine::exec_update`] — `UPDATE … SET … [WHERE …]`
//!
//! ## How it works
//!
//! - **INSERT** evaluates each row's value expressions into a [`Tuple`], bulk-inserts into the
//!   table heap, then inserts `(key, rid)` pairs into every registered index for the table.
//! - **DELETE** scans for qualifying rows (optional `WHERE` predicate), deletes the heap tuples,
//!   then deletes the corresponding keys from every index.
//! - **UPDATE** scans for qualifying rows, applies the assignment list to each tuple, writes the
//!   updated tuple back to the heap, then maintains indexes whose covered columns were touched.
//!
//! ## NULL semantics
//!
//! Predicates in `WHERE` are evaluated by
//! [`BooleanExpression`](crate::execution::expression::BooleanExpression): any `NULL` in a
//! comparison short-circuits to `false`, so rows with `NULL` keys do not qualify.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use fallible_iterator::FallibleIterator;

use crate::{
    FileId, IndexId, TransactionId, Value,
    binder::{Bound, BoundDelete, BoundExpr, BoundInsert, BoundUpdate},
    catalog::{InboundFk, LiveIndex, manager::Catalog, systable::FkAction},
    engine::{Engine, EngineError, StatementResult},
    execution::expression::BooleanExpression,
    heap::file::HeapFile,
    index::CompositeKey,
    parser::statements::{Statement, UpdateStatement},
    primitives::{ColumnId, Predicate, RecordId},
    transaction::Transaction,
    tuple::{Tuple, TupleSchema},
};

/// Precomputed data for one outgoing FK constraint, built once per INSERT statement.
struct FkCheck {
    name: String,
    ref_heap: Arc<HeapFile>,
    /// Fast path: parent index whose columns cover `ref_columns`.
    /// Stores (index, `local_col_ids_in_index_key_order`) so we can build
    /// the `CompositeKey` directly from the child tuple without re-mapping.
    index_lookup: Option<(Arc<LiveIndex>, Vec<ColumnId>)>,
    /// Fallback pairs used in the heap scan: (`local_col`, `ref_col`).
    local_ref_pairs: Vec<(ColumnId, ColumnId)>,
}

/// Precomputed data for one inbound FK, built once per DELETE/UPDATE statement.
///
/// "Inbound" means this table is the parent — other tables' rows depend on it.
struct InboundFkCheck {
    constraint_name: String,
    child_table_id: FileId,
    child_heap: Arc<HeapFile>,
    /// `(child_col, ref_col)` pairs in ordinal order.
    col_pairs: Vec<(ColumnId, ColumnId)>,
    on_delete: crate::catalog::systable::FkAction,
    on_update: crate::catalog::systable::FkAction,
    /// Fast lookup of child rows: (child index, `ref_col_ids_in_index_key_order`).
    /// `ref_col_ids_in_index_key_order[i]` is the parent column whose value
    /// feeds index position `i` when building the child search key.
    child_index: Option<(Arc<LiveIndex>, Vec<ColumnId>)>,
    /// All indexes on the child table, for maintenance during CASCADE/SET NULL.
    child_indexes: Vec<Arc<LiveIndex>>,
}

impl Engine<'_> {
    /// Executes `INSERT INTO <table> …` by inserting tuples into the heap and updating indexes.
    ///
    /// The binder has already resolved the target table and coerced values to the table schema.
    /// This executor step evaluates each row's expressions into a [`Tuple`], bulk-inserts those
    /// tuples into the backing heap, and (if any indexes exist) inserts one `(key, rid)` entry per
    /// index per inserted row.
    ///
    /// # SQL examples
    ///
    /// Assume a fixed schema throughout:
    ///
    /// ```sql
    /// -- users(id, email) where the binder resolves: id → 0, email → 1
    /// ```
    ///
    /// ```sql
    /// -- 1. Single-row insert
    /// --
    /// --   INSERT INTO users VALUES (1, 'a@x');
    /// --
    /// --   BoundInsert {
    /// --       name: "users",
    /// --       file_id: <resolved>,
    /// --       rows: [
    /// --           [Expr::Literal(1), Expr::Literal("a@x")],
    /// --       ],
    /// --       ..
    /// --   }
    /// ```
    ///
    /// ```sql
    /// -- 2. Multi-row insert
    /// --
    /// --   INSERT INTO users VALUES (1, 'a@x'), (2, 'b@x');
    /// --
    /// --   BoundInsert { rows: [ ..two rows.. ], .. }
    /// ```
    ///
    /// # Errors
    ///
    /// Propagates heap insertion and index maintenance failures (I/O errors, type errors while
    /// forming index keys, or access-method insert failures).
    pub(super) fn exec_insert(&self, statement: Statement) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(statement, |catalog, bound, txn| {
            let Bound::Insert(BoundInsert {
                name,
                file_id,
                rows,
                ..
            }) = bound
            else {
                unreachable!("binder returned non-Insert variant for Insert input");
            };

            let tuples = rows
                .iter()
                .map(|row| Tuple::new(row.iter().map(BoundExpr::eval).collect()))
                .collect::<Vec<_>>();
            let rids = Self::insert_rows_and_indexes(catalog, txn, file_id, tuples)?;
            Ok(StatementResult::inserted(name, rids))
        })
    }

    /// Inserts `tuples` into the heap and updates every index registered for `file_id`.
    ///
    /// Fast path: when no indexes and no FK constraints exist, uses
    /// [`HeapFile::bulk_insert`] to batch writes.
    ///
    /// Checked path: inserts one tuple at a time, running FK and UNIQUE checks
    /// before each heap write, then updating indexes.
    fn insert_rows_and_indexes(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
        tuples: Vec<Tuple>,
    ) -> Result<usize, EngineError> {
        let tid = txn.transaction_id();
        let heap = catalog.get_table_heap(file_id)?;
        let indexes = catalog.indexes_for(file_id);
        let fk_checks = Self::build_fk_checks(catalog, txn, file_id)?;

        if indexes.is_empty() && fk_checks.is_empty() {
            return heap
                .bulk_insert(tid, tuples)
                .map(|rids| rids.len())
                .map_err(EngineError::from);
        }

        // Build a list of (constraint_name, live_index) for every UNIQUE constraint
        // that has a backing index, so we can check for violations before each insert.
        let unique_checks: Vec<(String, Arc<LiveIndex>)> = if indexes.is_empty() {
            Vec::new()
        } else {
            let info = catalog.get_table_info_by_id(txn, file_id)?;
            let backing_ids: HashMap<IndexId, String> = info
                .unique_constraints
                .into_iter()
                .filter_map(|uc| {
                    uc.backing_index_id
                        .map(|id| (id, uc.name.as_str().to_owned()))
                })
                .collect();
            indexes
                .iter()
                .filter_map(|live| {
                    backing_ids
                        .get(&live.index_id)
                        .map(|name| (name.clone(), Arc::clone(live)))
                })
                .collect()
        };

        let mut count = 0;
        tuples
            .into_iter()
            .try_for_each(|tuple| -> Result<(), EngineError> {
                for fk in &fk_checks {
                    if !Self::fk_ref_exists(fk, &tuple, tid)? {
                        return Err(EngineError::ForeignKeyViolation {
                            constraint: fk.name.clone(),
                        });
                    }
                }
                for (constraint, live) in &unique_checks {
                    let key = live.create_index_key(&tuple)?;
                    if !live.access.search(tid, &key)?.is_empty() {
                        return Err(EngineError::UniqueViolation {
                            constraint: constraint.clone(),
                        });
                    }
                }
                let rid = heap.insert_tuple(tid, &tuple)?;
                for index in &indexes {
                    index.insert(tid, &tuple, rid)?;
                }
                count += 1;
                Ok(())
            })?;
        Ok(count)
    }

    /// Builds [`FkCheck`] descriptors for every outgoing FK on `file_id`.
    ///
    /// For each FK we try to find a matching index on the parent table whose
    /// `table_columns` (as a set) equals `ref_columns`. When found we precompute
    /// the local column id list in the index's key order so the per-row check is
    /// just a projection + point lookup. When no matching index exists we fall
    /// back to a full heap scan of the parent.
    fn build_fk_checks(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
    ) -> Result<Vec<FkCheck>, EngineError> {
        let info = catalog.get_table_info_by_id(txn, file_id)?;
        if info.foreign_keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut checks = Vec::new();
        for fk in &info.foreign_keys {
            let ref_heap = catalog.get_table_heap(fk.ref_table_id)?;
            let ref_indexes = catalog.indexes_for(fk.ref_table_id);

            let ref_col_set: HashSet<ColumnId> = fk.ref_columns.iter().copied().collect();
            let matching_index = ref_indexes.into_iter().find(|live| {
                let idx_cols: HashSet<ColumnId> = live.table_columns.iter().copied().collect();
                idx_cols == ref_col_set
            });

            let index_lookup = matching_index.map(|live| {
                // Map ref_col → local_col so we can project in the index's key order.
                let ref_to_local: HashMap<ColumnId, ColumnId> = fk
                    .ref_columns
                    .iter()
                    .zip(fk.local_columns.iter())
                    .map(|(&r, &l)| (r, l))
                    .collect();
                let local_col_order: Vec<ColumnId> = live
                    .table_columns
                    .iter()
                    .map(|ref_col| ref_to_local[ref_col])
                    .collect();
                (live, local_col_order)
            });

            let local_ref_pairs: Vec<(ColumnId, ColumnId)> = fk
                .local_columns
                .iter()
                .zip(fk.ref_columns.iter())
                .map(|(&l, &r)| (l, r))
                .collect();

            checks.push(FkCheck {
                name: fk.name.as_str().to_owned(),
                ref_heap,
                index_lookup,
                local_ref_pairs,
            });
        }
        Ok(checks)
    }

    /// Returns `true` if the parent table contains a row satisfying the FK for `tuple`.
    ///
    /// NULL in any FK column short-circuits to `true` — SQL treats a NULL
    /// reference as "unknown", which is always allowed.
    ///
    /// Fast path: index point lookup on the parent.
    /// Fallback: full heap scan of the parent, comparing column values pairwise.
    fn fk_ref_exists(fk: &FkCheck, tuple: &Tuple, tid: TransactionId) -> Result<bool, EngineError> {
        let has_null = fk.local_ref_pairs.iter().any(|&(local_col, _)| {
            tuple
                .get(usize::from(local_col))
                .is_none_or(|v| *v == Value::Null)
        });
        if has_null {
            return Ok(true);
        }

        if let Some((index, local_cols)) = &fk.index_lookup {
            let values: Result<Vec<Value>, EngineError> = local_cols
                .iter()
                .map(|&col| {
                    tuple.get(usize::from(col)).cloned().ok_or_else(|| {
                        EngineError::type_error(format!(
                            "FK column {} out of bounds in tuple",
                            usize::from(col)
                        ))
                    })
                })
                .collect();
            let key = CompositeKey::new(values?);
            return Ok(!index.access.search(tid, &key)?.is_empty());
        }

        // Heap scan fallback: walk every parent row looking for a match.
        let mut scan = fk.ref_heap.scan(tid)?;
        while let Some((_, parent_row)) = FallibleIterator::next(&mut scan)? {
            let all_match = fk.local_ref_pairs.iter().all(|&(local_col, ref_col)| {
                tuple.get(usize::from(local_col)) == parent_row.get(usize::from(ref_col))
            });
            if all_match {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Executes `DELETE FROM <table> [WHERE <predicate>]` and updates indexes.
    ///
    /// The binder turns the optional `WHERE` into a
    /// [`BooleanExpression`](crate::execution::expression::BooleanExpression) tree evaluated over
    /// each table tuple. This method collects all matching `(rid, tuple)` pairs, deletes the heap
    /// tuples, then deletes the corresponding keys from every registered index.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- 1. Unconditional delete (delete all rows)
    /// --
    /// --   DELETE FROM users;
    /// --
    /// --   BoundDelete { name: "users", file_id: <resolved>, filter: None }
    /// ```
    ///
    /// ```sql
    /// -- 2. Conditional delete
    /// --
    /// --   DELETE FROM users WHERE id = 1;
    /// --
    /// --   BoundDelete {
    /// --       name: "users",
    /// --       file_id: <resolved>,
    /// --       filter: Some(BooleanExpression::Leaf { Column(0), Equals, Literal(1) }),
    /// --   }
    /// ```
    ///
    /// # NULL handling
    ///
    /// If the predicate evaluates to `false` due to `NULL` short-circuiting, the row is not
    /// deleted (matching `WHERE` behavior).
    pub(super) fn exec_delete(&self, statement: Statement) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(statement, |catalog, bound, txn| {
            let Bound::Delete(BoundDelete {
                name,
                file_id,
                filter,
            }) = bound
            else {
                unreachable!("binder returned non-Delete variant for Delete input");
            };

            let predicate = filter.as_ref();
            let deleted = Self::delete_rows_and_indexes(catalog, txn, file_id, predicate)?;
            Ok(StatementResult::deleted(name, deleted))
        })
    }

    /// Deletes every heap row matching `predicate`, enforces inbound FK constraints,
    /// and removes corresponding index entries.
    ///
    /// For each matched row:
    /// 1. Applies inbound FK actions (`RESTRICT` → error; `CASCADE` → recurse into child; `SET
    ///    NULL` → null out child FK columns).
    /// 2. Deletes the heap tuple.
    /// 3. Removes the row's key from every registered index.
    fn delete_rows_and_indexes(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
        predicate: Option<&BooleanExpression>,
    ) -> Result<usize, EngineError> {
        let tid = txn.transaction_id();
        let heap = catalog.get_table_heap(file_id)?;
        let rows = Self::collect_matching(&heap, tid, predicate)?;
        let inbound_checks = Self::build_inbound_fk_checks(catalog, txn, file_id)?;
        let indexes = catalog.indexes_for(file_id);

        let mut deleted = 0;
        rows.into_iter()
            .try_for_each(|(rid, tuple)| -> Result<(), EngineError> {
                Self::apply_inbound_fk_on_delete(catalog, txn, &inbound_checks, &tuple, tid)?;
                heap.delete_tuple(tid, rid)?;
                for index in &indexes {
                    index.delete(tid, &tuple, rid)?;
                }
                deleted += 1;
                Ok(())
            })?;
        Ok(deleted)
    }

    /// Builds [`InboundFkCheck`] descriptors for every FK that points at `file_id` as parent.
    fn build_inbound_fk_checks(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
    ) -> Result<Vec<InboundFkCheck>, EngineError> {
        let inbound = catalog.find_inbound_fks(txn, file_id)?;
        if inbound.is_empty() {
            return Ok(Vec::new());
        }

        let mut checks = Vec::new();
        for InboundFk {
            constraint_name,
            child_table_id,
            child_columns,
            ref_columns,
            on_delete,
            on_update,
        } in inbound
        {
            let child_heap = catalog.get_table_heap(child_table_id)?;
            let child_indexes = catalog.indexes_for(child_table_id);

            // Try to find an index on the child that exactly covers the FK columns.
            let child_col_set: HashSet<ColumnId> = child_columns.iter().copied().collect();
            let child_to_ref: HashMap<ColumnId, ColumnId> = child_columns
                .iter()
                .zip(ref_columns.iter())
                .map(|(&c, &r)| (c, r))
                .collect();

            let matching = child_indexes.iter().find(|live| {
                let idx_cols: HashSet<ColumnId> = live.table_columns.iter().copied().collect();
                idx_cols == child_col_set
            });
            let child_index = matching.map(|live| {
                // For each index position (a child_col), record which ref_col
                // of the parent row supplies the search value.
                let ref_col_order: Vec<ColumnId> = live
                    .table_columns
                    .iter()
                    .map(|child_col| child_to_ref[child_col])
                    .collect();
                (Arc::clone(live), ref_col_order)
            });

            let col_pairs: Vec<(ColumnId, ColumnId)> = child_columns
                .iter()
                .zip(ref_columns.iter())
                .map(|(&c, &r)| (c, r))
                .collect();

            checks.push(InboundFkCheck {
                constraint_name,
                child_table_id,
                child_heap,
                col_pairs,
                on_delete: on_delete.unwrap_or(FkAction::NoAction),
                on_update: on_update.unwrap_or(FkAction::NoAction),
                child_index,
                child_indexes,
            });
        }
        Ok(checks)
    }

    /// Finds child rows that reference `parent_row` via the given inbound FK.
    ///
    /// Fast path: index point lookup on the child using the parent's ref-column values.
    /// Fallback: full heap scan of the child comparing FK column values pairwise.
    fn find_child_rows_for_fk(
        fk: &InboundFkCheck,
        parent_row: &Tuple,
        tid: TransactionId,
    ) -> Result<Vec<(RecordId, Tuple)>, EngineError> {
        if let Some((index, ref_col_order)) = &fk.child_index {
            let values: Result<Vec<Value>, EngineError> = ref_col_order
                .iter()
                .map(|&ref_col| {
                    parent_row
                        .get(usize::from(ref_col))
                        .cloned()
                        .ok_or_else(|| {
                            EngineError::type_error(format!(
                                "FK ref column {} out of bounds in parent row",
                                usize::from(ref_col)
                            ))
                        })
                })
                .collect();
            let key = CompositeKey::new(values?);
            let rids = index.access.search(tid, &key)?;
            let mut rows = Vec::new();
            for rid in rids {
                if let Some(tuple) = fk.child_heap.fetch_tuple(tid, rid)? {
                    rows.push((rid, tuple));
                }
            }
            return Ok(rows);
        }

        // Heap scan: build an equality predicate over the child FK columns.
        let predicate = Self::build_fk_equality_predicate(&fk.col_pairs, parent_row);
        Self::collect_matching(&fk.child_heap, tid, predicate.as_ref())
    }

    /// Builds a `col = val AND …` predicate over child FK columns from a parent row.
    ///
    /// Each `(child_col, ref_col)` pair in `col_pairs` contributes one leaf:
    /// `child_col = parent_row[ref_col]`. The leaves are chained with `And`.
    fn build_fk_equality_predicate(
        col_pairs: &[(ColumnId, ColumnId)],
        parent_row: &Tuple,
    ) -> Option<BooleanExpression> {
        let mut predicate: Option<BooleanExpression> = None;
        for &(child_col, ref_col) in col_pairs {
            let value = parent_row
                .get(usize::from(ref_col))
                .cloned()
                .unwrap_or(Value::Null);
            let leaf =
                BooleanExpression::col_op_lit(usize::from(child_col), Predicate::Equals, value);
            predicate = Some(match predicate {
                None => leaf,
                Some(p) => BooleanExpression::And(Box::new(p), Box::new(leaf)),
            });
        }
        predicate
    }

    /// Enforces inbound FK constraints when a parent row is being deleted.
    ///
    /// For each inbound FK:
    /// - `RESTRICT` / `NO ACTION` → error if any child rows exist.
    /// - `CASCADE` → recursively delete the matching child rows.
    /// - `SET NULL` → set the child FK columns to `NULL` and sync indexes.
    fn apply_inbound_fk_on_delete(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        inbound_checks: &[InboundFkCheck],
        parent_row: &Tuple,
        tid: TransactionId,
    ) -> Result<(), EngineError> {
        use crate::catalog::systable::FkAction;
        for fk in inbound_checks {
            let child_rows = Self::find_child_rows_for_fk(fk, parent_row, tid)?;
            if child_rows.is_empty() {
                continue;
            }
            match fk.on_delete {
                FkAction::NoAction | FkAction::Restrict | FkAction::SetDefault => {
                    return Err(EngineError::FkParentViolation {
                        constraint: fk.constraint_name.clone(),
                    });
                }
                FkAction::Cascade => {
                    let predicate = Self::build_fk_equality_predicate(&fk.col_pairs, parent_row);
                    Self::delete_rows_and_indexes(
                        catalog,
                        txn,
                        fk.child_table_id,
                        predicate.as_ref(),
                    )?;
                }
                FkAction::SetNull => {
                    Self::set_null_child_fk_cols(fk, child_rows, tid)?;
                }
            }
        }
        Ok(())
    }

    /// Enforces inbound FK constraints when a parent row's referenced columns change.
    ///
    /// For each inbound FK whose referenced columns appear in the update:
    /// - `RESTRICT` / `NO ACTION` → error if any child rows still reference the old key.
    /// - `CASCADE` → update child FK columns to the new parent key values.
    /// - `SET NULL` → set the child FK columns to `NULL` and sync indexes.
    fn apply_inbound_fk_on_update(
        _catalog: &Catalog,
        _txn: &Transaction<'_>,
        inbound_checks: &[InboundFkCheck],
        old_row: &Tuple,
        new_row: &Tuple,
        tid: TransactionId,
    ) -> Result<(), EngineError> {
        use crate::catalog::systable::FkAction;
        for fk in inbound_checks {
            // Skip if none of the ref columns actually changed for this row.
            let ref_changed = fk.col_pairs.iter().any(|&(_, ref_col)| {
                old_row.get(usize::from(ref_col)) != new_row.get(usize::from(ref_col))
            });
            if !ref_changed {
                continue;
            }

            let child_rows = Self::find_child_rows_for_fk(fk, old_row, tid)?;
            if child_rows.is_empty() {
                continue;
            }
            match fk.on_update {
                FkAction::NoAction | FkAction::Restrict | FkAction::SetDefault => {
                    return Err(EngineError::FkParentViolation {
                        constraint: fk.constraint_name.clone(),
                    });
                }
                FkAction::Cascade => {
                    for (rid, mut child_tuple) in child_rows {
                        let old = child_tuple.clone();
                        for &(child_col, ref_col) in &fk.col_pairs {
                            if let Some(v) = child_tuple.get_mut(usize::from(child_col)) {
                                *v = new_row
                                    .get(usize::from(ref_col))
                                    .cloned()
                                    .unwrap_or(Value::Null);
                            }
                        }
                        fk.child_heap.update_tuple(tid, rid, &child_tuple)?;
                        Self::sync_fk_child_indexes(
                            &fk.child_indexes,
                            tid,
                            rid,
                            &old,
                            &child_tuple,
                        )?;
                    }
                    // After cascade-updating child FK cols, check that the new
                    // child key satisfies any outbound FK on the child itself.
                    // (Deferred: skip for now; enforce at child INSERT/UPDATE time.)
                }
                FkAction::SetNull => {
                    Self::set_null_child_fk_cols(fk, child_rows, tid)?;
                }
            }
        }
        Ok(())
    }

    /// Sets the FK columns of every `child_rows` tuple to NULL and syncs indexes.
    fn set_null_child_fk_cols(
        fk: &InboundFkCheck,
        child_rows: Vec<(RecordId, Tuple)>,
        tid: TransactionId,
    ) -> Result<(), EngineError> {
        for (rid, mut child_tuple) in child_rows {
            let old = child_tuple.clone();
            for &(child_col, _) in &fk.col_pairs {
                if let Some(v) = child_tuple.get_mut(usize::from(child_col)) {
                    *v = Value::Null;
                }
            }
            fk.child_heap.update_tuple(tid, rid, &child_tuple)?;
            Self::sync_fk_child_indexes(&fk.child_indexes, tid, rid, &old, &child_tuple)?;
        }
        Ok(())
    }

    /// Maintains child indexes after a CASCADE or SET NULL row modification.
    ///
    /// Only indexes whose key actually changed are touched (same per-row skip logic
    /// used in the main UPDATE path).
    fn sync_fk_child_indexes(
        child_indexes: &[Arc<LiveIndex>],
        tid: TransactionId,
        rid: RecordId,
        old: &Tuple,
        new: &Tuple,
    ) -> Result<(), EngineError> {
        for index in child_indexes {
            let old_key = index.create_index_key(old)?;
            let new_key = index.create_index_key(new)?;
            if old_key != new_key {
                index.access.delete(tid, &old_key, rid)?;
                index.access.insert(tid, &new_key, rid)?;
            }
        }
        Ok(())
    }

    /// Executes `UPDATE <table> SET … [WHERE <predicate>]` and maintains affected indexes.
    ///
    /// The binder resolves each assignment's target column to an index in the table schema and
    /// evaluates the assignment expressions against the *old* row to produce new values. This
    /// executor step:
    ///
    /// - Collects all rows matching the optional `WHERE` predicate.
    /// - Applies the assignment list to each tuple and writes it back to the heap.
    /// - Updates indexes whose covered columns intersect the assignment set.
    ///
    /// Index maintenance is pruned in two ways:
    ///
    /// - **Statement-level pruning:** if none of an index's `table_columns` appear in the
    ///   assignments, the index is skipped for every row.
    /// - **Row-level pruning:** even for affected indexes, if the projected key is unchanged for a
    ///   particular row, the delete+insert pair is skipped.
    ///
    /// # SQL examples
    ///
    /// ```sql
    /// -- 1. Unconditional update
    /// --
    /// --   UPDATE users SET email = 'x@y';
    /// --
    /// --   BoundUpdate {
    /// --       name: "users",
    /// --       file_id: <resolved>,
    /// --       assignments: [(1, Value::String("x@y"))],
    /// --       filter: None,
    /// --       ..
    /// --   }
    /// ```
    ///
    /// ```sql
    /// -- 2. Conditional update
    /// --
    /// --   UPDATE users SET email = 'x@y' WHERE id = 1;
    /// --
    /// --   BoundUpdate { assignments: [(1, ..)], filter: Some(BooleanExpression::Leaf { .. }), .. }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a type error if applying an assignment violates the table schema (e.g. wrong type
    /// for the target column). Propagates heap update and index maintenance failures.
    pub(super) fn exec_update(
        &self,
        statement: UpdateStatement,
    ) -> Result<StatementResult, EngineError> {
        self.bind_and_execute(Statement::Update(statement), |catalog, bound, txn| {
            let Bound::Update(BoundUpdate {
                name,
                file_id,
                schema,
                assignments,
                filter,
            }) = bound
            else {
                unreachable!("binder returned non-Update variant for Update input");
            };

            let heap_file = catalog.get_table_heap(file_id)?;
            let txn_id = txn.transaction_id();
            let rows = Self::collect_matching(&heap_file, txn_id, filter.as_ref())?;
            let updated = rows.len();

            let affected_indexes =
                Self::get_affected_indices(assignments.as_slice(), catalog, file_id);

            let assignment_cols: HashSet<ColumnId> = assignments.iter().map(|(c, _)| *c).collect();

            // Outbound FK checks: only FKs whose local columns appear in the assignment set.
            let outbound_fk_checks: Vec<FkCheck> = Self::build_fk_checks(catalog, txn, file_id)?
                .into_iter()
                .filter(|fk| {
                    fk.local_ref_pairs
                        .iter()
                        .any(|&(local_col, _)| assignment_cols.contains(&local_col))
                })
                .collect();

            // Inbound FK checks: only those whose ref columns appear in the assignment set.
            let all_inbound = Self::build_inbound_fk_checks(catalog, txn, file_id)?;
            let inbound_checks: Vec<InboundFkCheck> = all_inbound
                .into_iter()
                .filter(|fk| {
                    fk.col_pairs
                        .iter()
                        .any(|&(_, ref_col)| assignment_cols.contains(&ref_col))
                })
                .collect();

            let needs_old = !affected_indexes.is_empty()
                || !outbound_fk_checks.is_empty()
                || !inbound_checks.is_empty();

            let unique_checks: HashMap<IndexId, String> = if affected_indexes.is_empty() {
                HashMap::new()
            } else {
                let info = catalog.get_table_info_by_id(txn, file_id)?;
                info.unique_constraints
                    .into_iter()
                    .filter_map(|uc| {
                        uc.backing_index_id
                            .map(|id| (id, uc.name.as_str().to_owned()))
                    })
                    .collect()
            };

            for (rid, mut tuple) in rows {
                let old_tuple = needs_old.then(|| tuple.clone());
                Self::apply_update_assignments(&mut tuple, &assignments, &schema)?;

                if let Some(old) = &old_tuple {
                    Self::check_unique_for_update(
                        &affected_indexes,
                        &unique_checks,
                        txn_id,
                        old,
                        &tuple,
                    )?;
                    Self::check_outbound_fks_for_update(&outbound_fk_checks, old, &tuple, txn_id)?;
                    Self::apply_inbound_fk_on_update(
                        catalog,
                        txn,
                        &inbound_checks,
                        old,
                        &tuple,
                        txn_id,
                    )?;
                }

                heap_file.update_tuple(txn_id, rid, &tuple)?;

                if let Some(old) = &old_tuple {
                    Self::sync_indexes_for_updated_row(
                        &affected_indexes,
                        txn_id,
                        rid,
                        old,
                        &tuple,
                    )?;
                }
            }

            Ok(StatementResult::updated(name, updated))
        })
    }

    /// Returns only the indexes that must be maintained for this `UPDATE`.
    ///
    /// An index is considered "affected" when at least one of its covered
    /// table columns appears in `assignments`. This is a statement-level
    /// pruning step used before row-by-row update processing, so unaffected
    /// indexes are skipped entirely (no key projection and no index I/O).
    ///
    /// # Parameters
    ///
    /// - `assignments`: bound `SET` pairs as `(column_id, value)`.
    /// - `catalog`: source of live index metadata for the target table.
    /// - `file_id`: target table identifier.
    ///
    /// # Returns
    ///
    /// A vector of live indexes whose `table_columns` intersect the assignment
    /// column set.
    fn get_affected_indices(
        assignments: &[(ColumnId, Value)],
        catalog: &Catalog,
        file_id: FileId,
    ) -> Vec<Arc<LiveIndex>> {
        let touched_cols: HashSet<ColumnId> = assignments.iter().map(|(c, _)| *c).collect();

        let indexes = catalog.indexes_for(file_id);
        indexes
            .into_iter()
            .filter_map(|live| {
                for col in &live.table_columns {
                    if touched_cols.contains(col) {
                        return Some(live);
                    }
                }
                None
            })
            .collect::<Vec<_>>()
    }

    /// Applies all bound `SET` assignments to a tuple in-place.
    ///
    /// Each `(column_id, value)` pair is written into `tuple` using the table
    /// `schema` for type/nullability checks.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::TypeError`] if any assignment violates schema
    /// constraints (e.g. incompatible type or nullability).
    fn apply_update_assignments(
        tuple: &mut Tuple,
        assignments: &[(ColumnId, Value)],
        schema: &TupleSchema,
    ) -> Result<(), EngineError> {
        for (col_id, value) in assignments {
            tuple
                .set_field(usize::from(*col_id), value.clone(), schema)
                .map_err(|e| EngineError::type_error(e.to_string()))?;
        }
        Ok(())
    }

    /// Checks that applying `new_tuple` would not violate any UNIQUE constraint.
    ///
    /// Called once per row before `heap_file.update_tuple`, so a violation is caught
    /// before any heap or index writes happen for that row.
    ///
    /// Only indexes in `affected_indexes` (those whose columns appear in the assignment
    /// list) can possibly be violated — unaffected indexes are skipped. Within those,
    /// only indexes whose `index_id` appears in `unique_checks` enforce uniqueness.
    ///
    /// The `old_key == new_key` case is explicitly allowed: the current row already
    /// holds that key, so no duplicate is introduced.
    fn check_unique_for_update(
        affected_indexes: &[Arc<LiveIndex>],
        unique_checks: &HashMap<IndexId, String>,
        txn_id: TransactionId,
        old_tuple: &Tuple,
        new_tuple: &Tuple,
    ) -> Result<(), EngineError> {
        for live in affected_indexes {
            let Some(constraint) = unique_checks.get(&live.index_id) else {
                continue;
            };
            let old_key = live.create_index_key(old_tuple)?;
            let new_key = live.create_index_key(new_tuple)?;
            if old_key != new_key && !live.access.search(txn_id, &new_key)?.is_empty() {
                return Err(EngineError::UniqueViolation {
                    constraint: constraint.clone(),
                });
            }
        }
        Ok(())
    }

    /// Synchronizes all affected indexes for one updated row.
    ///
    /// Checks that updating a child row does not break any outgoing FK constraint.
    ///
    /// For each FK in `outbound_checks`, compares the old and new values of the
    /// FK's local columns. If they changed, verifies the new value exists in the
    /// parent table (index lookup or heap scan via [`Self::fk_ref_exists`]).
    ///
    /// Skips FKs where the local columns are unchanged — same optimisation as
    /// `check_unique_for_update` skipping `old_key == new_key`.
    fn check_outbound_fks_for_update(
        outbound_checks: &[FkCheck],
        old_tuple: &Tuple,
        new_tuple: &Tuple,
        tid: TransactionId,
    ) -> Result<(), EngineError> {
        for fk in outbound_checks {
            let cols_changed = fk.local_ref_pairs.iter().any(|&(local_col, _)| {
                old_tuple.get(usize::from(local_col)) != new_tuple.get(usize::from(local_col))
            });
            if cols_changed && !Self::fk_ref_exists(fk, new_tuple, tid)? {
                return Err(EngineError::ForeignKeyViolation {
                    constraint: fk.name.clone(),
                });
            }
        }
        Ok(())
    }

    /// For each index, this computes the key from `old_tuple` and `new_tuple`.
    /// If the key is unchanged, no index write is performed. If the key changed,
    /// the old `(key, rid)` mapping is deleted and the new mapping is inserted.
    ///
    /// # Errors
    ///
    /// Propagates key-projection and index access-method errors.
    fn sync_indexes_for_updated_row(
        affected_indexes: &[Arc<LiveIndex>],
        txn_id: TransactionId,
        rid: RecordId,
        old_tuple: &Tuple,
        new_tuple: &Tuple,
    ) -> Result<(), EngineError> {
        // Per-row pruning: even when an index is affected at the statement
        // level, this specific row's projected key might be unchanged.
        for live in affected_indexes {
            let old_key = live.create_index_key(old_tuple)?;
            let new_key = live.create_index_key(new_tuple)?;
            if old_key != new_key {
                live.access.delete(txn_id, &old_key, rid)?;
                live.access.insert(txn_id, &new_key, rid)?;
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
        engine::Engine,
        index::{CompositeKey, IndexKind},
        parser::Parser,
        primitives::{ColumnId, NonEmptyString},
        transaction::TransactionManager,
        tuple::{Field, TupleSchema},
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

    fn field(name: &str, col_type: Type) -> Field {
        Field::new_non_empty(NonEmptyString::new(name).unwrap(), col_type)
    }

    // After INSERT, every registered index on the table contains the new
    // (key, rid) pair: index search by key returns exactly the record id
    // the heap insert produced.
    #[test]
    fn insert_updates_single_column_hash_index() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        // users(id INT64, email STRING).
        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("email", Type::String).not_null(),
                ]),
                None,
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

        // After commit, both rows must be visible from the index.
        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        let key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "exactly one rid for unique email");

        // Sanity: the rid the index returned actually points into the user
        // table's heap (file_id matches).
        assert_eq!(hits[0].file_id, table_file_id);
    }

    // Composite indexes pull values from declaration order — search must
    // return hits keyed on (col_b, col_a), not (col_a, col_b).
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
                None,
            )
            .unwrap();
        // Index on (b, a) — declaration order matters.
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

        // Correct order (b, a) = (20, 10) → hit.
        let key_ok = CompositeKey::new(vec![Value::Int64(20), Value::Int64(10)]);
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key_ok)
            .unwrap();
        assert_eq!(hits.len(), 1);

        // Swapped (a, b) = (10, 20) → no hit.
        let key_swapped = CompositeKey::new(vec![Value::Int64(10), Value::Int64(20)]);
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &key_swapped)
            .unwrap();
        probe_txn.commit().unwrap();
        assert!(miss.is_empty(), "swapped key must not match");
    }

    // INSERT into a table with no indexes still works — the no-index path
    // skips the projection loop entirely.
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
                None,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO noidx (x) VALUES (1), (2), (3);");
        // No assertion needed beyond "this didn't panic or error" — the
        // smoke test is that the no-index branch runs.
    }

    // After DELETE, the deleted rows' index entries are gone; surviving
    // rows still resolve via the index.
    #[test]
    fn delete_removes_index_entries_for_deleted_rows() {
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
                None,
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
        run(&engine, "DELETE FROM users WHERE id = 1;");

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();

        // Deleted row's key — must miss.
        let deleted_key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &deleted_key)
            .unwrap();
        assert!(
            miss.is_empty(),
            "index entry for deleted row should be gone, got {miss:?}"
        );

        // Surviving row's key — still hits.
        let live_key = CompositeKey::single(Value::String("c@d.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &live_key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "surviving row should still be in the index");
    }

    // Bulk delete — all rows wiped, all corresponding index entries gone.
    #[test]
    fn delete_all_clears_every_index_entry() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let table_file_id = catalog
            .create_table(
                &txn,
                "t",
                TupleSchema::new(vec![field("k", Type::Int64).not_null()]),
                None,
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "t_k_idx",
                "t",
                table_file_id,
                &[col_id(0)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO t (k) VALUES (10), (20), (30);");
        run(&engine, "DELETE FROM t;");

        let live = catalog.get_index_by_name("t_k_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        for k in [10i64, 20, 30] {
            let key = CompositeKey::single(Value::Int64(k));
            let hits = live
                .access
                .search(probe_txn.transaction_id(), &key)
                .unwrap();
            assert!(hits.is_empty(), "index entry for k={k} should be gone");
        }
        probe_txn.commit().unwrap();
    }

    // After UPDATE on an indexed column, the old key must not resolve and
    // the new key must — i.e. the index entry has actually moved.
    #[test]
    fn update_changes_index_entry_when_indexed_column_changes() {
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
                None,
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
            "INSERT INTO users (id, email) VALUES (1, 'old@b.com');",
        );
        run(
            &engine,
            "UPDATE users SET email = 'new@b.com' WHERE id = 1;",
        );

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();

        let old_key = CompositeKey::single(Value::String("old@b.com".to_string()));
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &old_key)
            .unwrap();
        assert!(miss.is_empty(), "old key must no longer resolve");

        let new_key = CompositeKey::single(Value::String("new@b.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &new_key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "new key must resolve to the updated row");
    }

    // UPDATE that only touches a non-indexed column still re-issues delete +
    // insert against every index (v1 isn't optimized to skip), but the
    // resulting index state must be correct: the indexed key still hits.
    #[test]
    fn update_unrelated_column_keeps_index_consistent() {
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
                None,
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
            "INSERT INTO users (id, email) VALUES (1, 'a@b.com');",
        );
        // Update only `id`; email is untouched.
        run(&engine, "UPDATE users SET id = 99 WHERE id = 1;");

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        let key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(
            hits.len(),
            1,
            "indexed column was untouched — entry must remain"
        );
    }

    // UPDATE on a table without indexes uses the lean path: no clone, no
    // fan-out. Smoke test that it still applies assignments correctly.
    #[test]
    fn update_on_table_without_indexes_still_works() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "noidx",
                TupleSchema::new(vec![
                    field("x", Type::Int64).not_null(),
                    field("y", Type::Int64).not_null(),
                ]),
                None,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO noidx (x, y) VALUES (1, 10), (2, 20);");
        run(&engine, "UPDATE noidx SET y = 999 WHERE x = 1;");
    }

    // Updating only one column of a composite (a, b) index must still
    // rotate the index entry — the new key has the changed component, the
    // old key must miss.
    #[test]
    fn update_partial_composite_index_rotates_entry() {
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
                None,
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "t_ab_idx",
                "t",
                table_file_id,
                &[col_id(0), col_id(1)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO t (a, b) VALUES (10, 20);");
        run(&engine, "UPDATE t SET b = 99 WHERE a = 10;");

        let live = catalog.get_index_by_name("t_ab_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();

        let old_key = CompositeKey::new(vec![Value::Int64(10), Value::Int64(20)]);
        let miss = live
            .access
            .search(probe_txn.transaction_id(), &old_key)
            .unwrap();
        assert!(miss.is_empty(), "old composite key must no longer resolve");

        let new_key = CompositeKey::new(vec![Value::Int64(10), Value::Int64(99)]);
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &new_key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(hits.len(), 1, "rotated key must resolve to the row");
    }

    // Update assigning the same value to an indexed column — projected key
    // is unchanged. The result must still resolve via the indexed key
    // (the per-row equality skip is invisible from outside).
    #[test]
    fn update_self_assign_indexed_column_keeps_entry() {
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
                None,
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
            "INSERT INTO users (id, email) VALUES (1, 'a@b.com');",
        );
        // Assigning the same literal — projected key on `email` doesn't change.
        run(&engine, "UPDATE users SET email = 'a@b.com' WHERE id = 1;");

        let live = catalog.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr.begin().unwrap();
        let key = CompositeKey::single(Value::String("a@b.com".to_string()));
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(
            hits.len(),
            1,
            "self-assign must leave the index entry untouched and resolvable"
        );
    }

    // Two indexes, one touched and one not, on the same table — UPDATE
    // touching only the second index's columns must:
    // - leave the first index's entries intact (unchanged keys still hit), AND
    // - rotate the second index's entries correctly.
    // This is the integration test for statement-level pruning.
    #[test]
    fn update_only_touches_indexes_whose_columns_changed() {
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
                    field("name", Type::String).not_null(),
                ]),
                None,
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
        catalog
            .create_index(
                &txn,
                "users_name_idx",
                "users",
                table_file_id,
                &[col_id(2)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(
            &engine,
            "INSERT INTO users (id, email, name) VALUES (1, 'a@b.com', 'Ada');",
        );
        // Update name only — email index must remain at rest.
        run(&engine, "UPDATE users SET name = 'Grace' WHERE id = 1;");

        let probe_txn = txn_mgr.begin().unwrap();

        let email_idx = catalog.get_index_by_name("users_email_idx").unwrap();
        let email_hits = email_idx
            .access
            .search(
                probe_txn.transaction_id(),
                &CompositeKey::single(Value::String("a@b.com".to_string())),
            )
            .unwrap();
        assert_eq!(
            email_hits.len(),
            1,
            "email index entry must remain — its column wasn't touched"
        );

        let name_idx = catalog.get_index_by_name("users_name_idx").unwrap();
        let old_name_miss = name_idx
            .access
            .search(
                probe_txn.transaction_id(),
                &CompositeKey::single(Value::String("Ada".to_string())),
            )
            .unwrap();
        assert!(old_name_miss.is_empty(), "old name key must miss");
        let new_name_hits = name_idx
            .access
            .search(
                probe_txn.transaction_id(),
                &CompositeKey::single(Value::String("Grace".to_string())),
            )
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(new_name_hits.len(), 1, "new name key must hit");
    }

    // DELETE on a table without indexes — pure heap path, no fan-out.
    #[test]
    fn delete_on_table_without_indexes_still_works() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_table(
                &txn,
                "noidx",
                TupleSchema::new(vec![field("x", Type::Int64).not_null()]),
                None,
            )
            .unwrap();
        txn.commit().unwrap();

        let engine = Engine::new(&catalog, &txn_mgr);
        run(&engine, "INSERT INTO noidx (x) VALUES (1), (2), (3);");
        run(&engine, "DELETE FROM noidx WHERE x = 2;");
    }
}

#[cfg(test)]
mod fk_tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        FileId, Type, Value,
        buffer_pool::page_store::PageStore,
        catalog::{ConstraintDef, manager::Catalog, systable::FkAction},
        engine::{Engine, EngineError, StatementResult},
        index::IndexKind,
        parser::Parser,
        primitives::{ColumnId, NonEmptyString},
        transaction::TransactionManager,
        tuple::{Field, Tuple, TupleSchema},
        wal::writer::Wal,
    };

    // ── infra ─────────────────────────────────────────────────────────────────

    fn make_infra(dir: &Path) -> (Catalog, TransactionManager) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).unwrap());
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        let catalog = Catalog::initialize(&bp, &wal, dir).unwrap();
        let txn_mgr = TransactionManager::new(wal, bp);
        (catalog, txn_mgr)
    }

    fn col(i: usize) -> ColumnId {
        ColumnId::try_from(i).unwrap()
    }

    fn field(name: &str, ty: Type) -> Field {
        Field::new_non_empty(NonEmptyString::new(name).unwrap(), ty)
    }

    /// Runs `sql` and expects success, panicking otherwise.
    fn run(engine: &Engine<'_>, sql: &str) {
        let stmt = Parser::new(sql).parse().expect("parse");
        engine.execute_statement(stmt).expect("execute");
    }

    /// Runs `sql` and returns the engine result (success or error).
    fn try_run(engine: &Engine<'_>, sql: &str) -> Result<StatementResult, EngineError> {
        let stmt = Parser::new(sql).parse().expect("parse");
        engine.execute_statement(stmt)
    }

    /// Runs a SELECT and returns the result rows.
    fn select_rows(engine: &Engine<'_>, sql: &str) -> Vec<Tuple> {
        let stmt = Parser::new(sql).parse().expect("parse");
        match engine.execute_statement(stmt).expect("select") {
            StatementResult::Selected { rows, .. } => rows,
            other => panic!("expected Selected, got {other:?}"),
        }
    }

    /// Creates `users(id INT64 NOT NULL, name STRING NOT NULL)` with a UNIQUE
    /// `BTree` index on `id`, and `orders(id INT64 NOT NULL, user_id INT64,
    /// amount INT64 NOT NULL)` with a FK `user_id → users.id`.
    ///
    /// `user_id` is nullable so that SET NULL tests can write NULL there.
    fn setup_schema(
        catalog: &Catalog,
        txn_mgr: &TransactionManager,
        on_delete: Option<FkAction>,
        on_update: Option<FkAction>,
    ) -> (FileId, FileId) {
        let txn = txn_mgr.begin().unwrap();

        let users = catalog
            .create_table(
                &txn,
                "users",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("name", Type::String).not_null(),
                ]),
                None,
            )
            .unwrap();

        // FK referenced columns must be PK or UNIQUE — create a Btree-backed unique on users.id.
        let idx_id = catalog
            .create_index(
                &txn,
                "users_id_idx",
                "users",
                users,
                &[col(0)],
                IndexKind::Btree,
            )
            .unwrap();
        catalog
            .add_constraint(&txn, users, ConstraintDef::Unique {
                name: "users_id_unique".to_owned(),
                columns: vec![col(0)],
                backing_index_id: Some(idx_id),
            })
            .unwrap();

        let orders = catalog
            .create_table(
                &txn,
                "orders",
                TupleSchema::new(vec![
                    field("id", Type::Int64).not_null(),
                    field("user_id", Type::Int64), // nullable for SET NULL tests
                    field("amount", Type::Int64).not_null(),
                ]),
                None,
            )
            .unwrap();

        catalog
            .add_constraint(&txn, orders, ConstraintDef::ForeignKey {
                name: "orders_user_fk".to_owned(),
                local_columns: vec![col(1)],
                ref_table_id: users,
                ref_columns: vec![col(0)],
                on_delete,
                on_update,
            })
            .unwrap();

        txn.commit().unwrap();
        (users, orders)
    }

    #[test]
    fn insert_child_with_valid_parent_ref_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        // user 1 exists — order should insert cleanly.
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );
    }

    #[test]
    fn insert_child_with_missing_parent_ref_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        // No user with id=99 exists.
        let err = try_run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 99, 100);",
        )
        .unwrap_err();
        assert!(
            matches!(err, EngineError::ForeignKeyViolation { .. }),
            "expected ForeignKeyViolation, got {err:?}"
        );
    }

    #[test]
    fn insert_child_with_null_fk_column_is_exempt() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        // NULL user_id is allowed — no parent lookup performed.
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, NULL, 100);",
        );
    }

    // ── UPDATE child: outbound check ─────────────────────────────────────────

    #[test]
    fn update_child_fk_col_to_valid_ref_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(
            &engine,
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');",
        );
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        // Reassign to another existing user — valid.
        run(&engine, "UPDATE orders SET user_id = 2 WHERE id = 10;");
    }

    #[test]
    fn update_child_fk_col_to_missing_ref_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        // user 99 does not exist.
        let err = try_run(&engine, "UPDATE orders SET user_id = 99 WHERE id = 10;").unwrap_err();
        assert!(
            matches!(err, EngineError::ForeignKeyViolation { .. }),
            "expected ForeignKeyViolation, got {err:?}"
        );
    }

    #[test]
    fn update_child_non_fk_col_skips_fk_check() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        // Updating `amount` does not touch the FK column — no lookup triggered.
        run(&engine, "UPDATE orders SET amount = 999 WHERE id = 10;");
    }

    #[test]
    fn update_child_fk_col_same_value_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        // Setting user_id to the value it already has — old == new, lookup is skipped.
        run(&engine, "UPDATE orders SET user_id = 1 WHERE id = 10;");
    }

    // ── DELETE parent: inbound RESTRICT ──────────────────────────────────────

    #[test]
    fn delete_parent_with_no_dependents_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        // No orders reference user 1 — delete is safe.
        run(&engine, "DELETE FROM users WHERE id = 1;");
    }

    #[test]
    fn delete_parent_with_dependents_restrict_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        // Default (None) → NoAction which behaves as Restrict.
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        let err = try_run(&engine, "DELETE FROM users WHERE id = 1;").unwrap_err();
        assert!(
            matches!(err, EngineError::FkParentViolation { .. }),
            "expected FkParentViolation, got {err:?}"
        );
    }

    // ── DELETE parent: CASCADE ────────────────────────────────────────────────

    #[test]
    fn delete_parent_cascade_removes_child_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, Some(FkAction::Cascade), None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100), (11, 1, 200);",
        );

        run(&engine, "DELETE FROM users WHERE id = 1;");

        // Both child rows must be gone.
        let rows = select_rows(&engine, "SELECT * FROM orders;");
        assert!(
            rows.is_empty(),
            "cascade delete must remove all child rows, got {rows:?}"
        );
    }

    #[test]
    fn delete_parent_cascade_leaves_unrelated_child_rows() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, Some(FkAction::Cascade), None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(
            &engine,
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');",
        );
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100), (11, 2, 200);",
        );

        run(&engine, "DELETE FROM users WHERE id = 1;");

        // Only order 10 (user 1) should be gone; order 11 (user 2) stays.
        let rows = select_rows(&engine, "SELECT * FROM orders;");
        assert_eq!(
            rows.len(),
            1,
            "only the cascade-targeted row should be removed"
        );
        assert_eq!(
            rows[0].get(0),
            Some(&Value::Int64(11)),
            "surviving order must be id=11"
        );
    }

    // ── DELETE parent: SET NULL ───────────────────────────────────────────────

    #[test]
    fn delete_parent_set_null_nullifies_child_fk_col() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, Some(FkAction::SetNull), None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        run(&engine, "DELETE FROM users WHERE id = 1;");

        // Order row must still exist but user_id (col 1) must now be NULL.
        let rows = select_rows(&engine, "SELECT * FROM orders;");
        assert_eq!(rows.len(), 1, "order row must survive SET NULL");
        assert_eq!(
            rows[0].get(1),
            Some(&Value::Null),
            "user_id must be NULL after SET NULL delete"
        );
    }

    // ── UPDATE parent: inbound RESTRICT ──────────────────────────────────────

    #[test]
    fn update_parent_ref_col_with_dependents_restrict_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        // Changing users.id while orders.user_id still references it.
        let err = try_run(&engine, "UPDATE users SET id = 999 WHERE id = 1;").unwrap_err();
        assert!(
            matches!(err, EngineError::FkParentViolation { .. }),
            "expected FkParentViolation, got {err:?}"
        );
    }

    #[test]
    fn update_parent_non_ref_col_with_dependents_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        // Updating `name` does not touch the referenced column — safe.
        run(&engine, "UPDATE users SET name = 'Alicia' WHERE id = 1;");
    }

    // ── UPDATE parent: CASCADE ────────────────────────────────────────────────

    #[test]
    fn update_parent_ref_col_cascade_updates_child_fk_col() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, Some(FkAction::Cascade));
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        run(&engine, "UPDATE users SET id = 42 WHERE id = 1;");

        // Child's user_id must have followed the parent's new id.
        let rows = select_rows(&engine, "SELECT * FROM orders;");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(1),
            Some(&Value::Int64(42)),
            "cascade update must propagate new parent key to child"
        );
    }

    // ── UPDATE parent: SET NULL ───────────────────────────────────────────────

    #[test]
    fn update_parent_ref_col_set_null_nullifies_child_fk_col() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, Some(FkAction::SetNull));
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        run(&engine, "UPDATE users SET id = 42 WHERE id = 1;");

        // Child's user_id must be NULL (set-null action on update).
        let rows = select_rows(&engine, "SELECT * FROM orders;");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(1),
            Some(&Value::Null),
            "set-null update must nullify child FK column"
        );
    }

    // ── SQL CREATE TABLE with FOREIGN KEY ─────────────────────────────────────

    // FK declared inline in CREATE TABLE is enforced just like one added via the
    // catalog API: inserting a child row with a missing parent ref must fail.
    #[test]
    fn create_table_with_fk_enforces_constraint() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        run(
            &engine,
            "CREATE TABLE users (id INT NOT NULL, name VARCHAR NOT NULL, UNIQUE (id));",
        );
        run(
            &engine,
            "CREATE TABLE orders (id INT NOT NULL, user_id INT, FOREIGN KEY (user_id) REFERENCES users (id));",
        );

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");

        // Valid child row — parent exists.
        run(&engine, "INSERT INTO orders (id, user_id) VALUES (10, 1);");

        // Missing parent — must be rejected.
        let err =
            try_run(&engine, "INSERT INTO orders (id, user_id) VALUES (20, 99);").unwrap_err();
        assert!(
            matches!(err, EngineError::ForeignKeyViolation { .. }),
            "expected ForeignKeyViolation, got {err:?}"
        );
    }

    // Deleting the parent row while a child still references it must be blocked.
    #[test]
    fn create_table_with_fk_blocks_parent_delete() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        let engine = Engine::new(&catalog, &txn_mgr);

        run(
            &engine,
            "CREATE TABLE users (id INT NOT NULL, name VARCHAR NOT NULL, UNIQUE (id));",
        );
        run(
            &engine,
            "CREATE TABLE orders (id INT NOT NULL, user_id INT, FOREIGN KEY (user_id) REFERENCES users (id));",
        );

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(&engine, "INSERT INTO orders (id, user_id) VALUES (10, 1);");

        let err = try_run(&engine, "DELETE FROM users WHERE id = 1;").unwrap_err();
        assert!(
            matches!(err, EngineError::FkParentViolation { .. }),
            "expected FkParentViolation, got {err:?}"
        );
    }
}
