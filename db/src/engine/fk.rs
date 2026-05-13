//! Foreign-key constraint checking and referential-action enforcement.
//!
//! This module handles both directions of a foreign-key relationship:
//!
//! - **Outbound** ("I am the child") — before inserting or updating a child row, confirm that the
//!   referenced parent value already exists. See [`ParentFkCheck`] and
//!   [`Engine::prepare_outbound_ref_checks`].
//! - **Inbound** ("I am the parent") — before deleting or updating a parent row, find every child
//!   row that still points at the old key and apply the declared referential action (`RESTRICT`,
//!   `CASCADE`, `SET NULL`, …). See [`InboundParentFkCheck`] and
//!   [`Engine::prepare_inbound_ref_checks`].
//!
//! ## Two-phase design
//!
//! Both directions follow the same pattern:
//!
//! 1. **Prepare once per statement** — build a list of check descriptors from the catalog. This is
//!    done once so catalog lookups are not repeated per row.
//! 2. **Execute per row** — apply the checks to each tuple as it is processed.
//!
//! ## Index fast path vs heap-scan fallback
//!
//! Checking whether a parent row exists (or finding child rows) can be done two ways:
//!
//! - **Index point-lookup** — if an index covers the exact set of FK columns, build a
//!   [`CompositeKey`] and call `index.search(...)`. This is O(log n) or O(1) depending on index
//!   kind.
//! - **Heap scan fallback** — when no covering index exists, scan every row in the heap and compare
//!   column values pairwise. This is O(n).
//!
//! [`Engine::find_covering_index`] picks the fast path when available; the descriptors store
//! whichever strategy applies so per-row code needs no catalog access.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use fallible_iterator::FallibleIterator;

use crate::{
    FileId, TransactionId, Value,
    catalog::{LiveIndex, ReferencingFk, manager::Catalog, systable::FkAction},
    engine::{ConstraintViolation, Engine, EngineError},
    execution::expression::BooleanExpression,
    heap::file::HeapFile,
    index::CompositeKey,
    primitives::{ColumnId, Predicate, RecordId},
    transaction::Transaction,
    tuple::Tuple,
};

/// Precomputed data for one outgoing FK constraint, built once per INSERT/UPDATE statement.
///
/// "Outgoing" means this table is the **child**: it holds the FK column(s) that must reference
/// a row in the parent table. One `ParentFkCheck` is built per FK declared on the child table
/// and cached for the lifetime of a single statement so that catalog lookups are not repeated
/// for every row.
///
/// At check time, [`Engine::referenced_row_exists`] uses either the index fast path or the
/// heap-scan fallback depending on which field is populated.
pub(super) struct ParentFkCheck {
    /// The constraint name as declared in `FOREIGN KEY … CONSTRAINT <name>`, used in error
    /// messages when the check fails.
    pub name: String,
    /// Open handle to the parent table's heap file, used for the heap-scan fallback.
    pub ref_heap: Arc<HeapFile>,
    /// Fast path: a parent index whose key columns exactly cover the referenced columns,
    /// paired with the child column IDs in index-key order.
    ///
    /// When `Some`, [`Engine::referenced_row_exists`] projects the child tuple straight into
    /// a [`CompositeKey`] and does a point-lookup without touching the heap.
    /// When `None`, the heap-scan fallback via `local_ref_pairs` is used instead.
    pub index_lookup: Option<(Arc<LiveIndex>, Vec<ColumnId>)>,
    /// Fallback column pairs `(child_col, parent_ref_col)` used when no covering index exists.
    ///
    /// For each pair, the value at `child_col` in the child tuple must equal the value at
    /// `parent_ref_col` in some row of the parent heap.
    pub local_ref_pairs: Vec<(ColumnId, ColumnId)>,
}

/// Precomputed data for one inbound FK, built once per DELETE/UPDATE statement.
///
/// "Inbound" means this table is the **parent**: other tables' rows reference it via a
/// foreign key. One `InboundParentFkCheck` is built for every FK in the database that points
/// at the parent table, and cached for the lifetime of a single statement.
///
/// When a parent row is about to be deleted or its referenced columns updated,
/// [`Engine::find_referencing_rows`] uses this descriptor to locate the child rows, and
/// [`Engine::enforce_referential_actions_on_delete`] /
/// [`Engine::enforce_referential_actions_on_update`] apply the correct referential action.
pub(super) struct InboundParentFkCheck {
    /// The constraint name as declared in the child table's `FOREIGN KEY` definition.
    pub constraint_name: String,
    /// Catalog identifier for the child table's heap file.
    pub child_table_id: FileId,
    /// Open handle to the child table's heap, used to read and write child rows.
    pub child_heap: Arc<HeapFile>,
    /// Column pairs `(child_col, parent_ref_col)` that define the join condition between the
    /// child and this parent table.
    pub col_pairs: Vec<(ColumnId, ColumnId)>,
    /// Action to take on child rows when a matching parent row is **deleted**.
    pub on_delete: FkAction,
    /// Action to take on child rows when a matching parent row's referenced columns are
    /// **updated**.
    pub on_update: FkAction,
    /// Fast lookup of child rows: a child-side index whose key columns exactly cover the FK
    /// columns, paired with the parent ref-column IDs that feed each key position.
    ///
    /// When `Some`, [`Engine::find_referencing_rows`] builds a [`CompositeKey`] from the
    /// parent row's ref-column values and probes the child index directly.
    /// When `None`, a full heap scan of the child table is used instead.
    pub fk_lookup_index: Option<(Arc<LiveIndex>, Vec<ColumnId>)>,
    /// Every index on the child table, kept for synchronization after a `CASCADE` update or
    /// `SET NULL` operation modifies child rows.
    pub maintenance_indexes: Vec<Arc<LiveIndex>>,
}

/// A before/after tuple pair for a single-row mutation (UPDATE, CASCADE, SET NULL).
///
/// Bundles the pre- and post-mutation state so callers cannot silently swap the two
/// `&Tuple` arguments that appear in every constraint-check and index-sync function.
/// `Copy` because both fields are shared references.
#[derive(Clone, Copy)]
pub(super) struct RowChange<'a> {
    /// The tuple as it existed before the mutation.
    pub before: &'a Tuple,
    /// The tuple as it will look after the mutation is applied.
    pub after: &'a Tuple,
}

impl Engine<'_> {
    /// Builds one [`ParentFkCheck`] per foreign key declared on the table `file_id`.
    ///
    /// **Outgoing** here means `file_id` is the *referencing* (child) table: each
    /// constraint names a parent table and columns that must already exist there
    /// before a child row is accepted. Callers cache this list once per statement
    /// (for example INSERT/UPDATE) and run [`Self::referenced_row_exists`] per row.
    ///
    /// # Arguments
    ///
    /// - `file_id`: catalog id of the child table whose FK definitions are read.
    ///
    /// # Returns
    ///
    /// An empty vector when the table has no foreign keys; otherwise one entry per
    /// constraint, in catalog order, holding the parent heap handle and either an
    /// index fast path or heap-scan fallback data.
    ///
    /// # Index vs heap scan
    ///
    /// For each FK, [`Self::find_covering_index`] picks a parent index whose key columns
    /// are exactly the referenced parent columns (same set as `fk.ref_columns`, no
    /// extra prefix columns). The first such index wins.
    ///
    /// When present, [`ParentFkCheck::index_lookup`] stores that index plus the child column IDs
    /// in index key order so a child tuple can be projected straight into a
    /// [`CompositeKey`]. If no index matches, [`ParentFkCheck::local_ref_pairs`]
    /// supplies `(local_col, ref_col)` pairs for a linear heap scan of the parent.
    ///
    /// # Errors
    ///
    /// Propagates catalog failures (unknown table, missing heap, etc.) from
    /// [`Catalog::get_table_info_by_id`] and [`Catalog::get_table_heap`].
    pub(super) fn prepare_outbound_ref_checks(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
    ) -> Result<Vec<ParentFkCheck>, EngineError> {
        let info = catalog.get_table_info_by_id(txn, file_id)?;

        info.foreign_keys
            .iter()
            .map(|fk| {
                let ref_table_id = fk.ref_table_id;
                let ref_heap = catalog.get_table_heap(ref_table_id)?;
                let ref_indexes = catalog.indexes_for(ref_table_id);

                let n = fk.local_columns.len();
                let mut ref_to_local: HashMap<ColumnId, ColumnId> = HashMap::with_capacity(n);
                let mut local_ref_pairs: Vec<(ColumnId, ColumnId)> = Vec::with_capacity(n);

                for (&local, &ref_col) in fk.local_columns.iter().zip(fk.ref_columns.iter()) {
                    ref_to_local.insert(ref_col, local);
                    local_ref_pairs.push((local, ref_col));
                }

                let index_lookup = Self::find_covering_index(&ref_indexes, &ref_to_local);

                Ok(ParentFkCheck {
                    name: fk.name.as_str().to_owned(),
                    ref_heap,
                    index_lookup,
                    local_ref_pairs,
                })
            })
            .collect()
    }

    /// Returns whether the child `tuple` satisfies one outbound foreign key described by `fk`.
    ///
    /// This is the **per-row** step after [`Self::prepare_outbound_ref_checks`]: each
    /// [`ParentFkCheck`] is reused for every child row in the statement so catalog work stays
    /// in the prepare phase only.
    ///
    /// For a composite FK, every `(child_col, parent_col)` pair must agree with values from the
    /// **same** parent row — the heap scan checks each parent tuple in full; the index path
    /// encodes the whole key at once.
    ///
    /// ## NULL semantics
    ///
    /// If **any** FK column on the child is missing or [`Value::Null`], the constraint is treated
    /// as satisfied immediately (`Ok(true)`). SQL does not treat NULL as equal to anything in a
    /// reference, so the child row does not assert a parent lookup and no scan or index probe runs.
    ///
    /// ## Index fast path vs heap scan
    ///
    /// When [`ParentFkCheck::index_lookup`] is `Some`, child values are projected into a
    /// [`CompositeKey`] in index-key order and the parent index is searched; any hit means a
    /// parent row with that key exists.
    ///
    /// Otherwise the parent heap is scanned; the first row where all `local_ref_pairs` compare
    /// equal (via [`Option`] equality on column values) ends the search successfully.
    ///
    /// # Arguments
    ///
    /// - `fk`: prepared descriptor (parent heap, optional covering index, column pairs).
    /// - `tuple`: the child row being inserted or updated.
    /// - `tid`: active transaction id for heap and index visibility.
    ///
    /// # Returns
    ///
    /// - `Ok(true)` — FK satisfied (including the NULL short-circuit), or a matching parent row was
    ///   found.
    /// - `Ok(false)` — all FK columns are non-null but no parent row matches. Callers (INSERT /
    ///   UPDATE) map this to [`ConstraintViolation::ForeignKeyViolation`] using
    ///   [`ParentFkCheck::name`].
    ///
    /// # Errors
    ///
    /// Propagates I/O and index errors from the heap scan or index search, and
    /// [`EngineError::TypeError`] from [`Self::project_key`] if a projected column is missing in
    /// the tuple.
    pub(super) fn referenced_row_exists(
        fk: &ParentFkCheck,
        tuple: &Tuple,
        tid: TransactionId,
    ) -> Result<bool, EngineError> {
        let has_null = fk
            .local_ref_pairs
            .iter()
            .any(|&(local_col, _)| tuple.get_col(local_col).is_none_or(|v| *v == Value::Null));
        if has_null {
            return Ok(true);
        }

        if let Some((index, local_cols)) = &fk.index_lookup {
            let key = Self::project_key(tuple, local_cols)?;
            return Ok(!index.access.search(tid, &key)?.is_empty());
        }

        let mut scan = fk.ref_heap.scan(tid)?;
        while let Some((_, parent_row)) = FallibleIterator::next(&mut scan)? {
            let all_match = fk.local_ref_pairs.iter().all(|&(local_col, ref_col)| {
                tuple.get_col(local_col) == parent_row.get_col(ref_col)
            });
            if all_match {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Builds one [`InboundParentFkCheck`] for every FK that names `file_id` as the parent table.
    ///
    /// Queries the catalog for all foreign keys that *reference* `file_id`, then for each one
    /// opens the child heap, collects all child-table indexes (for maintenance), and attempts
    /// to find a covering child-side index for fast row lookup via [`Self::find_covering_index`].
    ///
    /// Returns an empty vector immediately when no other table references `file_id`, avoiding
    /// any catalog work.
    ///
    /// # Errors
    ///
    /// Propagates catalog failures from [`Catalog::find_referencing_fks`] and
    /// [`Catalog::get_table_heap`].
    pub(super) fn prepare_inbound_ref_checks(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        file_id: FileId,
    ) -> Result<Vec<InboundParentFkCheck>, EngineError> {
        let referencing = catalog.find_referencing_fks(txn, file_id)?;
        if referencing.is_empty() {
            return Ok(Vec::new());
        }

        referencing
            .into_iter()
            .map(
                |ReferencingFk {
                     constraint_name,
                     child_table_id,
                     column_pairs,
                     on_delete,
                     on_update,
                 }| {
                    let child_heap = catalog.get_table_heap(child_table_id)?;
                    let maintenance_indexes = catalog.indexes_for(child_table_id);

                    let child_to_ref = column_pairs
                        .iter()
                        .copied()
                        .collect::<HashMap<ColumnId, ColumnId>>();

                    let fk_lookup_index =
                        Self::find_covering_index(&maintenance_indexes, &child_to_ref);

                    Ok(InboundParentFkCheck {
                        constraint_name,
                        child_table_id,
                        child_heap,
                        col_pairs: column_pairs,
                        on_delete: on_delete.unwrap_or(FkAction::NoAction),
                        on_update: on_update.unwrap_or(FkAction::NoAction),
                        fk_lookup_index,
                        maintenance_indexes,
                    })
                },
            )
            .collect()
    }

    /// Returns every child row that references `parent_row` through the given inbound FK.
    ///
    /// ## Fast path
    ///
    /// When `fk.fk_lookup_index` is `Some`, the parent row's ref-column values are projected
    /// into a [`CompositeKey`] and the child index is probed directly.  Each matching
    /// [`RecordId`] is then fetched from the child heap to produce the full tuple.
    ///
    /// ## Heap-scan fallback
    ///
    /// When no covering index exists, a predicate is built with
    /// [`Self::build_ref_match_predicate`] and every row in the child heap is scanned.  Only
    /// rows that satisfy the predicate are returned.
    ///
    /// # Errors
    ///
    /// Returns storage errors from the index search or heap fetch/scan.
    pub(super) fn find_referencing_rows(
        fk: &InboundParentFkCheck,
        parent_row: &Tuple,
        tid: TransactionId,
    ) -> Result<Vec<(RecordId, Tuple)>, EngineError> {
        if let Some((index, ref_col_order)) = &fk.fk_lookup_index {
            let key = Self::project_key(parent_row, ref_col_order)?;
            let rids = index.access.search(tid, &key)?;
            let mut rows = Vec::new();
            for rid in rids {
                if let Some(tuple) = fk.child_heap.fetch_tuple(tid, rid)? {
                    rows.push((rid, tuple));
                }
            }
            return Ok(rows);
        }

        let predicate = Self::build_ref_match_predicate(&fk.col_pairs, parent_row);
        Self::collect_matching_rows(&fk.child_heap, tid, predicate.as_ref())
    }

    /// Builds a `WHERE child_col = <parent_value> AND …` predicate for the heap-scan fallback.
    ///
    /// Iterates over `col_pairs` and, for each `(child_col, ref_col)` pair, reads the current
    /// value of `ref_col` from `parent_row` and creates a column-equals-literal leaf expression.
    /// Multiple leaves are combined with `AND`.  Returns `None` only when `col_pairs` is empty,
    /// which should not happen for a well-formed FK definition.
    fn build_ref_match_predicate(
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

    /// Applies the declared referential action to child rows when a parent row is deleted.
    ///
    /// Iterates over every [`InboundParentFkCheck`] for this parent table.  For each one,
    /// [`Self::find_referencing_rows`] locates the child rows that still point at `parent_row`.
    /// If none exist the constraint is satisfied and the loop continues.  Otherwise the
    /// `on_delete` action determines what happens:
    ///
    /// - `RESTRICT` / `NO ACTION` / `SET DEFAULT` — returns a
    ///   [`ConstraintViolation::FkParentViolation`] error immediately.
    /// - `CASCADE` — builds a match predicate and calls [`Self::delete_rows_and_indexes`]
    ///   recursively on the child table.
    /// - `SET NULL` — calls [`Self::nullify_fk_columns`] to clear the child FK columns and update
    ///   indexes.
    ///
    /// # Errors
    ///
    /// - [`EngineError::Constraint`] → [`ConstraintViolation::FkParentViolation`] when `RESTRICT`
    ///   (or equivalent) is declared and matching child rows are found.
    /// - Storage or cascade errors propagated from [`Self::delete_rows_and_indexes`] or
    ///   [`Self::nullify_fk_columns`].
    pub(super) fn enforce_referential_actions_on_delete(
        catalog: &Catalog,
        txn: &Transaction<'_>,
        inbound_checks: &[InboundParentFkCheck],
        parent_row: &Tuple,
        tid: TransactionId,
    ) -> Result<(), EngineError> {
        for fk in inbound_checks {
            let child_rows = Self::find_referencing_rows(fk, parent_row, tid)?;
            if child_rows.is_empty() {
                continue;
            }
            match fk.on_delete {
                FkAction::NoAction | FkAction::Restrict | FkAction::SetDefault => {
                    return Err(ConstraintViolation::FkParentViolation {
                        constraint: fk.constraint_name.clone(),
                    }
                    .into());
                }
                FkAction::Cascade => {
                    let predicate = Self::build_ref_match_predicate(&fk.col_pairs, parent_row);
                    Self::delete_rows_and_indexes(
                        catalog,
                        txn,
                        fk.child_table_id,
                        predicate.as_ref(),
                    )?;
                }
                FkAction::SetNull => {
                    Self::nullify_fk_columns(fk, child_rows, tid)?;
                }
            }
        }
        Ok(())
    }

    /// Applies the declared referential action to child rows when a parent row's referenced
    /// columns change.
    ///
    /// For each [`InboundParentFkCheck`], first checks whether any ref-column value actually
    /// changed between `change.before` and `change.after` — if nothing changed the constraint
    /// cannot be violated and the loop continues.
    ///
    /// When a ref-column did change, [`Self::find_referencing_rows`] locates child rows that
    /// still point at the *old* key.  If none exist the constraint is satisfied.  Otherwise
    /// the `on_update` action determines what happens:
    ///
    /// - `RESTRICT` / `NO ACTION` / `SET DEFAULT` — returns a
    ///   [`ConstraintViolation::FkParentViolation`] error immediately.
    /// - `CASCADE` — rewrites each child row in-place so its FK columns reflect the *new* parent
    ///   key values, then calls [`Self::sync_indexes_after_fk_action`] for changed keys.
    /// - `SET NULL` — calls [`Self::nullify_fk_columns`] to clear the child FK columns and update
    ///   indexes.
    ///
    /// # Errors
    ///
    /// - [`EngineError::Constraint`] → [`ConstraintViolation::FkParentViolation`] when `RESTRICT`
    ///   (or equivalent) is declared and matching child rows are found.
    /// - Storage errors propagated from heap writes or index maintenance.
    pub(super) fn enforce_referential_actions_on_update(
        inbound_checks: &[InboundParentFkCheck],
        change: RowChange<'_>,
        tid: TransactionId,
    ) -> Result<(), EngineError> {
        for fk in inbound_checks {
            let ref_changed = fk.col_pairs.iter().any(|&(_, ref_col)| {
                change.before.get(usize::from(ref_col)) != change.after.get(usize::from(ref_col))
            });
            if !ref_changed {
                continue;
            }

            let child_rows = Self::find_referencing_rows(fk, change.before, tid)?;
            if child_rows.is_empty() {
                continue;
            }
            match fk.on_update {
                FkAction::NoAction | FkAction::Restrict | FkAction::SetDefault => {
                    return Err(ConstraintViolation::FkParentViolation {
                        constraint: fk.constraint_name.clone(),
                    }
                    .into());
                }
                FkAction::Cascade => {
                    for (rid, mut child_tuple) in child_rows {
                        let old = child_tuple.clone();
                        for &(child_col, ref_col) in &fk.col_pairs {
                            if let Some(v) = child_tuple.get_mut(usize::from(child_col)) {
                                *v = change.after
                                    .get(usize::from(ref_col))
                                    .cloned()
                                    .unwrap_or(Value::Null);
                            }
                        }
                        fk.child_heap.update_tuple(tid, rid, &child_tuple)?;
                        Self::sync_indexes_after_fk_action(
                            &fk.maintenance_indexes,
                            tid,
                            rid,
                            RowChange { before: &old, after: &child_tuple },
                        )?;
                    }
                }
                FkAction::SetNull => {
                    Self::nullify_fk_columns(fk, child_rows, tid)?;
                }
            }
        }
        Ok(())
    }

    /// Sets the FK columns of every row in `child_rows` to `NULL` and keeps indexes current.
    ///
    /// For each `(rid, tuple)` pair, clones the tuple, sets each column listed in
    /// `fk.col_pairs` to [`Value::Null`], writes the updated tuple back to the child heap via
    /// `update_tuple`, and calls [`Self::sync_indexes_after_fk_action`] to remove stale index
    /// entries and insert new ones for the modified row.
    ///
    /// # Errors
    ///
    /// Returns storage errors from the heap write or index maintenance.
    pub(super) fn nullify_fk_columns(
        fk: &InboundParentFkCheck,
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
            Self::sync_indexes_after_fk_action(
                &fk.maintenance_indexes,
                tid,
                rid,
                RowChange { before: &old, after: &child_tuple },
            )?;
        }
        Ok(())
    }

    /// Updates indexes after a `CASCADE` or `SET NULL` modification to a child row.
    ///
    /// For each index in `maintenance_indexes`, computes the key for both `change.before` and
    /// `change.after`.  When the keys differ — meaning this index covers a column that was
    /// changed — it deletes the old entry and inserts the new one.  Indexes whose key did not
    /// change are left untouched to avoid unnecessary writes.
    ///
    /// # Errors
    ///
    /// Returns storage errors from [`LiveIndex::access`] delete or insert calls.
    pub(super) fn sync_indexes_after_fk_action(
        maintenance_indexes: &[Arc<LiveIndex>],
        tid: TransactionId,
        rid: RecordId,
        change: RowChange<'_>,
    ) -> Result<(), EngineError> {
        for index in maintenance_indexes {
            let old_key = index.create_index_key(change.before)?;
            let new_key = index.create_index_key(change.after)?;
            if old_key != new_key {
                index.access.delete(tid, &old_key, rid)?;
                index.access.insert(tid, &new_key, rid)?;
            }
        }
        Ok(())
    }

    /// Projects `col_ids` out of `tuple` and wraps them in a [`CompositeKey`].
    ///
    /// Returns an error if any column index is out of bounds in the tuple.
    fn project_key(tuple: &Tuple, col_ids: &[ColumnId]) -> Result<CompositeKey, EngineError> {
        let values: Result<Vec<Value>, EngineError> = col_ids
            .iter()
            .map(|&col| {
                tuple.get(usize::from(col)).cloned().ok_or_else(|| {
                    EngineError::TypeError(format!(
                        "FK column {} out of bounds in tuple",
                        usize::from(col)
                    ))
                })
            })
            .collect();
        Ok(CompositeKey::new(values?))
    }

    /// Finds the first index whose key columns exactly match the keys of `col_remap`.
    ///
    /// `col_remap` maps the *target* column ID (e.g. a parent ref-column) to the *source*
    /// column ID (e.g. the corresponding child FK column).  An index "covers" the remap when
    /// its `table_columns` set equals the key set of `col_remap` — no extra prefix columns,
    /// no missing columns.
    ///
    /// When a covering index is found, the return value pairs it with a `Vec<ColumnId>` that
    /// lists the *source* column IDs in the same order as the index key positions, so callers
    /// can project a tuple straight into a [`CompositeKey`] without re-sorting.
    ///
    /// Returns `None` when no index covers the remap; callers fall back to a heap scan.
    fn find_covering_index(
        indexes: &[Arc<LiveIndex>],
        col_remap: &HashMap<ColumnId, ColumnId>,
    ) -> Option<(Arc<LiveIndex>, Vec<ColumnId>)> {
        let live = indexes.iter().find(|live| {
            let idx_cols: HashSet<ColumnId> = live.table_columns.iter().copied().collect();
            idx_cols == col_remap.keys().copied().collect()
        })?;
        let col_order = live
            .table_columns
            .iter()
            .map(|col| col_remap[col])
            .collect();
        Some((Arc::clone(live), col_order))
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        FileId, Type, Value,
        buffer_pool::page_store::PageStore,
        catalog::{ConstraintDef, manager::Catalog, systable::FkAction},
        engine::{ConstraintViolation, Engine, EngineError, StatementResult},
        index::IndexKind,
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

    fn col(i: usize) -> ColumnId {
        ColumnId::try_from(i).unwrap()
    }

    fn field(name: &str, ty: Type) -> Field {
        Field::new_non_empty(NonEmptyString::new(name).unwrap(), ty)
    }

    fn run(engine: &Engine<'_>, sql: &str) {
        let stmt = Parser::new(sql).parse().expect("parse");
        engine.execute_statement(stmt).expect("execute");
    }

    fn try_run(engine: &Engine<'_>, sql: &str) -> Result<StatementResult, EngineError> {
        let stmt = Parser::new(sql).parse().expect("parse");
        engine.execute_statement(stmt)
    }

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
                vec![],
            )
            .unwrap();

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
                vec![],
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

        let err = try_run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 99, 100);",
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Constraint(ConstraintViolation::ForeignKeyViolation { .. })
            ),
            "expected ForeignKeyViolation, got {err:?}"
        );
    }

    #[test]
    fn insert_child_with_null_fk_column_is_exempt() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, NULL, 100);",
        );
    }

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

        let err = try_run(&engine, "UPDATE orders SET user_id = 99 WHERE id = 10;").unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Constraint(ConstraintViolation::ForeignKeyViolation { .. })
            ),
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
        run(&engine, "UPDATE orders SET user_id = 1 WHERE id = 10;");
    }

    #[test]
    fn delete_parent_with_no_dependents_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(&engine, "DELETE FROM users WHERE id = 1;");
    }

    #[test]
    fn delete_parent_with_dependents_restrict_errors() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr) = make_infra(dir.path());
        setup_schema(&catalog, &txn_mgr, None, None);
        let engine = Engine::new(&catalog, &txn_mgr);

        run(&engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');");
        run(
            &engine,
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100);",
        );

        let err = try_run(&engine, "DELETE FROM users WHERE id = 1;").unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Constraint(ConstraintViolation::FkParentViolation { .. })
            ),
            "expected FkParentViolation, got {err:?}"
        );
    }

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

        let rows = select_rows(&engine, "SELECT * FROM orders;");
        assert_eq!(rows.len(), 1, "order row must survive SET NULL");
        assert_eq!(
            rows[0].get(1),
            Some(&Value::Null),
            "user_id must be NULL after SET NULL delete"
        );
    }

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

        let err = try_run(&engine, "UPDATE users SET id = 999 WHERE id = 1;").unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Constraint(ConstraintViolation::FkParentViolation { .. })
            ),
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
        run(&engine, "UPDATE users SET name = 'Alicia' WHERE id = 1;");
    }

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

        let rows = select_rows(&engine, "SELECT * FROM orders;");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(1),
            Some(&Value::Int64(42)),
            "cascade update must propagate new parent key to child"
        );
    }

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

        let rows = select_rows(&engine, "SELECT * FROM orders;");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(1),
            Some(&Value::Null),
            "set-null update must nullify child FK column"
        );
    }

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
        run(&engine, "INSERT INTO orders (id, user_id) VALUES (10, 1);");

        let err =
            try_run(&engine, "INSERT INTO orders (id, user_id) VALUES (20, 99);").unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::Constraint(ConstraintViolation::ForeignKeyViolation { .. })
            ),
            "expected ForeignKeyViolation, got {err:?}"
        );
    }

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
            matches!(
                err,
                EngineError::Constraint(ConstraintViolation::FkParentViolation { .. })
            ),
            "expected FkParentViolation, got {err:?}"
        );
    }
}
