use std::{collections::HashMap, sync::Arc};

use crate::{
    FileId, IndexId, PAGE_SIZE,
    catalog::{
        CatalogError,
        manager::{Catalog, TableInfo},
        systable::{IndexRow, SystemTable},
    },
    index::{AnyIndex, IndexError, IndexKind},
    primitives::ColumnId,
    transaction::Transaction,
};

/// Default head-bucket count for a freshly created hash index.
///
/// Frozen at creation; reopens read this value back from
/// `SystemTable::Indexes`. Sized as a small power of two — enough for the
/// tests and early workloads, but small enough that index files don't
/// dominate disk for tiny tables. Tunable via a `WITH (buckets = N)`
/// clause once the parser supports it.
const DEFAULT_HASH_BUCKETS: u32 = 64;

/// In-memory description of one secondary index.
///
/// This is the merged, validated view of N [`IndexRow`]s that share the same
/// `index_id` — one per indexed column. The catalog reads rows off disk, calls
/// [`IndexInfo::from_rows`] to fold them, and hands the result to the
/// [`crate::index::IndexManager`] which then constructs and registers the live
/// access method.
///
/// Mirrors the role [`TableInfo`] plays for tables: pure metadata, no
/// runtime resources (no `PageStore`, no atomic counters). Cheap to clone.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct IndexInfo {
    /// Catalog-assigned identifier, stable across renames.
    pub index_id: IndexId,
    /// SQL-visible name (`CREATE INDEX <name> …`).
    pub name: String,
    /// `FileId` of the indexed table's heap. Use this to look up the table
    /// in [`Catalog::open_heaps`] when the executor needs to materialize a
    /// row from a `RecordId` returned by the index.
    pub table_id: FileId,
    /// Names of the indexed columns, in declaration order. Length is the
    /// index's arity (1 for single-column, ≥2 for composite).
    pub columns: Vec<String>,
    /// Which access-method family backs this index.
    pub kind: IndexKind,
    /// `FileId` of the index's own pages — distinct from `table_id`. This
    /// is what [`crate::index::hash::HashIndex::new`] needs.
    pub index_file_id: FileId,
    /// Static-hash bucket count. Frozen at creation; only meaningful for
    /// `IndexKind::Hash`. Stored as 0 for B-tree.
    pub num_buckets: u32,
}

// Allowed dead-code: the public DDL path (`CREATE INDEX`) that calls these
// helpers is the next milestone; until then they're exercised only by the
// in-module unit tests below.
#[allow(dead_code)]
impl IndexInfo {
    /// Folds a group of catalog rows for a single index into one
    /// `IndexInfo`.
    ///
    /// **Caller's responsibility**: every row in `rows` must share the same
    /// `index_id`. Group rows by `index_id` first, then call this once per
    /// group. (`from_row_set` further down handles the ungrouped case.)
    ///
    /// # Errors
    ///
    /// - [`CatalogError::Corruption`] if `rows` is empty, if rows disagree on any index-level field
    ///   (`index_name`, `table_id`, `kind`, `index_file_id`, `num_buckets`), if `column_position`s
    ///   aren't a contiguous `0..n` sequence, or if positions duplicate.
    pub(super) fn from_rows(rows: Vec<IndexRow>) -> Result<Self, CatalogError> {
        Self::try_from(rows)
    }

    /// Folds a flat list of unsorted rows (typically the entire contents of
    /// `SystemTable::Indexes`) into one `IndexInfo` per index.
    ///
    /// Rows are grouped by `index_id` and each group is fed through
    /// [`Self::from_rows`]. The output order is unspecified.
    pub(super) fn from_row_set(rows: Vec<IndexRow>) -> Result<Vec<Self>, CatalogError> {
        let mut by_id: HashMap<IndexId, Vec<IndexRow>> = HashMap::new();
        for r in rows {
            by_id.entry(r.index_id).or_default().push(r);
        }
        by_id.into_values().map(Self::from_rows).collect()
    }

    /// Constructs a `CatalogError::systable_corruption` error for the `SystemTable::Indexes`.
    ///
    /// This is used to report catalog corruption specifically associated with the
    /// indexes system table, attaching a descriptive message.
    #[inline]
    fn corruption(message: impl Into<String>) -> CatalogError {
        CatalogError::systable_corruption(SystemTable::Indexes, message)
    }
}

impl TryFrom<Vec<IndexRow>> for IndexInfo {
    type Error = CatalogError;

    fn try_from(rows: Vec<IndexRow>) -> Result<Self, Self::Error> {
        let (index_id, name, table_id, kind, index_file_id, num_buckets) = {
            let first = rows
                .first()
                .ok_or_else(|| Self::corruption("empty row group for an index"))?;

            for r in &rows {
                if r.same_index_metadata_as(first) {
                    continue;
                }
                return Err(Self::corruption(format!(
                    "rows for index_id={} disagree on shared fields",
                    first.index_id.0
                )));
            }

            (
                first.index_id,
                first.index_name.clone(),
                first.table_id,
                first.index_type,
                first.index_file_id,
                first.num_buckets,
            )
        };

        let mut rows_with_positions = rows
            .into_iter()
            .map(|row| (row.column_position, row))
            .collect::<Vec<(ColumnId, IndexRow)>>();

        rows_with_positions.sort_by_key(|(col_id, _)| *col_id);
        for (i, (col_id, _)) in rows_with_positions.iter().enumerate() {
            let expected = ColumnId::try_from(i)
                .map_err(|_| Self::corruption("index has more columns than fit in ColumnId"))?;
            if *col_id != expected {
                return Err(Self::corruption(format!(
                    "index_id={} has gap or duplicate at position {}: found {}",
                    index_id.0,
                    usize::from(expected),
                    usize::from(*col_id)
                )));
            }
        }

        let columns = rows_with_positions
            .into_iter()
            .map(|(_, row)| row.column_name)
            .collect();

        Ok(Self {
            index_id,
            name,
            table_id,
            columns,
            kind,
            index_file_id,
            num_buckets,
        })
    }
}

impl Catalog {
    /// Adds a freshly opened index to the registry, indexed by both its name
    /// and the table (via `table_file_id`) it belongs to.
    ///
    /// Returns the stored `Arc<AnyIndex>` so callers can immediately use the
    /// index without re-looking-it-up.
    ///
    /// # Errors
    ///
    /// - [`IndexError::DuplicateName`] if an index with the same name is already registered. The
    ///   caller should roll back the surrounding DDL transaction.
    pub fn register_index(
        &self,
        name: impl Into<String>,
        table_file_id: FileId,
        index: AnyIndex,
    ) -> Result<Arc<AnyIndex>, IndexError> {
        let name = name.into();
        let index = Arc::new(index);

        let mut by_name = self.indexes_by_name.write();
        if by_name.contains_key(&name) {
            return Err(IndexError::DuplicateName(name));
        }

        let mut by_table = self.open_indexes.write();
        by_name.insert(name, Arc::clone(&index));
        by_table
            .entry(table_file_id)
            .or_default()
            .push(Arc::clone(&index));
        Ok(index)
    }

    /// Removes the index named `name` from the in-memory registry, if it
    /// exists. Returns the dropped `Arc` so the caller can decide what to do
    /// with the underlying file (typically: delete it after the `DROP INDEX`
    /// transaction commits). Returns `None` if no such index was registered.
    ///
    /// Does *not* delete the index's data file or remove its rows from
    /// `SystemTable::Indexes` — the orchestrating `drop_index` does those.
    pub fn unregister_index(&self, name: &str) -> Option<Arc<AnyIndex>> {
        let mut by_name = self.indexes_by_name.write();
        if !by_name.contains_key(name) {
            return None;
        }

        let index = by_name.remove(name)?;
        let mut by_table = self.open_indexes.write();
        for entries in by_table.values_mut() {
            entries.retain(|e| !Arc::ptr_eq(e, &index));
        }
        by_table.retain(|_, v| !v.is_empty());
        Some(index)
    }

    /// Every index currently registered against the table whose heap is
    /// `table_file_id`. Empty `Vec` for tables with no indexes.
    ///
    /// Returns cloned `Arc`s so the read lock is released before the caller
    /// uses them — index operations themselves take buffer-pool locks, and
    /// holding the registry lock across that would invite deadlocks.
    pub fn indexes_for(&self, table_file_id: FileId) -> Vec<Arc<AnyIndex>> {
        self.open_indexes
            .read()
            .get(&table_file_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Checks whether the given `index` is registered as belonging to the table identified by
    /// `table_file_id`.
    ///
    /// Returns `true` if the index registry for `table_file_id` contains an index pointer equal to
    /// `index` (using pointer equality). Returns `false` otherwise.
    ///
    /// # Arguments
    ///
    /// * `table_file_id` - The file ID identifying the table whose index ownership should be
    ///   checked.
    /// * `index` - An `Arc` to the index whose association is being queried.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // `catalog`, `table_file_id`, and `index` must already exist.
    /// let is_owner = catalog.index_belongs_to_table(table_file_id, &index);
    /// ```
    pub fn index_belongs_to_table(&self, table_file_id: FileId, index: &Arc<AnyIndex>) -> bool {
        self.indexes_for(table_file_id)
            .iter()
            .any(|candidate| Arc::ptr_eq(candidate, index))
    }

    /// Resolves an index name to its live instance, if registered.
    pub fn get_index_by_name(&self, name: &str) -> Option<Arc<AnyIndex>> {
        self.indexes_by_name.read().get(name).map(Arc::clone)
    }

    /// Creates a new secondary index and registers it everywhere — system
    /// table, in-memory registry, buffer pool, on-disk file.
    ///
    /// Mirrors [`Self::create_table`] in shape: the engine layer hands over
    /// a fully-resolved request (the binder has already checked name
    /// uniqueness, resolved column indices, etc.), and this method takes
    /// care of allocation, persistence, and access-method initialization.
    ///
    /// Returns the assigned [`IndexId`]. Composite indexes write one
    /// `IndexRow` per column, all sharing the same `index_id`.
    ///
    /// # Errors
    ///
    /// - [`CatalogError::Unsupported`] for `IndexKind::Btree` (not yet implemented).
    /// - [`CatalogError::TableNotFound`] if `table_file_id` has no registered heap.
    /// - [`CatalogError::InvalidCatalogRow`] if a column index is out of bounds.
    /// - Propagates buffer-pool registration, index init, and system-table insert errors.
    pub fn create_index(
        &self,
        txn: &Transaction<'_>,
        index_name: &str,
        table_name: &str,
        table_file_id: FileId,
        column_indices: &[ColumnId],
        kind: IndexKind,
    ) -> Result<IndexId, CatalogError> {
        let TableInfo { schema, .. } = self.get_table_info(txn, table_name)?;

        let mut column_names = Vec::with_capacity(column_indices.len());
        let mut key_types = Vec::with_capacity(column_indices.len());

        for &col_id in column_indices {
            let i = usize::from(col_id);
            let field = schema.field(i).ok_or_else(|| {
                CatalogError::invalid_catalog_row(format!(
                    "index column index {i} out of bounds for table '{table_name}'"
                ))
            })?;
            column_names.push(field.name.clone());
            key_types.push(field.field_type);
        }

        let index_id = self.next_index_id();
        let index_file_id = self.next_file_id();
        let num_buckets = DEFAULT_HASH_BUCKETS;
        self.create_index_file(index_file_id, index_name, num_buckets)?;

        let any = AnyIndex::create(
            kind,
            index_file_id,
            key_types,
            num_buckets,
            self.buffer_pool().clone(),
            num_buckets,
        )?;
        any.init(txn.transaction_id())?;

        for (position, column_name) in column_names.iter().enumerate() {
            let pos = ColumnId::try_from(position).map_err(|_| {
                CatalogError::invalid_catalog_row("index column position overflows ColumnId")
            })?;
            let row = IndexRow::new(
                index_id,
                index_name.to_string(),
                table_file_id,
                column_name.clone(),
                pos,
                kind,
                index_file_id,
                num_buckets,
            )?;
            self.insert_systable_tuple(txn, &row)?;
        }

        self.register_index(index_name, table_file_id, any)?;
        Ok(index_id)
    }

    /// Allocates the on-disk file for a fresh index and registers it with the buffer pool.
    ///
    /// The file is sized to hold the head pages up front so [`HashIndex::init`]
    /// can stamp them inside the transaction.
    fn create_index_file(
        &self,
        index_file_id: FileId,
        index_name: &str,
        num_buckets: u32,
    ) -> Result<(), CatalogError> {
        let file_path = self.index_file_path(index_name);
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path)?;
        f.set_len(u64::from(num_buckets) * PAGE_SIZE as u64)?;
        drop(f);

        self.buffer_pool()
            .register_file(index_file_id, &file_path)
            .map_err(|_| {
                CatalogError::Io(std::io::Error::other("failed to register index file"))
            })?;
        Ok(())
    }

    /// Tears down a registered index: removes its rows from the system
    /// table, evicts the in-memory entry, and deletes the backing file.
    ///
    /// Counterpart to [`Self::create_index`]. Idempotency lives at the
    /// binder layer (`DROP INDEX IF EXISTS` is decided there); reaching
    /// this method means the caller intends to actually drop something.
    ///
    /// # Errors
    ///
    /// - [`CatalogError::IndexNameNotFound`] if no `IndexRow` matches `index_name`.
    /// - Propagates system-table delete and filesystem errors.
    pub fn drop_index(&self, txn: &Transaction<'_>, index_name: &str) -> Result<(), CatalogError> {
        let rows =
            self.scan_system_table_where::<IndexRow, _>(txn, |r| r.index_name == index_name)?;
        let index_file_id = rows
            .first()
            .map(|r| r.index_file_id)
            .ok_or_else(|| CatalogError::IndexNameNotFound(index_name.to_string()))?;

        self.delete_systable_rows::<IndexRow, _>(txn, |r| r.index_name == index_name)?;
        self.unregister_index(index_name);

        let _ = self.buffer_pool().unregister_file(index_file_id);
        let path = self.index_file_path(index_name);
        if path.exists() {
            std::fs::remove_file(path)?;
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
        PAGE_SIZE, TransactionId, Type, Value,
        buffer_pool::page_store::PageStore,
        index::{CompositeKey, hash::HashIndex},
        primitives::{PageNumber, RecordId, SlotId},
        wal::writer::Wal,
    };

    fn make_infra(dir: &Path) -> (Arc<Wal>, Arc<PageStore>) {
        let wal = Arc::new(Wal::new(&dir.join("wal.log"), 0).expect("failed to create WAL"));
        let bp = Arc::new(PageStore::new(64, wal.clone()));
        (wal, bp)
    }

    fn idx_row(index_id: i64, index_name: &str, column_name: &str, position: u32) -> IndexRow {
        IndexRow::new(
            IndexId::new(index_id),
            index_name.to_string(),
            FileId::new(7),
            column_name.to_string(),
            ColumnId::try_from(position).unwrap(),
            IndexKind::Hash,
            FileId::new(42),
            64,
        )
        .unwrap()
    }

    #[test]
    fn index_info_from_rows_single_column() {
        let info = IndexInfo::from_rows(vec![idx_row(1, "users_email_idx", "email", 0)]).unwrap();
        assert_eq!(info.index_id, IndexId::new(1));
        assert_eq!(info.name, "users_email_idx");
        assert_eq!(info.columns, vec!["email".to_string()]);
        assert_eq!(info.kind, IndexKind::Hash);
        assert_eq!(info.num_buckets, 64);
    }

    #[test]
    fn index_info_from_rows_composite_orders_by_position() {
        let info = IndexInfo::from_rows(vec![
            idx_row(2, "users_name_idx", "first_name", 1),
            idx_row(2, "users_name_idx", "last_name", 0),
        ])
        .unwrap();
        assert_eq!(info.columns, vec!["last_name", "first_name"]);
    }

    #[test]
    fn index_info_from_rows_rejects_empty_group() {
        let err = IndexInfo::from_rows(vec![]).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn index_info_from_rows_rejects_disagreeing_table_id() {
        let row_a = idx_row(1, "idx", "a", 0);
        let mut row_b = idx_row(1, "idx", "b", 1);
        row_b.table_id = FileId::new(99);
        let err = IndexInfo::from_rows(vec![row_a, row_b]).unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn index_info_from_rows_rejects_position_gap() {
        let err = IndexInfo::from_rows(vec![idx_row(1, "idx", "a", 0), idx_row(1, "idx", "c", 2)])
            .unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn index_info_from_rows_rejects_duplicate_position() {
        let err = IndexInfo::from_rows(vec![idx_row(1, "idx", "a", 0), idx_row(1, "idx", "b", 0)])
            .unwrap_err();
        assert!(matches!(err, CatalogError::Corruption { .. }));
    }

    #[test]
    fn index_info_from_row_set_groups_by_index_id() {
        let rows = vec![
            idx_row(1, "idx_a", "x", 0),
            idx_row(2, "idx_b", "y", 0),
            idx_row(2, "idx_b", "z", 1),
        ];
        let mut infos = IndexInfo::from_row_set(rows).unwrap();
        infos.sort_by_key(|i| i.index_id);
        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].name, "idx_a");
        assert_eq!(infos[0].columns, vec!["x"]);
        assert_eq!(infos[1].name, "idx_b");
        assert_eq!(infos[1].columns, vec!["y", "z"]);
    }

    fn fresh_hash(
        catalog: &Catalog,
        bp: &Arc<PageStore>,
        wal: &Wal,
        dir: &Path,
        num_buckets: u32,
        key_types: Vec<Type>,
    ) -> HashIndex {
        let file_id = catalog.next_file_id();
        let path = dir.join(format!("idx_{}.dat", file_id.0));

        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(u64::from(num_buckets) * PAGE_SIZE as u64)
            .unwrap();
        drop(f);
        bp.register_file(file_id, &path).unwrap();

        let idx = HashIndex::new(file_id, key_types, num_buckets, Arc::clone(bp), num_buckets);
        let init_txn = TransactionId::new(file_id.0);
        wal.log_begin(init_txn).unwrap();
        idx.init(init_txn).unwrap();
        bp.release_all(init_txn);
        idx
    }

    #[test]
    fn empty_catalog_has_no_indexes() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();
        assert!(catalog.indexes_for(FileId::new(42)).is_empty());
        assert!(catalog.get_index_by_name("missing").is_none());
    }

    #[test]
    fn register_index_then_lookup_by_name_and_by_table() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();

        let table_file_id = FileId::new(7);
        let hash = fresh_hash(&catalog, &bp, &wal, dir.path(), 4, vec![Type::Int32]);
        let arc = catalog
            .register_index("users_id_idx", table_file_id, hash.into())
            .unwrap();

        let by_name = catalog.get_index_by_name("users_id_idx").unwrap();
        assert!(Arc::ptr_eq(&arc, &by_name));

        let table_indexes = catalog.indexes_for(table_file_id);
        assert_eq!(table_indexes.len(), 1);
        assert!(Arc::ptr_eq(&arc, &table_indexes[0]));
    }

    #[test]
    fn register_index_duplicate_name_rejected() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();
        let table_file_id = FileId::new(7);

        let h1 = fresh_hash(&catalog, &bp, &wal, dir.path(), 4, vec![Type::Int32]);
        let h2 = fresh_hash(&catalog, &bp, &wal, dir.path(), 4, vec![Type::Int32]);
        catalog
            .register_index("dup", table_file_id, h1.into())
            .unwrap();
        let err = catalog
            .register_index("dup", table_file_id, h2.into())
            .unwrap_err();
        match err {
            crate::index::IndexError::DuplicateName(name) => assert_eq!(name, "dup"),
            other => panic!("expected DuplicateName, got {other:?}"),
        }
        assert!(catalog.get_index_by_name("dup").is_some());
        assert_eq!(catalog.indexes_for(table_file_id).len(), 1);
    }

    #[test]
    fn unregister_index_removes_from_both_views() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();
        let table_file_id = FileId::new(7);

        let hash = fresh_hash(&catalog, &bp, &wal, dir.path(), 4, vec![Type::Int32]);
        catalog
            .register_index("idx", table_file_id, hash.into())
            .unwrap();

        let dropped = catalog.unregister_index("idx").unwrap();
        assert!(catalog.get_index_by_name("idx").is_none());
        assert!(catalog.indexes_for(table_file_id).is_empty());
        assert!(Arc::strong_count(&dropped) >= 1);
    }

    #[test]
    fn unregister_index_missing_returns_none() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();
        assert!(catalog.unregister_index("never-registered").is_none());
    }

    #[test]
    fn indexes_for_groups_multiple_indexes_on_same_table() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();
        let table_file_id = FileId::new(7);

        let h1 = fresh_hash(&catalog, &bp, &wal, dir.path(), 4, vec![Type::Int32]);
        let h2 = fresh_hash(&catalog, &bp, &wal, dir.path(), 4, vec![Type::String]);
        catalog
            .register_index("by_id", table_file_id, h1.into())
            .unwrap();
        catalog
            .register_index("by_email", table_file_id, h2.into())
            .unwrap();

        assert_eq!(catalog.indexes_for(table_file_id).len(), 2);
    }

    #[test]
    fn indexes_for_isolates_different_tables() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();
        let table_a = FileId::new(7);
        let table_b = FileId::new(8);

        let ha = fresh_hash(&catalog, &bp, &wal, dir.path(), 4, vec![Type::Int32]);
        let hb = fresh_hash(&catalog, &bp, &wal, dir.path(), 4, vec![Type::Int32]);
        catalog.register_index("a_idx", table_a, ha.into()).unwrap();
        catalog.register_index("b_idx", table_b, hb.into()).unwrap();

        assert_eq!(catalog.indexes_for(table_a).len(), 1);
        assert_eq!(catalog.indexes_for(table_b).len(), 1);
        assert!(!Arc::ptr_eq(
            &catalog.indexes_for(table_a)[0],
            &catalog.indexes_for(table_b)[0]
        ));
    }

    #[test]
    fn registered_index_actually_works_end_to_end() {
        let dir = tempdir().unwrap();
        let (wal, bp) = make_infra(dir.path());
        let catalog = Catalog::initialize(&bp, &wal, dir.path()).unwrap();
        let table_file_id = FileId::new(7);

        let hash = fresh_hash(&catalog, &bp, &wal, dir.path(), 4, vec![Type::Int32]);
        catalog
            .register_index("idx", table_file_id, hash.into())
            .unwrap();

        let txn = TransactionId::new(1000);
        wal.log_begin(txn).unwrap();

        let live = catalog.get_index_by_name("idx").unwrap();
        let key = CompositeKey::single(Value::Int32(7));
        let r = RecordId::new(FileId::new(1), PageNumber::new(0), SlotId(0));
        live.insert(txn, &key, r).unwrap();
        assert_eq!(live.search(txn, &key).unwrap(), vec![r]);
    }
}
