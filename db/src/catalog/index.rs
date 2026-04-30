use std::{collections::HashMap, sync::Arc};

use crate::{
    FileId, IndexId, PAGE_SIZE, TransactionId, Type,
    catalog::{
        CatalogError,
        manager::{Catalog, TableInfo},
        systable::{ColumnRow, IndexRow, PrimaryKeyColumnRow, SystemTable, TableRow},
    },
    index::{AnyIndex, CompositeKey, IndexError, IndexKind},
    primitives::{ColumnId, RecordId},
    transaction::Transaction,
    tuple::{Field, Tuple, TupleSchema},
};

/// In-memory view of a live, registered index used by DML.
///
/// Pairs the access method with the **table-column projection** the index
/// covers. `AnyIndex` knows the per-column declared types (`key_types()`)
/// but not which columns of the *table tuple* feed those types — that's
/// `table_columns`. The DML hot path needs both: the projection to pluck
/// the right values out of a `Tuple`, and the access method to insert /
/// delete the resulting `CompositeKey`.
///
/// Stored in the catalog registry under `Arc<LiveIndex>` so cloning is
/// cheap and the lock can be released before any index work runs.
#[derive(Debug)]
pub struct LiveIndex {
    /// The live access method. Call `access.insert` / `access.delete` /
    /// `access.search` from DML.
    pub access: AnyIndex,
    /// Indices into the **table** schema, in the order this index was
    /// declared. To build the index key from a `Tuple`, project these
    /// columns in order: `tuple.get(table_columns[0])`, etc.
    ///
    /// Length matches `access.key_types().len()`.
    pub table_columns: Vec<ColumnId>,
}

impl LiveIndex {
    /// Constructs a `CompositeKey` from a table `Tuple` by projecting the columns
    /// this index covers, in declaration order.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidCatalogRow`] if any entry in
    /// `table_columns` is not a valid index in the tuple, which indicates
    /// corrupt catalog metadata or invalid index registration.
    pub fn create_index_key(&self, tuple: &Tuple) -> Result<CompositeKey, CatalogError> {
        let mut values = Vec::with_capacity(self.table_columns.len());
        for &col_id in &self.table_columns {
            let i = usize::from(col_id);
            let value = tuple.get(i).cloned().ok_or_else(|| {
                CatalogError::invalid_catalog_row(format!(
                    "index column {i} out of bounds for tuple with {} fields",
                    tuple.len()
                ))
            })?;
            values.push(value);
        }
        Ok(CompositeKey::new(values))
    }

    pub fn insert(
        &self,
        txn: TransactionId,
        tuple: &Tuple,
        rid: RecordId,
    ) -> Result<(), CatalogError> {
        let key = self.create_index_key(tuple)?;
        self.access.insert(txn, &key, rid)?;
        Ok(())
    }

    /// Removes the `(key, rid)` pair for `tuple` from the index.
    ///
    /// `tuple` must be the **pre-deletion** row state — that's the tuple
    /// whose projection produced the key currently stored in the index.
    /// DML callers therefore have to pass the old tuple before calling
    /// `heap.delete_tuple`, or carry it forward from a prior scan.
    pub fn delete(
        &self,
        txn: TransactionId,
        tuple: &Tuple,
        rid: RecordId,
    ) -> Result<(), CatalogError> {
        let key = self.create_index_key(tuple)?;
        self.access.delete(txn, &key, rid)?;
        Ok(())
    }
}

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
    /// Adds a freshly opened index to the registry under both its name and
    /// the table (via `table_file_id`) it belongs to.
    ///
    /// `table_columns` is the projection into the table schema — the same
    /// `Vec<ColumnId>` the binder resolved from the `CREATE INDEX (...)`
    /// column list. It's stored alongside the access method so DML can
    /// build keys from tuples without re-reading the system table.
    ///
    /// Returns the stored `Arc<LiveIndex>` so callers can immediately use
    /// the index without re-looking-it-up.
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
        table_columns: Vec<ColumnId>,
    ) -> Result<Arc<LiveIndex>, IndexError> {
        let name = name.into();
        let live = Arc::new(LiveIndex {
            access: index,
            table_columns,
        });

        let mut by_name = self.indexes_by_name.write();
        if by_name.contains_key(&name) {
            return Err(IndexError::DuplicateName(name));
        }

        let mut by_table = self.open_indexes.write();
        by_name.insert(name, Arc::clone(&live));
        by_table
            .entry(table_file_id)
            .or_default()
            .push(Arc::clone(&live));
        Ok(live)
    }

    /// Removes the index named `name` from the in-memory registry, if it
    /// exists. Returns the dropped `Arc` so the caller can decide what to do
    /// with the underlying file (typically: delete it after the `DROP INDEX`
    /// transaction commits). Returns `None` if no such index was registered.
    ///
    /// Does *not* delete the index's data file or remove its rows from
    /// `SystemTable::Indexes` — the orchestrating `drop_index` does those.
    pub fn unregister_index(&self, name: &str) -> Option<Arc<LiveIndex>> {
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
    pub fn indexes_for(&self, table_file_id: FileId) -> Vec<Arc<LiveIndex>> {
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
    pub fn index_belongs_to_table(&self, table_file_id: FileId, index: &Arc<LiveIndex>) -> bool {
        self.indexes_for(table_file_id)
            .iter()
            .any(|candidate| Arc::ptr_eq(candidate, index))
    }

    /// Resolves an index name to its live instance, if registered.
    pub fn get_index_by_name(&self, name: &str) -> Option<Arc<LiveIndex>> {
        self.indexes_by_name.read().get(name).map(Arc::clone)
    }

    /// Returns the user-facing table name registered for `file_id`, if any.
    ///
    /// Reads from the in-memory `user_tables` cache, which is fully
    /// populated by `replay_user_objects` at startup. Used by SHOW
    /// INDEXES (and any future tooling) to render `FileId`s back into
    /// names without re-scanning the system table.
    pub fn table_name_by_id(&self, file_id: FileId) -> Option<String> {
        self.user_tables
            .read()
            .iter()
            .find_map(|(name, info)| (info.file_id == file_id).then(|| name.clone()))
    }

    /// Returns one [`IndexInfo`] per registered index, sorted by name.
    ///
    /// Used by `SHOW INDEXES` (no `FROM` clause). Folds raw `IndexRow`
    /// records into the merged view per index.
    pub fn list_indexes(&self, txn: &Transaction<'_>) -> Result<Vec<IndexInfo>, CatalogError> {
        let rows = self.scan_system_table::<IndexRow>(txn)?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut infos = IndexInfo::from_row_set(rows)?;
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(infos)
    }

    /// Returns the indexes attached to `file_id`, sorted by name.
    ///
    /// Counterpart of [`Self::list_indexes`] for `SHOW INDEXES FROM <table>`.
    pub fn list_indexes_for(
        &self,
        txn: &Transaction<'_>,
        file_id: FileId,
    ) -> Result<Vec<IndexInfo>, CatalogError> {
        let rows = self.scan_system_table_where::<IndexRow, _>(txn, |r| r.table_id == file_id)?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut infos = IndexInfo::from_row_set(rows)?;
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(infos)
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
        let (column_names, key_types) =
            Self::resolve_index_key_columns(&schema, table_name, column_indices)?;

        let (index_id, index_file_id, num_buckets) = (
            self.next_index_id(),
            self.next_file_id(),
            DEFAULT_HASH_BUCKETS,
        );
        self.create_index_file(index_file_id, index_name, num_buckets)?;
        let index = match kind {
            IndexKind::Hash => AnyIndex::hash()
                .file_id(index_file_id)
                .key_types(key_types)
                .num_buckets(num_buckets)
                .store(self.buffer_pool().clone())
                .existing_pages(num_buckets)
                .build()?,
            IndexKind::Btree => AnyIndex::btree()
                .file_id(index_file_id)
                .key_types(key_types)
                .store(self.buffer_pool().clone())
                .existing_pages(num_buckets)
                .build()?,
        };
        index.init(txn.transaction_id())?;

        for (position, column_name) in column_names.iter().enumerate() {
            let pos = ColumnId::try_from(position).map_err(|_| {
                CatalogError::invalid_catalog_row("index column position overflows ColumnId")
            })?;
            let row = match kind {
                IndexKind::Hash => IndexRow::hash(
                    index_id,
                    index_name,
                    table_file_id,
                    column_name.clone(),
                    pos,
                    index_file_id,
                    num_buckets,
                )?,
                IndexKind::Btree => IndexRow::btree(
                    index_id,
                    index_name,
                    table_file_id,
                    column_name.clone(),
                    pos,
                    index_file_id,
                )?,
            };
            self.insert_systable_tuple(txn, &row)?;
        }

        self.register_index(index_name, table_file_id, index, column_indices.to_vec())?;
        Ok(index_id)
    }

    /// Resolves a `CREATE INDEX` column list into persisted column names and key types.
    ///
    /// The binder hands `create_index` a list of table-column offsets (`column_indices`) in
    /// declaration order. For index creation we need to:
    ///
    /// - Persist the referenced column **names** into the `Indexes` system table (so index replay
    ///   can resolve them back against `Columns`)
    /// - Feed the corresponding declared [`Type`]s into [`AnyIndex::create`]
    ///
    /// Returns `(column_names, key_types)` with matching lengths.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidCatalogRow`] if any `ColumnId` is out of bounds for `schema`.
    fn resolve_index_key_columns(
        schema: &TupleSchema,
        table_name: &str,
        column_indices: &[ColumnId],
    ) -> Result<(Vec<String>, Vec<Type>), CatalogError> {
        column_indices
            .iter()
            .map(|&col_id| {
                let i = usize::from(col_id);
                let Field {
                    name, field_type, ..
                } = schema.field(i).ok_or_else(|| {
                    CatalogError::invalid_catalog_row(format!(
                        "index column index {i} out of bounds for table '{table_name}'"
                    ))
                })?;
                Ok((name.clone(), field_type))
            })
            .collect()
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
        let index_file_id = self
            .scan_system_table_where::<IndexRow, _>(txn, |r| r.index_name == index_name)?
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

    /// Replays on-disk catalog state into in-memory caches and ID counters.
    ///
    /// Called once at the end of [`Catalog::initialize`] so a freshly opened
    /// catalog reflects whatever the previous process committed. The work
    /// breaks into two pieces:
    ///
    /// 1. **Counter recovery.** Scan `Tables` and `Indexes` for every `FileId` and `IndexId`
    ///    already in use, then bump `next_file_id` / `next_index_id` past them. Without this step
    ///    `create_table` / `create_index` after a restart would re-allocate IDs that already exist
    ///    on disk and silently corrupt state.
    ///
    /// 2. **Index registry rebuild.** Group `IndexRow`s by `index_id`, resolve each index's column
    ///    names against `Columns` to recover table-column offsets and types, open the index file,
    ///    build the live access method, and register it under the same name DML callers will use.
    ///
    /// The user-table cache (`user_tables`) is **not** populated here —
    /// `get_table_info` still loads tables lazily on first lookup. Indexes
    /// don't have a lazy path, so they have to be replayed eagerly.
    pub(super) fn replay_user_objects(&self) -> Result<(), CatalogError> {
        let tid = TransactionId::new(1);

        let tables = self.scan_system_table_with_tid::<TableRow>(tid)?;
        let columns = self.scan_system_table_with_tid::<ColumnRow>(tid)?;
        let pk_columns = self.scan_system_table_with_tid::<PrimaryKeyColumnRow>(tid)?;
        let index_rows = self.scan_system_table_with_tid::<IndexRow>(tid)?;

        let max_file_id: u64 = {
            tables
                .iter()
                .map(|t| u64::from(t.table_id))
                .chain(index_rows.iter().map(|i| u64::from(i.index_file_id)))
                .max()
                .unwrap_or(0)
        };
        self.bump_next_file_id_past(max_file_id);

        let max_index_id = index_rows
            .iter()
            .map(|r| i64::from(r.index_id))
            .max()
            .unwrap_or(0);
        self.bump_next_index_id_past(max_index_id);

        // table_id → (column_name → (offset, type)). Built once before
        // table_replay consumes `columns` so the index lookup pass below can
        // use it without re-scanning. Required for index replay; cheap for
        // table replay.
        let mut col_lookup: HashMap<FileId, HashMap<String, (ColumnId, Type)>> = HashMap::new();
        for c in &columns {
            col_lookup
                .entry(c.table_id)
                .or_default()
                .insert(c.column_name.clone(), (c.position, c.column_type));
        }

        // Eagerly populate user_tables / open_heaps. Without this,
        // table_exists returns false after restart for committed tables,
        // which lets `CREATE TABLE foo` silently duplicate an existing one.
        self.replay_tables(tables, columns, pk_columns)?;
        if index_rows.is_empty() {
            return Ok(());
        }

        let infos = IndexInfo::from_row_set(index_rows)?;
        for info in infos {
            let cols = col_lookup.get(&info.table_id).ok_or_else(|| {
                CatalogError::systable_corruption(
                    SystemTable::Indexes,
                    format!(
                        "index '{}' references table_id {} with no Columns rows",
                        info.name, info.table_id
                    ),
                )
            })?;
            let (table_columns, key_types) = Self::resolve_index_columns(&info, cols)?;
            self.replay_one_index(&info, &table_columns, &key_types)?;
        }
        Ok(())
    }

    /// Reconstructs a single index from disk and registers it.
    ///
    /// Steps for one index:
    /// - Resolve every column name in [`IndexInfo::columns`] against the table's `Columns` rows to
    ///   recover `(ColumnId, Type)` pairs.
    /// - Register the index file with the buffer pool, sized from the file's actual length on disk
    ///   so [`AnyIndex::create`] knows about any overflow pages allocated previously.
    /// - Build the access method *without* calling `init` — the head pages were stamped at creation
    ///   time and re-initializing would clobber data.
    fn replay_one_index(
        &self,
        info: &IndexInfo,
        table_columns: &[ColumnId],
        key_types: &[Type],
    ) -> Result<(), CatalogError> {
        let path = self.index_file_path(&info.name);
        let bytes = std::fs::metadata(&path)?.len();
        let pages = u32::try_from(bytes.div_ceil(PAGE_SIZE as u64))
            .map_err(|_| CatalogError::FileTooLarge)?;

        self.buffer_pool()
            .register_file(info.index_file_id, &path)
            .map_err(|_| {
                CatalogError::Io(std::io::Error::other(
                    "failed to register index file on replay",
                ))
            })?;

        let index = match info.kind {
            IndexKind::Hash => AnyIndex::hash()
                .file_id(info.index_file_id)
                .key_types(key_types.to_vec())
                .num_buckets(info.num_buckets)
                .store(self.buffer_pool().clone())
                .existing_pages(pages)
                .build()?,
            IndexKind::Btree => AnyIndex::btree()
                .file_id(info.index_file_id)
                .key_types(key_types.to_vec())
                .store(self.buffer_pool().clone())
                .existing_pages(pages)
                .build()?,
        };

        self.register_index(
            info.name.clone(),
            info.table_id,
            index,
            table_columns.to_vec(),
        )?;
        Ok(())
    }

    /// Resolves an index's stored column-name list into table-column IDs and key types.
    ///
    /// During index replay, [`IndexInfo`] carries the index definition as persisted in the
    /// `Indexes` system table: a list of column **names** in declaration order. To rebuild a live
    /// access method we need:
    ///
    /// - `table_columns`: the resolved [`ColumnId`]s into the table schema (used as a projection
    ///   when forming index keys from tuples)
    /// - `key_types`: the corresponding declared [`Type`]s (fed into [`AnyIndex::create`])
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Corruption`] if any referenced column name is missing from `cols`,
    /// which indicates a corrupted `Indexes`/`Columns` system-table relationship.
    fn resolve_index_columns(
        info: &IndexInfo,
        cols: &HashMap<String, (ColumnId, Type)>,
    ) -> Result<(Vec<ColumnId>, Vec<Type>), CatalogError> {
        info.columns
            .iter()
            .map(|col_name| {
                let (cid, ty) = cols.get(col_name).ok_or_else(|| {
                    CatalogError::systable_corruption(
                        SystemTable::Indexes,
                        format!(
                            "index '{}' references unknown column '{}' in table_id {}",
                            info.name, col_name, info.table_id
                        ),
                    )
                })?;

                Ok((*cid, *ty))
            })
            .collect()
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
        IndexRow::hash(
            IndexId::new(index_id),
            index_name,
            FileId::new(7),
            column_name,
            ColumnId::try_from(position).unwrap(),
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
            .register_index("users_id_idx", table_file_id, hash.into(), vec![
                ColumnId::try_from(0usize).unwrap(),
            ])
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
        let cols = vec![ColumnId::try_from(0usize).unwrap()];
        catalog
            .register_index("dup", table_file_id, h1.into(), cols.clone())
            .unwrap();
        let err = catalog
            .register_index("dup", table_file_id, h2.into(), cols)
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
            .register_index("idx", table_file_id, hash.into(), vec![
                ColumnId::try_from(0usize).unwrap(),
            ])
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
        let cols = vec![ColumnId::try_from(0usize).unwrap()];
        catalog
            .register_index("by_id", table_file_id, h1.into(), cols.clone())
            .unwrap();
        catalog
            .register_index("by_email", table_file_id, h2.into(), cols)
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
        let cols = vec![ColumnId::try_from(0usize).unwrap()];
        catalog
            .register_index("a_idx", table_a, ha.into(), cols.clone())
            .unwrap();
        catalog
            .register_index("b_idx", table_b, hb.into(), cols)
            .unwrap();

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
            .register_index("idx", table_file_id, hash.into(), vec![
                ColumnId::try_from(0usize).unwrap(),
            ])
            .unwrap();

        let txn = TransactionId::new(1000);
        wal.log_begin(txn).unwrap();

        let live = catalog.get_index_by_name("idx").unwrap();
        let key = CompositeKey::single(Value::Int32(7));
        let r = RecordId::new(FileId::new(1), PageNumber::new(0), SlotId(0));
        live.access.insert(txn, &key, r).unwrap();
        assert_eq!(live.access.search(txn, &key).unwrap(), vec![r]);
    }

    // ── Catalog::create_index / drop_index orchestrators ──────────────────

    use crate::{
        transaction::TransactionManager,
        tuple::{Field, TupleSchema},
    };

    fn col_id(idx: usize) -> ColumnId {
        ColumnId::try_from(idx).unwrap()
    }

    fn make_full_infra(dir: &Path) -> (Catalog, TransactionManager, Arc<PageStore>, Arc<Wal>) {
        let (wal, bp) = make_infra(dir);
        let catalog = Catalog::initialize(&bp, &wal, dir).expect("catalog init failed");
        let txn_mgr = TransactionManager::new(Arc::clone(&wal), Arc::clone(&bp));
        (catalog, txn_mgr, bp, wal)
    }

    /// Two-column user table — created inside its own transaction so each
    /// caller can start a fresh txn for the index work that follows.
    fn make_users_table(catalog: &Catalog, txn_mgr: &TransactionManager) -> FileId {
        let schema = TupleSchema::new(vec![
            Field::new("id", Type::Int64).not_null(),
            Field::new("email", Type::String).not_null(),
        ]);
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog.create_table(&txn, "users", schema, None).unwrap();
        txn.commit().unwrap();
        file_id
    }

    /// Three-column variant for composite-index tests.
    fn make_three_col_table(catalog: &Catalog, txn_mgr: &TransactionManager) -> FileId {
        let schema = TupleSchema::new(vec![
            Field::new("a", Type::Int64).not_null(),
            Field::new("b", Type::Int64).not_null(),
            Field::new("c", Type::Int64).not_null(),
        ]);
        let txn = txn_mgr.begin().unwrap();
        let file_id = catalog.create_table(&txn, "t", schema, None).unwrap();
        txn.commit().unwrap();
        file_id
    }

    // create_index registers the index in both lookup views, writes a row to
    // SystemTable::Indexes, and lays down the backing file.
    #[test]
    fn create_index_basic_succeeds() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());
        let table_file_id = make_users_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let id = catalog
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

        assert_ne!(id, IndexId::default(), "index_id should be allocated");

        assert!(
            catalog.get_index_by_name("users_email_idx").is_some(),
            "registry should contain the new index by name"
        );
        assert_eq!(
            catalog.indexes_for(table_file_id).len(),
            1,
            "registry should contain the new index by table"
        );

        let path = dir.path().join("idx_users_email_idx.dat");
        assert!(path.exists(), "index file should exist on disk");

        let txn2 = txn_mgr.begin().unwrap();
        let rows = catalog
            .scan_system_table_where::<IndexRow, _>(&txn2, |r| r.index_name == "users_email_idx")
            .unwrap();
        txn2.commit().unwrap();
        assert_eq!(
            rows.len(),
            1,
            "exactly one IndexRow per single-column index"
        );
        assert_eq!(rows[0].column_name, "email");
        assert_eq!(rows[0].column_position, col_id(0));
        assert_eq!(rows[0].table_id, table_file_id);
        assert_eq!(rows[0].index_type, IndexKind::Hash);
        assert_eq!(rows[0].num_buckets, DEFAULT_HASH_BUCKETS);
    }

    // Composite indexes write one IndexRow per column, all sharing index_id.
    #[test]
    fn create_index_composite_writes_one_row_per_column() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());
        let table_file_id = make_three_col_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let id = catalog
            .create_index(
                &txn,
                "t_ca_idx",
                "t",
                table_file_id,
                // (c, a) — declaration order matters for composite key layout.
                &[col_id(2), col_id(0)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        let mut rows = catalog
            .scan_system_table_where::<IndexRow, _>(&txn2, |r| r.index_id == id)
            .unwrap();
        txn2.commit().unwrap();
        rows.sort_by_key(|r| r.column_position);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].column_position, col_id(0));
        assert_eq!(rows[0].column_name, "c");
        assert_eq!(rows[1].column_position, col_id(1));
        assert_eq!(rows[1].column_name, "a");
        for r in &rows {
            assert_eq!(r.index_id, id);
            assert_eq!(r.index_name, "t_ca_idx");
            assert_eq!(r.table_id, table_file_id);
        }
    }

    #[test]
    fn create_index_btree_succeeds_and_registers_index() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());
        let table_file_id = make_users_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let id = catalog
            .create_index(
                &txn,
                "users_id_btree",
                "users",
                table_file_id,
                &[col_id(0)],
                IndexKind::Btree,
            )
            .unwrap();
        let rows = catalog
            .scan_system_table_where::<IndexRow, _>(&txn, |r| r.index_name == "users_id_btree")
            .unwrap();
        txn.commit().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].index_id, id);
        assert!(catalog.get_index_by_name("users_id_btree").is_some());
    }

    // The function looks up TableInfo by name to resolve column types/names,
    // so a bogus name surfaces as TableNotFound.
    #[test]
    fn create_index_unknown_table_returns_table_not_found() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let err = catalog
            .create_index(
                &txn,
                "ix",
                "ghost",
                FileId::new(999),
                &[col_id(0)],
                IndexKind::Hash,
            )
            .unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, CatalogError::TableNotFound { ref table_name } if table_name == "ghost"),
            "unexpected error: {err:?}"
        );
    }

    // A column index that doesn't exist in the table schema is rejected with
    // InvalidCatalogRow. The binder should normally catch this first, but the
    // catalog defends in depth.
    #[test]
    fn create_index_column_oob_returns_invalid_row() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());
        let table_file_id = make_users_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        let err = catalog
            .create_index(
                &txn,
                "ix",
                "users",
                table_file_id,
                &[col_id(99)],
                IndexKind::Hash,
            )
            .unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, CatalogError::InvalidCatalogRow { .. }),
            "unexpected error: {err:?}"
        );
    }

    // drop_index removes the file, the registry entry, and every IndexRow
    // for the named index.
    #[test]
    fn drop_index_existing_removes_file_rows_and_registry() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());
        let table_file_id = make_users_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
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

        let path = dir.path().join("idx_users_email_idx.dat");
        assert!(path.exists());

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_index(&txn2, "users_email_idx").unwrap();
        txn2.commit().unwrap();

        assert!(!path.exists(), "index file should be removed");
        assert!(catalog.get_index_by_name("users_email_idx").is_none());
        assert!(catalog.indexes_for(table_file_id).is_empty());

        let txn3 = txn_mgr.begin().unwrap();
        let rows = catalog
            .scan_system_table_where::<IndexRow, _>(&txn3, |r| r.index_name == "users_email_idx")
            .unwrap();
        txn3.commit().unwrap();
        assert!(rows.is_empty(), "all IndexRows should be deleted");
    }

    // drop_index against a name that was never registered errors as
    // IndexNameNotFound — the binder normally short-circuits this for
    // IF EXISTS, but the catalog itself is non-idempotent.
    #[test]
    fn drop_index_missing_returns_index_name_not_found() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());

        let txn = txn_mgr.begin().unwrap();
        let err = catalog.drop_index(&txn, "ghost_idx").unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, CatalogError::IndexNameNotFound(ref n) if n == "ghost_idx"),
            "unexpected error: {err:?}"
        );
    }

    // drop_table cascades: every index on the dropped table is torn down too,
    // including its file on disk, its registry entry, and its IndexRows.
    #[test]
    fn drop_table_cascades_to_all_indexes_on_table() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());
        let table_file_id = make_users_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_index(
                &txn,
                "users_id_idx",
                "users",
                table_file_id,
                &[col_id(0)],
                IndexKind::Hash,
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

        let p1 = dir.path().join("idx_users_id_idx.dat");
        let p2 = dir.path().join("idx_users_email_idx.dat");
        assert!(p1.exists() && p2.exists());

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_table(&txn2, "users").unwrap();
        txn2.commit().unwrap();

        assert!(!p1.exists(), "index file 1 should be removed by cascade");
        assert!(!p2.exists(), "index file 2 should be removed by cascade");
        assert!(catalog.get_index_by_name("users_id_idx").is_none());
        assert!(catalog.get_index_by_name("users_email_idx").is_none());
        assert!(catalog.indexes_for(table_file_id).is_empty());

        let txn3 = txn_mgr.begin().unwrap();
        let leftover = catalog
            .scan_system_table_where::<IndexRow, _>(&txn3, |r| r.table_id == table_file_id)
            .unwrap();
        txn3.commit().unwrap();
        assert!(
            leftover.is_empty(),
            "no IndexRows should remain for a dropped table"
        );
    }

    // create_index threads the table-column projection into the registry so
    // DML can build CompositeKeys from tuples without re-reading the system
    // table.
    #[test]
    fn create_index_stores_table_columns_in_registry() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());
        let table_file_id = make_three_col_table(&catalog, &txn_mgr);

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_index(
                &txn,
                "t_ca_idx",
                "t",
                table_file_id,
                &[col_id(2), col_id(0)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let live = catalog.get_index_by_name("t_ca_idx").unwrap();
        assert_eq!(live.table_columns, vec![col_id(2), col_id(0)]);
        assert_eq!(live.access.key_types().len(), 2);
    }

    // Cascade only touches indexes on the dropped table — indexes on other
    // tables must survive untouched.
    #[test]
    fn drop_table_cascade_does_not_touch_other_tables_indexes() {
        let dir = tempdir().unwrap();
        let (catalog, txn_mgr, ..) = make_full_infra(dir.path());
        let users_id = make_users_table(&catalog, &txn_mgr);

        // Second table with its own index.
        let other_id = {
            let schema = TupleSchema::new(vec![Field::new("k", Type::Int64).not_null()]);
            let txn = txn_mgr.begin().unwrap();
            let id = catalog.create_table(&txn, "other", schema, None).unwrap();
            txn.commit().unwrap();
            id
        };

        let txn = txn_mgr.begin().unwrap();
        catalog
            .create_index(
                &txn,
                "users_id_idx",
                "users",
                users_id,
                &[col_id(0)],
                IndexKind::Hash,
            )
            .unwrap();
        catalog
            .create_index(
                &txn,
                "other_k_idx",
                "other",
                other_id,
                &[col_id(0)],
                IndexKind::Hash,
            )
            .unwrap();
        txn.commit().unwrap();

        let txn2 = txn_mgr.begin().unwrap();
        catalog.drop_table(&txn2, "users").unwrap();
        txn2.commit().unwrap();

        assert!(catalog.get_index_by_name("users_id_idx").is_none());
        assert!(
            catalog.get_index_by_name("other_k_idx").is_some(),
            "unrelated index should still be registered"
        );
        assert_eq!(catalog.indexes_for(other_id).len(), 1);
        assert!(dir.path().join("idx_other_k_idx.dat").exists());
    }

    // ── replay_user_objects: catalog reopen ───────────────────────────────

    // After dropping the first catalog and reopening from the same data
    // directory, every previously-registered index must reappear in the
    // registry with the right table_columns and the right table file id.
    //
    // Note: tests force-flush the buffer pool before reopen because we
    // don't replay WAL yet — dirty pages would otherwise vanish with the
    // old PageStore. WAL replay is a separate, larger gap.
    #[test]
    fn replay_restores_registered_indexes_after_reopen() {
        let dir = tempdir().unwrap();
        let table_file_id;

        // First open: create table + composite index, then drop the catalog.
        {
            let (catalog, txn_mgr, bp, _) = make_full_infra(dir.path());
            table_file_id = make_three_col_table(&catalog, &txn_mgr);

            let txn = txn_mgr.begin().unwrap();
            catalog
                .create_index(
                    &txn,
                    "t_ca_idx",
                    "t",
                    table_file_id,
                    &[col_id(2), col_id(0)],
                    IndexKind::Hash,
                )
                .unwrap();
            txn.commit().unwrap();
            bp.flush_all().unwrap();
        }

        // Second open: same data dir, fresh PageStore + WAL + Catalog.
        let (catalog2, ..) = make_full_infra(dir.path());

        let live = catalog2
            .get_index_by_name("t_ca_idx")
            .expect("index should be replayed into the registry");

        assert_eq!(
            live.table_columns,
            vec![col_id(2), col_id(0)],
            "table_columns must be reconstructed from IndexRow + ColumnRow"
        );
        assert_eq!(live.access.kind(), IndexKind::Hash);
        assert_eq!(catalog2.indexes_for(table_file_id).len(), 1);
    }

    // After reopen, next_file_id must resume past every existing FileId
    // (table heaps and index files), and next_index_id past every existing
    // IndexId. Otherwise a fresh allocation would silently collide.
    #[test]
    fn replay_bumps_id_counters_past_existing_state() {
        let dir = tempdir().unwrap();
        let pre_max_index_id;
        let pre_index_file_id;
        let pre_table_file_id;

        {
            let (catalog, txn_mgr, bp, _) = make_full_infra(dir.path());
            pre_table_file_id = make_users_table(&catalog, &txn_mgr);

            let txn = txn_mgr.begin().unwrap();
            pre_max_index_id = catalog
                .create_index(
                    &txn,
                    "users_email_idx",
                    "users",
                    pre_table_file_id,
                    &[col_id(1)],
                    IndexKind::Hash,
                )
                .unwrap();
            txn.commit().unwrap();

            // Pull the index_file_id back out of the IndexRow on disk so the
            // assertion is independent of internal allocation order.
            let probe_txn = txn_mgr.begin().unwrap();
            let rows = catalog
                .scan_system_table_where::<IndexRow, _>(&probe_txn, |r| {
                    r.index_name == "users_email_idx"
                })
                .unwrap();
            probe_txn.commit().unwrap();
            pre_index_file_id = rows[0].index_file_id;
            bp.flush_all().unwrap();
        }

        let (catalog2, ..) = make_full_infra(dir.path());

        // The next allocations must skip past everything we saw before.
        let next_file = catalog2.next_file_id();
        assert!(
            next_file.0 > pre_table_file_id.0,
            "next_file_id ({next_file:?}) must be past the table file id ({pre_table_file_id:?})"
        );
        assert!(
            next_file.0 > pre_index_file_id.0,
            "next_file_id ({next_file:?}) must be past the index file id ({pre_index_file_id:?})"
        );

        let next_index = catalog2.next_index_id();
        assert!(
            next_index.0 > pre_max_index_id.0,
            "next_index_id ({next_index:?}) must be past the prior max ({pre_max_index_id:?})"
        );
    }

    // After reopen, table_exists must return true for every previously
    // committed user table. Without eager replay, this returned false until
    // something triggered a lazy lookup, and a CREATE TABLE in between would
    // silently duplicate an existing table.
    #[test]
    fn replay_makes_user_tables_visible_to_table_exists() {
        let dir = tempdir().unwrap();

        {
            let (catalog, txn_mgr, bp, _) = make_full_infra(dir.path());
            // Two tables with different shapes — make_users_table makes
            // "users", and we add "orders" inline.
            make_users_table(&catalog, &txn_mgr);
            let txn = txn_mgr.begin().unwrap();
            catalog
                .create_table(
                    &txn,
                    "orders",
                    TupleSchema::new(vec![Field::new("oid", Type::Int64).not_null()]),
                    None,
                )
                .unwrap();
            txn.commit().unwrap();
            bp.flush_all().unwrap();
        }

        let (catalog2, ..) = make_full_infra(dir.path());
        assert!(
            catalog2.table_exists("users"),
            "table_exists must see eagerly-replayed users"
        );
        assert!(
            catalog2.table_exists("orders"),
            "table_exists must see eagerly-replayed orders"
        );
        assert!(
            !catalog2.table_exists("never_made"),
            "table_exists must remain false for tables that don't exist"
        );
    }

    // The corruption scenario: after reopen, CREATE TABLE for an existing
    // name must error, not silently duplicate. This is the actual bug the
    // eager replay closes — without it, the duplicate slipped through both
    // the binder check and the catalog check.
    #[test]
    fn create_table_after_reopen_rejects_duplicate_name() {
        let dir = tempdir().unwrap();

        {
            let (catalog, txn_mgr, bp, _) = make_full_infra(dir.path());
            make_users_table(&catalog, &txn_mgr);
            bp.flush_all().unwrap();
        }

        let (catalog2, txn_mgr2, ..) = make_full_infra(dir.path());

        let txn = txn_mgr2.begin().unwrap();
        let schema = TupleSchema::new(vec![
            Field::new("id", Type::Int64).not_null(),
            Field::new("email", Type::String).not_null(),
        ]);
        let err = catalog2
            .create_table(&txn, "users", schema, None)
            .unwrap_err();
        txn.commit().unwrap();

        assert!(
            matches!(err, CatalogError::TableAlreadyExists { ref table_name } if table_name == "users"),
            "expected TableAlreadyExists, got: {err:?}"
        );
    }

    // Composite primary keys must round-trip through replay — the ordinal
    // ordering preserved on disk via PrimaryKeyColumnRow must produce the
    // same Vec<ColumnId> after restart.
    #[test]
    fn replay_preserves_composite_primary_key_ordering() {
        let dir = tempdir().unwrap();

        {
            let (catalog, txn_mgr, bp, _) = make_full_infra(dir.path());
            let txn = txn_mgr.begin().unwrap();
            catalog
                .create_table(
                    &txn,
                    "kv",
                    TupleSchema::new(vec![
                        Field::new("a", Type::Int64).not_null(),
                        Field::new("b", Type::Int64).not_null(),
                    ]),
                    // Declare PK as (b, a) — order matters and must
                    // round-trip across restart.
                    Some(vec![col_id(1), col_id(0)]),
                )
                .unwrap();
            txn.commit().unwrap();
            bp.flush_all().unwrap();
        }

        let (catalog2, txn_mgr2, ..) = make_full_infra(dir.path());
        let txn = txn_mgr2.begin().unwrap();
        let info = catalog2.get_table_info(&txn, "kv").unwrap();
        txn.commit().unwrap();

        assert_eq!(
            info.primary_key,
            Some(vec![col_id(1), col_id(0)]),
            "composite PK column order must survive replay"
        );
    }

    // The replayed live access method must point at the actual on-disk
    // file: previously-inserted (key, rid) pairs are visible after reopen.
    #[test]
    fn replay_preserves_index_data_visibility() {
        let dir = tempdir().unwrap();

        let inserted_key;
        {
            let (catalog, txn_mgr, bp, _) = make_full_infra(dir.path());
            let table_file_id = make_users_table(&catalog, &txn_mgr);
            let txn = txn_mgr.begin().unwrap();
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

            let engine = crate::engine::Engine::new(&catalog, &txn_mgr);
            engine
                .execute_statement(
                    crate::parser::Parser::new(
                        "INSERT INTO users (id, email) VALUES (1, 'persisted@x.com');",
                    )
                    .parse()
                    .unwrap(),
                )
                .unwrap();

            inserted_key =
                CompositeKey::single(crate::Value::String("persisted@x.com".to_string()));
            bp.flush_all().unwrap();
        }

        // Reopen.
        let (catalog2, txn_mgr2, ..) = make_full_infra(dir.path());
        let live = catalog2.get_index_by_name("users_email_idx").unwrap();
        let probe_txn = txn_mgr2.begin().unwrap();
        let hits = live
            .access
            .search(probe_txn.transaction_id(), &inserted_key)
            .unwrap();
        probe_txn.commit().unwrap();
        assert_eq!(
            hits.len(),
            1,
            "previously inserted (key, rid) must survive a reopen"
        );
    }
}
