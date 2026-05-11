//! Table scan executors: sequential and (future) index-based tuple retrieval.
//!
//! This module provides the scan layer that sits at the leaves of a query
//! execution plan.  Each scan implements both [`FallibleIterator`] — so
//! callers can pull tuples one at a time — and [`Executor`], which adds the
//! `schema()` and `rewind()` contract required by the planner.
//!
//! # Current state
//! * [`SeqScan`] is fully implemented and scans every visible tuple in a [`HeapFile`] under a given
//!   transaction.
//! * [`IndexScan`] resolves [`RecordId`]s from an [`AnyIndex`] (`search` / `range_search`) and
//!   materializes table tuples via [`HeapFile::fetch_tuple`].

use fallible_iterator::FallibleIterator;

use super::{ExecutionError, Executor};
use crate::{
    TransactionId,
    heap::file::{HeapFile, HeapScan},
    index::{AnyIndex, CompositeKey},
    primitives::RecordId,
    tuple::{Tuple, TupleSchema},
};

/// A sequential scan over every tuple stored in a [`HeapFile`].
///
/// The scan is lazy: no I/O happens until the first call to
/// [`FallibleIterator::next`].  The underlying [`HeapScan`] is created on
/// demand and discarded by [`Executor::rewind`], which resets iteration back
/// to the beginning of the file.
///
/// `SeqScan` borrows the heap file for its lifetime `'a`, so the file must
/// outlive the scan.
pub struct SeqScan<'a> {
    file: &'a HeapFile,
    txn: TransactionId,
    schema: TupleSchema,
    inner: Option<HeapScan<'a>>,
}

impl std::fmt::Debug for SeqScan<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeqScan")
            .field("txn", &self.txn)
            .field("schema", &self.schema)
            .field("initialized", &self.inner.is_some())
            .finish()
    }
}

impl<'a> SeqScan<'a> {
    /// Creates a new sequential scan over `file` scoped to transaction `txn`.
    ///
    /// The schema is cloned from the file eagerly so that `schema()` never
    /// touches the file again.  The actual page iterator is not started until
    /// the first call to `next()`.
    pub fn new(file: &'a HeapFile, txn: TransactionId) -> Self {
        let schema = file.schema().clone(); // need a pub schema() accessor on HeapFile
        Self {
            file,
            txn,
            schema,
            inner: None,
        }
    }
}

impl FallibleIterator for SeqScan<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    /// Advances the scan and returns the next tuple, or `Ok(None)` when all
    /// tuples have been consumed.
    ///
    /// On the first call the underlying [`HeapScan`] is initialized
    /// automatically, so callers do not need to call any `open()` method.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionError::Storage`] if the heap layer reports an I/O or
    /// storage error while reading a page.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        if self.inner.is_none() {
            self.inner = Some(HeapScan::new(self.file, self.txn));
        }
        match fallible_iterator::FallibleIterator::next(self.inner.as_mut().unwrap()) {
            Ok(Some((_rid, tuple))) => Ok(Some(tuple)),
            Ok(None) => Ok(None),
            Err(e) => Err(ExecutionError::Storage(e.to_string())),
        }
    }
}

/// How [`IndexScan`] collects [`RecordId`]s from an [`AnyIndex`] before heap lookups.
#[derive(Debug, Clone)]
pub enum IndexScanSpec {
    /// Point probe: [`AnyIndex::search`].
    Search(CompositeKey),
    /// Inclusive key range: [`AnyIndex::range_search`].
    Range {
        start: CompositeKey,
        end: CompositeKey,
    },
}

/// Index scan: look up qualifying RIDs in a secondary index, then fetch full rows from the heap.
///
/// B-tree vs hash is determined by the concrete [`AnyIndex`] instance; this operator only calls
/// [`AnyIndex::search`] or [`AnyIndex::range_search`]. Empty index slots (stale RIDs) are skipped.
pub struct IndexScan<'a> {
    heap: &'a HeapFile,
    index: &'a AnyIndex,
    txn: TransactionId,
    schema: TupleSchema,
    spec: IndexScanSpec,
    /// Filled on first [`FallibleIterator::next`]; cleared by [`Executor::rewind`].
    rids: Option<Vec<RecordId>>,
    next_rid: usize,
}

impl std::fmt::Debug for IndexScan<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexScan")
            .field("txn", &self.txn)
            .field("schema", &self.schema)
            .field("spec", &self.spec)
            .field("index", &self.index)
            .field("initialized", &self.rids.is_some())
            .finish_non_exhaustive()
    }
}

impl<'a> IndexScan<'a> {
    /// Builds an index scan over `heap` using `index`, under transaction `txn`, according to
    /// `spec`.
    pub fn new(
        heap: &'a HeapFile,
        index: &'a AnyIndex,
        txn: TransactionId,
        spec: IndexScanSpec,
    ) -> Self {
        Self {
            heap,
            index,
            txn,
            schema: heap.schema().clone(),
            spec,
            rids: None,
            next_rid: 0,
        }
    }

    /// Loads the `RecordId`s (RIDs) qualifying for this index scan into the internal buffer.
    ///
    /// - For [`IndexScanSpec::Search`], looks up all RIDs associated with the given key using the
    ///   index.
    /// - For [`IndexScanSpec::Range`], retrieves all RIDs within the specified key range.
    ///
    /// The RIDs are obtained from the index and stored in `self.rids`. The index may include some
    /// stale RIDs (i.e., deleted records) which are skipped later during scanning. After loading,
    /// `self.next_rid` is reset to 0 to begin iteration from the start.
    ///
    /// Returns an error if the underlying index access fails, wrapping the error as an
    /// [`ExecutionError::Index`].
    fn load_rids(&mut self) -> Result<(), ExecutionError> {
        let vec = match &self.spec {
            IndexScanSpec::Search(key) => self.index.search(self.txn, key),
            IndexScanSpec::Range { start, end } => self.index.range_search(self.txn, start, end),
        }
        .map_err(|e| ExecutionError::Index(e.to_string()))?;
        self.rids = Some(vec);
        self.next_rid = 0;
        Ok(())
    }
}

impl Executor for SeqScan<'_> {
    /// Returns the schema of the tuples produced by this scan.
    ///
    /// The schema is cloned once at construction time and remains stable for
    /// the entire lifetime of the scan, including across `rewind()` calls.
    fn schema(&self) -> &TupleSchema {
        &self.schema
    }

    /// Resets the scan so the next call to `next()` returns the first tuple
    /// again.
    ///
    /// Dropping the inner [`HeapScan`] is enough: `next()` will recreate it
    /// on demand.
    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.inner = None; // next() will recreate
        Ok(())
    }
}

impl FallibleIterator for IndexScan<'_> {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        if self.rids.is_none() {
            self.load_rids()?;
        }
        let rids = self
            .rids
            .as_ref()
            .ok_or_else(|| ExecutionError::TypeError("index scan rid buffer missing".into()))?;
        while self.next_rid < rids.len() {
            let rid = rids[self.next_rid];
            self.next_rid += 1;
            if let Some(t) = self
                .heap
                .fetch_tuple(self.txn, rid)
                .map_err(|e| ExecutionError::Storage(e.to_string()))?
            {
                return Ok(Some(t));
            }
        }
        Ok(None)
    }
}

impl Executor for IndexScan<'_> {
    fn schema(&self) -> &TupleSchema {
        &self.schema
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        self.rids = None;
        self.next_rid = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fallible_iterator::FallibleIterator;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        FileId, TransactionId,
        buffer_pool::page_store::PageStore,
        index::{AnyIndex, CompositeKey},
        primitives::{PageNumber, RecordId, SlotId},
        tuple::{Field, Tuple, TupleSchema},
        types::{Type, Value},
        wal::writer::Wal,
    };

    fn field(name: &str, field_type: Type) -> Field {
        Field::new(name, field_type).unwrap()
    }

    fn schema() -> TupleSchema {
        TupleSchema::new(vec![field("id", Type::Int32), field("flag", Type::Bool)])
    }

    fn make_tuple(id: i32, flag: bool) -> Tuple {
        Tuple::new(vec![Value::Int32(id), Value::Bool(flag)])
    }

    fn begin_txn(wal: &Wal, id: u64) -> TransactionId {
        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        txn
    }

    fn make_registered_heap_file(existing_pages: u32) -> (HeapFile, Arc<Wal>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(16, Arc::clone(&wal)));

        let file_id = FileId::new(1);
        let path = dir.path().join("heap.db");

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        let needed = (existing_pages as usize).max(4) * crate::storage::PAGE_SIZE;
        file.set_len(needed as u64).unwrap();
        drop(file);

        store.register_file(file_id, &path).unwrap();

        let heap = HeapFile::new(
            file_id,
            schema(),
            Arc::clone(&store),
            existing_pages,
            Arc::clone(&wal),
        );
        (heap, wal, dir)
    }

    fn make_heap_and_btree_index() -> (HeapFile, AnyIndex, Arc<Wal>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(16, Arc::clone(&wal)));

        let heap_fid = FileId::new(1);
        let heap_path = dir.path().join("heap.db");
        let hf = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&heap_path)
            .unwrap();
        hf.set_len(4 * crate::storage::PAGE_SIZE as u64).unwrap();
        drop(hf);
        store.register_file(heap_fid, &heap_path).unwrap();
        let heap = HeapFile::new(heap_fid, schema(), Arc::clone(&store), 0, Arc::clone(&wal));

        let idx_fid = FileId::new(2);
        let idx_path = dir.path().join("btree_idx.db");
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&idx_path)
            .unwrap();
        store.register_file(idx_fid, &idx_path).unwrap();

        let any = AnyIndex::btree()
            .file_id(idx_fid)
            .key_types(vec![Type::Int32])
            .store(store)
            .existing_pages(0)
            .build()
            .unwrap();

        (heap, any, wal, dir)
    }

    // SeqScan yields all tuples for a heap file in insertion order until exhaustion.
    #[test]
    fn test_seqscan_next_returns_inserted_tuples_then_none() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);

        let t1 = make_tuple(1, true);
        let t2 = make_tuple(2, false);
        heap.insert_tuple(txn, &t1).unwrap();
        heap.insert_tuple(txn, &t2).unwrap();

        let mut scan = SeqScan::new(&heap, txn);

        let mut out = Vec::new();
        while let Some(t) = scan.next().unwrap() {
            out.push(t);
        }

        assert_eq!(out.len(), 2);
        assert_eq!(out[0], t1);
        assert_eq!(out[1], t2);
    }

    // schema() returns the cached schema matching the underlying heap file.
    #[test]
    fn test_seqscan_schema_matches_heap_schema() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);

        let scan = SeqScan::new(&heap, txn);
        assert_schema_equivalent(scan.schema(), heap.schema());
    }

    // --- edge cases ---

    // Empty heap returns Ok(None) immediately.
    #[test]
    fn test_seqscan_next_empty_heap_returns_none() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);

        let mut scan = SeqScan::new(&heap, txn);
        assert_eq!(scan.next().unwrap(), None);
    }

    // Debug output reflects lazy initialization before/after first next().
    #[test]
    fn test_seqscan_debug_reflects_initialized_flag() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);

        heap.insert_tuple(txn, &make_tuple(1, true)).unwrap();

        let mut scan = SeqScan::new(&heap, txn);
        let before = format!("{scan:?}");
        assert!(before.contains("initialized"));
        assert!(
            before.contains("false"),
            "expected initialized=false in Debug, got {before}"
        );

        let _ = scan.next().unwrap();

        let after = format!("{scan:?}");
        assert!(after.contains("initialized"));
        assert!(
            after.contains("true"),
            "expected initialized=true in Debug, got {after}"
        );
    }

    // Rewind resets iteration so the next tuple is the first one again.
    #[test]
    fn test_seqscan_rewind_resets_iteration() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);

        let t1 = make_tuple(10, true);
        let t2 = make_tuple(11, false);
        heap.insert_tuple(txn, &t1).unwrap();
        heap.insert_tuple(txn, &t2).unwrap();

        let mut scan = SeqScan::new(&heap, txn);

        let first = scan.next().unwrap().unwrap();
        assert_eq!(first, t1);

        scan.rewind().unwrap();

        let first_again = scan.next().unwrap().unwrap();
        assert_eq!(first_again, t1);
    }

    // schema() remains stable across next()/rewind() calls.
    #[test]
    fn test_seqscan_schema_stable_across_next_and_rewind() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);

        heap.insert_tuple(txn, &make_tuple(1, true)).unwrap();

        let mut scan = SeqScan::new(&heap, txn);
        let p1 = std::ptr::from_ref::<TupleSchema>(scan.schema());

        let _ = scan.next().unwrap();
        let p2 = std::ptr::from_ref::<TupleSchema>(scan.schema());

        scan.rewind().unwrap();
        let p3 = std::ptr::from_ref::<TupleSchema>(scan.schema());

        assert_eq!(p1, p2);
        assert_eq!(p2, p3);
        assert_eq!(
            scan.schema().physical_num_fields(),
            heap.schema().physical_num_fields()
        );
        assert_eq!(
            scan.schema().field(0).unwrap().name,
            heap.schema().field(0).unwrap().name
        );
    }

    // Rewind is a no-op for an empty scan (still returns None).
    #[test]
    fn test_seqscan_rewind_on_empty_keeps_none() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);

        let mut scan = SeqScan::new(&heap, txn);
        assert_eq!(scan.next().unwrap(), None);

        scan.rewind().unwrap();
        assert_eq!(scan.next().unwrap(), None);
    }

    fn assert_schema_equivalent(a: &TupleSchema, b: &TupleSchema) {
        assert_eq!(a.physical_num_fields(), b.physical_num_fields());
        for i in 0..a.physical_num_fields() {
            let af = a.field(i).unwrap();
            let bf = b.field(i).unwrap();
            assert_eq!(af.name, bf.name);
            assert_eq!(af.field_type, bf.field_type);
            assert_eq!(af.nullable, bf.nullable);
        }
    }

    #[test]
    fn test_index_scan_search_returns_matching_heap_tuple() {
        let (heap, index, wal, _dir) = make_heap_and_btree_index();
        let txn = begin_txn(&wal, 1);

        let row = make_tuple(42, true);
        let rid = heap.insert_tuple(txn, &row).unwrap();
        let key = CompositeKey::single(Value::Int32(42));
        index.insert(txn, &key, rid).unwrap();

        let mut scan = IndexScan::new(&heap, &index, txn, IndexScanSpec::Search(key.clone()));
        assert_schema_equivalent(scan.schema(), heap.schema());

        assert_eq!(scan.next().unwrap(), Some(row));
        assert_eq!(scan.next().unwrap(), None);
    }

    #[test]
    fn test_index_scan_range_returns_tuples_in_bounds() {
        let (heap, index, wal, _dir) = make_heap_and_btree_index();
        let txn = begin_txn(&wal, 1);

        let r10 = heap.insert_tuple(txn, &make_tuple(10, true)).unwrap();
        let r20 = heap.insert_tuple(txn, &make_tuple(20, false)).unwrap();
        let r30 = heap.insert_tuple(txn, &make_tuple(30, true)).unwrap();
        index
            .insert(txn, &CompositeKey::single(Value::Int32(10)), r10)
            .unwrap();
        index
            .insert(txn, &CompositeKey::single(Value::Int32(20)), r20)
            .unwrap();
        index
            .insert(txn, &CompositeKey::single(Value::Int32(30)), r30)
            .unwrap();

        let mut scan = IndexScan::new(&heap, &index, txn, IndexScanSpec::Range {
            start: CompositeKey::single(Value::Int32(15)),
            end: CompositeKey::single(Value::Int32(25)),
        });

        let mut out = Vec::new();
        while let Some(t) = scan.next().unwrap() {
            out.push(t);
        }
        out.sort_by_key(|t| match t.get(0).unwrap() {
            Value::Int32(v) => *v,
            _ => 0,
        });
        assert_eq!(out, vec![make_tuple(20, false)]);
    }

    #[test]
    fn test_index_scan_rewind_replays_from_index() {
        let (heap, index, wal, _dir) = make_heap_and_btree_index();
        let txn = begin_txn(&wal, 1);

        let row = make_tuple(7, false);
        let rid = heap.insert_tuple(txn, &row).unwrap();
        let key = CompositeKey::single(Value::Int32(7));
        index.insert(txn, &key, rid).unwrap();

        let mut scan = IndexScan::new(&heap, &index, txn, IndexScanSpec::Search(key));
        assert_eq!(scan.next().unwrap(), Some(row.clone()));
        assert_eq!(scan.next().unwrap(), None);

        scan.rewind().unwrap();
        assert_eq!(scan.next().unwrap(), Some(row));
        assert_eq!(scan.next().unwrap(), None);
    }

    #[test]
    fn test_index_scan_skips_stale_rid_after_heap_delete() {
        let (heap, index, wal, _dir) = make_heap_and_btree_index();
        let txn = begin_txn(&wal, 1);

        let row = make_tuple(99, true);
        let rid = heap.insert_tuple(txn, &row).unwrap();
        let key = CompositeKey::single(Value::Int32(99));
        index.insert(txn, &key, rid).unwrap();
        heap.delete_tuple(txn, rid).unwrap();

        let mut scan = IndexScan::new(&heap, &index, txn, IndexScanSpec::Search(key));
        assert_eq!(scan.next().unwrap(), None);
    }

    #[test]
    fn test_fetch_tuple_wrong_file_id_errors() {
        let (heap, wal, _dir) = make_registered_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let bad = RecordId::new(
            FileId::new(999),
            PageNumber::new(0),
            SlotId::new(0).unwrap(),
        );
        let err = heap.fetch_tuple(txn, bad).unwrap_err();
        assert!(matches!(err, crate::heap::file::HeapError::BadRecordId));
    }
}
