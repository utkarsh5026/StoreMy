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
//! * [`IndexScan`] is a placeholder; all methods call `todo!()`.

use fallible_iterator::FallibleIterator;

use super::{ExecutionError, Executor};
use crate::{
    TransactionId,
    heap::file::{HeapFile, HeapScan},
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
    /// The heap file being scanned.
    file: &'a HeapFile,
    /// Transaction under whose visibility rules tuples are read.
    txn: TransactionId,
    /// Schema cloned from the heap file at construction time.
    schema: TupleSchema,
    /// The live page-level iterator; `None` before the first `next()` call or
    /// after a `rewind()`.
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
    /// On the first call the underlying [`HeapScan`] is initialised
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

/// Index scan executor — not yet implemented.
///
/// This is a stub that will eventually drive tuple retrieval through a B-tree
/// or other index structure.  All methods currently call `todo!()`.
#[derive(Debug)]
pub struct IndexScan;

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

impl FallibleIterator for IndexScan {
    type Item = Tuple;
    type Error = ExecutionError;

    fn next(&mut self) -> Result<Option<Tuple>, ExecutionError> {
        todo!()
    }
}

impl Executor for IndexScan {
    fn schema(&self) -> &TupleSchema {
        todo!()
    }

    fn rewind(&mut self) -> Result<(), ExecutionError> {
        todo!()
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
        tuple::{Field, Tuple, TupleSchema},
        types::{Type, Value},
        wal::writer::Wal,
    };

    fn schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Int32),
            Field::new("flag", Type::Bool),
        ])
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
        assert_eq!(scan.schema().num_fields(), heap.schema().num_fields());
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
        assert_eq!(a.num_fields(), b.num_fields());
        for i in 0..a.num_fields() {
            let af = a.field(i).unwrap();
            let bf = b.field(i).unwrap();
            assert_eq!(af.name, bf.name);
            assert_eq!(af.field_type, bf.field_type);
            assert_eq!(af.nullable, bf.nullable);
        }
    }
}
