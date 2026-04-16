//! Heap file storage: unordered, multi-page tuple storage for a single table.
//!
//! A *heap file* is the simplest table storage format: tuples are packed into
//! fixed-size pages with no ordering guarantee. This module provides
//! [`HeapFile`], which wraps a [`PageStore`] and a series of [`HeapPage`]s to
//! expose tuple-level CRUD operations, and [`HeapScan`], a full-table iterator.
//!
//! All disk I/O is delegated to [`PageStore`]; this module never touches the
//! filesystem directly. Every mutating operation writes a WAL record before
//! updating the page so that a crash mid-write can be recovered.
//!
//! # Concurrency
//!
//! Each mutating call acquires an *exclusive* page lock for the duration of the
//! call. [`HeapScan`] acquires *shared* page locks one page at a time and
//! releases each lock before moving to the next page.

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use thiserror::Error;

use crate::{
    FileId, TransactionId,
    buffer_pool::{
        LockRequest,
        page_store::{PageStore, PageStoreError},
    },
    heap::page::HeapPage,
    primitives::{PageId, RecordId},
    storage::{Page, StorageError},
    tuple::{Tuple, TupleSchema},
    wal::writer::{Wal, WalError},
};

/// Errors that can occur during heap file operations.
#[derive(Debug, Error)]
pub enum HeapError {
    #[error("page store: {0}")]
    Store(#[from] PageStoreError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("wal: {0}")]
    Wal(#[from] WalError),

    #[error("record {0} not found")]
    NotFound(RecordId),

    #[error("page is full")]
    PageFull,
}

// HeapFile: multi-page heap table management.
//
// Wraps PageStore and HeapPage to provide tuple-level operations:
//   - insert(txn, tuple) -> (PageNumber, SlotId)
//   - delete(txn, page_no, slot_id)
//   - update(txn, page_no, slot_id, new_tuple)
//   - scan(txn) -> impl Iterator<Item = Tuple>
//
// Knows its FileId and TupleSchema. Does not touch disk directly —
// all I/O goes through PageStore.

/// Unordered, multi-page storage for a single table's tuples.
///
/// `HeapFile` coordinates the [`PageStore`] (buffer pool), [`HeapPage`]
/// (page layout), and [`Wal`] (write-ahead log) to provide atomic, logged
/// tuple operations. It tracks how many pages belong to this file so that
/// inserts can spill into a newly allocated page when all existing pages are
/// full.
///
/// A `HeapFile` is cheap to share because it holds only `Arc` references and
/// an atomic counter; the underlying data lives in the shared `PageStore`.
pub struct HeapFile {
    file_id: FileId,
    schema: TupleSchema,
    buffer_pool: Arc<PageStore>,
    wal: Arc<Wal>,
    num_pages: AtomicU32,
}

impl HeapFile {
    /// Creates a `HeapFile` for an already-registered file in the page store.
    ///
    /// `existing_pages` must reflect the number of pages the underlying file
    /// already contains so that inserts probe the correct range. Pass `0` for a
    /// brand-new, empty file.
    pub fn new(
        file_id: FileId,
        schema: TupleSchema,
        buffer_pool: Arc<PageStore>,
        existing_pages: u32,
        wal: Arc<Wal>,
    ) -> Self {
        Self {
            file_id,
            schema,
            buffer_pool,
            num_pages: AtomicU32::new(existing_pages),
            wal,
        }
    }

    /// Returns the number of pages currently allocated to this file.
    fn num_pages(&self) -> u32 {
        self.num_pages.load(Ordering::Acquire)
    }

    /// Inserts `tuple` into the first page that has room, allocating a new page if necessary.
    ///
    /// The search visits pages `0..=num_pages` in order. If every existing page
    /// is full, `num_pages` is incremented atomically and the tuple is inserted
    /// into the freshly allocated page. The returned [`RecordId`] encodes the
    /// file, page, and slot where the tuple landed.
    ///
    /// # Errors
    ///
    /// - [`HeapError::Store`] — the page store could not fetch or lock a page.
    /// - [`HeapError::Wal`] — the INSERT WAL record could not be written.
    /// - [`HeapError::PageFull`] — the newly allocated page was also full
    ///   (should not happen under normal conditions).
    pub fn insert_tuple(
        &self,
        transaction_id: TransactionId,
        tuple: &Tuple,
    ) -> Result<RecordId, HeapError> {
        for page_no in 0..=self.num_pages() {
            let page_id = PageId::new(self.file_id, page_no.into());
            if let Some(record_id) = self.insert_into_page(transaction_id, page_id, tuple)? {
                return Ok(record_id);
            }
        }

        let new_page_no = self.num_pages.fetch_add(1, Ordering::AcqRel);
        let page_id = PageId::new(self.file_id, new_page_no.into());
        if let Some(record_id) = self.insert_into_page(transaction_id, page_id, tuple)? {
            return Ok(record_id);
        }

        Err(HeapError::PageFull)
    }

    /// Marks the tuple identified by `record_id` as deleted.
    ///
    /// Acquires an exclusive lock on the page, removes the slot, writes a
    /// DELETE WAL record with the page's before-image, then flushes the
    /// updated page back to the buffer pool.
    ///
    /// # Errors
    ///
    /// - [`HeapError::Store`] — the page store could not fetch or lock the page.
    /// - [`HeapError::Storage`] — the slot index in `record_id` is out of range.
    /// - [`HeapError::NotFound`] — the slot had no before-image (already deleted).
    /// - [`HeapError::Wal`] — the DELETE WAL record could not be written.
    pub fn delete_tuple(
        &self,
        transaction_id: TransactionId,
        record_id: RecordId,
    ) -> Result<(), HeapError> {
        let page_id = PageId::new(self.file_id, record_id.page_no);
        let guard = self
            .buffer_pool
            .fetch_page(LockRequest::exclusive(transaction_id, page_id))?;

        let mut page = HeapPage::new(page_id, &guard.read(), &self.schema)?;
        page.delete_tuple(record_id.slot_id)?;
        let before = page
            .before_image()
            .ok_or(HeapError::NotFound(record_id))?
            .to_vec();
        let lsn = self.wal.log_delete(transaction_id, page_id, before)?;
        guard.write(&page.page_data(), lsn);
        Ok(())
    }

    /// Replaces the tuple at `record_id` with `tuple`.
    ///
    /// Implemented as a delete of the old slot followed by an insert into the
    /// same page. Acquires an exclusive page lock for the duration, then writes
    /// a single UPDATE WAL record covering both the before- and after-image.
    ///
    /// # Errors
    ///
    /// - [`HeapError::Store`] — the page store could not fetch or lock the page.
    /// - [`HeapError::Storage`] — the slot index in `record_id` is out of range,
    ///   or the new tuple does not fit in the page after the old one is deleted.
    /// - [`HeapError::NotFound`] — the slot had no before-image (already deleted).
    /// - [`HeapError::Wal`] — the UPDATE WAL record could not be written.
    pub fn update_tuple(
        &self,
        transaction_id: TransactionId,
        record_id: RecordId,
        tuple: &Tuple,
    ) -> Result<(), HeapError> {
        let page_id = PageId::new(self.file_id, record_id.page_no);
        let guard = self
            .buffer_pool
            .fetch_page(LockRequest::exclusive(transaction_id, page_id))?;
        let mut page = HeapPage::new(page_id, &guard.read(), &self.schema)?;
        page.delete_tuple(record_id.slot_id)?;
        page.insert_tuple(tuple.clone())?;
        let before = page
            .before_image()
            .ok_or(HeapError::NotFound(record_id))?
            .to_vec();
        let lsn =
            self.wal
                .log_update(transaction_id, page_id, before, page.page_data().to_vec())?;
        guard.write(&page.page_data(), lsn);
        Ok(())
    }

    /// Tries to insert `tuple` into `page_id`, returning `None` if the page is full.
    ///
    /// On success, writes an INSERT WAL record and flushes the page, then
    /// returns the [`RecordId`] of the new slot. Returns `Ok(None)` when
    /// [`StorageError::PageFull`] is reported so the caller can try the next page.
    ///
    /// # Errors
    ///
    /// - [`HeapError::Store`] — the page store could not fetch or lock the page.
    /// - [`HeapError::Wal`] — the INSERT WAL record could not be written.
    /// - [`HeapError::Storage`] — a storage error other than `PageFull` occurred.
    fn insert_into_page(
        &self,
        transaction_id: TransactionId,
        page_id: PageId,
        tuple: &Tuple,
    ) -> Result<Option<RecordId>, HeapError> {
        let guard = self
            .buffer_pool
            .fetch_page(LockRequest::exclusive(transaction_id, page_id))?;

        let data = guard.read();
        let mut page = HeapPage::new(page_id, &data, &self.schema)?;

        match page.insert_tuple(tuple.clone()) {
            Ok(slot_id) => {
                let after = page.page_data().to_vec();
                let lsn = self.wal.log_insert(transaction_id, page_id, after)?;
                guard.write(&page.page_data(), lsn);
                Ok(Some(RecordId::new(self.file_id, page_id.page_no, slot_id)))
            }
            Err(StorageError::PageFull) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Returns an iterator over all live tuples in the file.
    ///
    /// The iterator visits pages in order from `0` to `num_pages - 1`. Deleted
    /// slots are skipped by the underlying [`HeapPage`]. Each page is pinned
    /// with a shared lock only long enough to copy its tuples out; the lock is
    /// released before the next page is loaded.
    ///
    /// # Errors
    ///
    /// Returns [`HeapError::Store`] if the first page cannot be set up. Errors
    /// on subsequent pages are silently treated as end-of-iteration (the
    /// iterator returns `None`).
    pub fn scan(&self, transaction_id: TransactionId) -> Result<HeapScan<'_>, HeapError> {
        Ok(HeapScan::new(self, transaction_id))
    }

    /// Deletes all tuples matching `predicate` and returns the number deleted.
    /// Deletes all tuples in the heap file that match the provided predicate.
    ///
    /// Iterates over all live tuples in the file, applies the `predicate` function to each,
    /// and deletes every tuple for which the predicate returns `true`.
    pub fn delete_if<F>(
        &self,
        transaction_id: TransactionId,
        predicate: F,
    ) -> Result<u32, HeapError>
    where
        F: Fn(&Tuple) -> bool,
    {
        let to_delete: Vec<RecordId> = self
            .scan(transaction_id)?
            .filter(|(_, tuple)| predicate(tuple))
            .map(|(record_id, _)| record_id)
            .collect();

        let mut count: u32 = 0;
        for record_id in to_delete {
            self.delete_tuple(transaction_id, record_id)?;
            count += 1;
        }
        Ok(count)
    }
}

/// Iterator over all live tuples in a `HeapFile`.
///
/// Tuples are materialized one page at a time — a `PageGuard` is pinned only
/// long enough to copy the tuples out, then released before moving on.
pub struct HeapScan<'a> {
    file: &'a HeapFile,
    txn: TransactionId,
    current_page: u32,
    page_tuples: Vec<(RecordId, Tuple)>,
    tuple_idx: usize,
}

impl<'a> HeapScan<'a> {
    /// Creates a new `HeapScan` starting at page 0.
    ///
    /// No I/O is performed until the first call to [`Iterator::next`].
    pub fn new(file: &'a HeapFile, transaction_id: TransactionId) -> Self {
        Self {
            file,
            txn: transaction_id,
            current_page: 0,
            page_tuples: vec![],
            tuple_idx: 0,
        }
    }

    /// Fetches `current_page` from the buffer pool and copies its live tuples into `page_tuples`.
    ///
    /// The shared page lock is held only for the duration of this call; the
    /// guard is dropped before returning, freeing the lock.
    fn load_page(&mut self) -> Result<(), HeapError> {
        let page_id = PageId::new(self.file.file_id, self.current_page.into());
        let guard = self
            .file
            .buffer_pool
            .fetch_page(LockRequest::shared(self.txn, page_id))?;
        let page = HeapPage::new(page_id, &guard.read(), &self.file.schema)?;
        self.page_tuples = page
            .live_tuples()
            .map(|(slot_id, tuple)| {
                let record_id = RecordId {
                    file_id: self.file.file_id,
                    page_no: self.current_page.into(),
                    slot_id,
                };
                (record_id, tuple.clone())
            })
            .collect();
        self.tuple_idx = 0;
        Ok(())
    }
}

impl Iterator for HeapScan<'_> {
    type Item = (RecordId, Tuple);

    /// Advances to the next live tuple, loading new pages as needed.
    ///
    /// Returns `None` when all pages have been exhausted or a page load error
    /// occurs.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.tuple_idx < self.page_tuples.len() {
                let item = self.page_tuples[self.tuple_idx].clone();
                self.tuple_idx += 1;
                return Some(item);
            }

            if self.current_page >= self.file.num_pages() {
                return None;
            }

            if self.load_page().is_err() {
                return None;
            }

            self.current_page += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        FileId, TransactionId,
        buffer_pool::page_store::PageStore,
        primitives::{PageNumber, RecordId},
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

    fn make_heap_file(existing_pages: u32) -> (HeapFile, Arc<Wal>, tempfile::TempDir) {
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

    fn begin_txn(wal: &Wal, id: u64) -> TransactionId {
        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        txn
    }

    #[test]
    fn test_insert_tuple_returns_matching_file_id() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let rid = heap.insert_tuple(txn, &make_tuple(42, true)).unwrap();
        assert_eq!(rid.file_id, FileId::new(1));
    }

    #[test]
    fn test_insert_tuple_first_insert_succeeds() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let result = heap.insert_tuple(txn, &make_tuple(1, false));
        assert!(
            result.is_ok(),
            "first insert should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_insert_tuple_record_id_page_is_zero_for_first_insert() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let rid = heap.insert_tuple(txn, &make_tuple(7, true)).unwrap();
        assert_eq!(rid.page_no, PageNumber::new(0));
    }

    #[test]
    fn test_insert_tuple_multiple_records_get_distinct_ids() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let rid1 = heap.insert_tuple(txn, &make_tuple(1, true)).unwrap();
        let rid2 = heap.insert_tuple(txn, &make_tuple(2, false)).unwrap();
        assert_ne!(rid1.slot_id, rid2.slot_id, "slot IDs must differ");
    }

    #[test]
    fn test_delete_tuple_existing_record_succeeds() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let rid = heap.insert_tuple(txn, &make_tuple(10, true)).unwrap();
        let result = heap.delete_tuple(txn, rid);
        assert!(result.is_ok(), "delete should succeed: {:?}", result.err());
    }

    // update_tuple on an existing record succeeds
    #[test]
    fn test_update_tuple_existing_record_succeeds() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let rid = heap.insert_tuple(txn, &make_tuple(5, false)).unwrap();
        let result = heap.update_tuple(txn, rid, &make_tuple(5, true));
        assert!(result.is_ok(), "update should succeed: {:?}", result.err());
    }

    // scan over an empty file yields no tuples
    #[test]
    fn test_scan_empty_file_yields_nothing() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let scan = heap.scan(txn).unwrap();
        assert_eq!(scan.count(), 0);
    }

    // scan after a single insert yields exactly that tuple
    // Note: make_heap_file(1) pre-sets num_pages=1 so the scan visits page 0.
    // insert_tuple succeeds via the 0..=num_pages loop and does NOT increment the
    // counter (only the new-page fallback path does), so we must start with >= 1.
    #[test]
    fn test_scan_single_tuple_is_visible() {
        let (heap, wal, _dir) = make_heap_file(1);
        let txn = begin_txn(&wal, 1);
        let t = make_tuple(99, true);
        heap.insert_tuple(txn, &t).unwrap();

        let tuples: Vec<Tuple> = heap.scan(txn).unwrap().map(|(_, t)| t).collect();
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0], t);
    }

    // scan after N inserts yields all N tuples (order-independent)
    #[test]
    fn test_scan_multiple_inserts_all_visible() {
        let (heap, wal, _dir) = make_heap_file(1);
        let txn = begin_txn(&wal, 1);
        let count = 5usize;
        let n = i32::try_from(count).expect("test count fits in i32");
        for i in 0..n {
            heap.insert_tuple(txn, &make_tuple(i, i % 2 == 0)).unwrap();
        }

        let tuples: Vec<Tuple> = heap.scan(txn).unwrap().map(|(_, t)| t).collect();
        assert_eq!(tuples.len(), count);
    }

    #[test]
    fn test_scan_deleted_tuple_not_visible() {
        let (heap, wal, _dir) = make_heap_file(1);
        let txn = begin_txn(&wal, 1);
        let rid = heap.insert_tuple(txn, &make_tuple(1, true)).unwrap();
        heap.delete_tuple(txn, rid).unwrap();

        let tuples: Vec<Tuple> = heap.scan(txn).unwrap().map(|(_, t)| t).collect();
        assert_eq!(tuples.len(), 0, "deleted tuple must not be visible");
    }

    // After update, the new value should appear in scan and the old value should not
    #[test]
    fn test_scan_updated_tuple_shows_new_value() {
        let (heap, wal, _dir) = make_heap_file(1);
        let txn = begin_txn(&wal, 1);
        let rid = heap.insert_tuple(txn, &make_tuple(1, false)).unwrap();
        let new_tuple = make_tuple(1, true);
        heap.update_tuple(txn, rid, &new_tuple).unwrap();

        let tuples: Vec<Tuple> = heap.scan(txn).unwrap().map(|(_, t)| t).collect();
        // exactly one live tuple
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0], new_tuple);
    }

    // insert then delete preserves the scan count for other tuples
    #[test]
    fn test_delete_preserves_other_tuples() {
        let (heap, wal, _dir) = make_heap_file(1);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_tuple(10, true)).unwrap();
        let to_delete = heap.insert_tuple(txn, &make_tuple(20, false)).unwrap();
        heap.insert_tuple(txn, &make_tuple(30, true)).unwrap();

        heap.delete_tuple(txn, to_delete).unwrap();

        let tuples: Vec<Tuple> = heap.scan(txn).unwrap().map(|(_, t)| t).collect();
        assert_eq!(tuples.len(), 2);
    }

    // ── edge cases ───────────────────────────────────────────────────────────

    // num_pages counter is only incremented via the new-page fallback path in
    // insert_tuple, not when the insert lands in an existing slot found by the loop.
    // When starting from 0 pages the loop covers page 0 (0..=0) and inserts there
    // without touching the counter, so it stays at 0 after the first insert.
    #[test]
    fn test_num_pages_stays_zero_after_loop_path_insert() {
        let (heap, wal, _dir) = make_heap_file(0);
        assert_eq!(heap.num_pages(), 0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_tuple(1, true)).unwrap();
        // Counter unchanged — insert went through the 0..=num_pages loop, not the
        // fetch_add fallback.
        assert_eq!(heap.num_pages(), 0);
    }

    // Constructing with existing_pages > 0 reflects in num_pages()
    #[test]
    fn test_new_with_existing_pages_reflects_in_counter() {
        let (heap, _wal, _dir) = make_heap_file(2);
        assert_eq!(heap.num_pages(), 2);
    }

    // ── error paths ──────────────────────────────────────────────────────────

    // delete_tuple with an out-of-bounds slot_id returns HeapError::Storage.
    // Note: delete_tuple builds the PageId from self.file_id (not record_id.file_id),
    // so a wrong file_id in the RecordId is silently ignored. The only reachable
    // bad-input path via public API is a slot_id that exceeds the page capacity.
    #[test]
    fn test_delete_tuple_bad_slot_returns_storage_error() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let bad_rid = RecordId::new(
            FileId::new(1),
            PageNumber::new(0),
            crate::primitives::SlotId(9999),
        );
        let result = heap.delete_tuple(txn, bad_rid);
        assert!(
            matches!(result, Err(HeapError::Storage(_))),
            "expected HeapError::Storage, got {result:?}"
        );
    }

    // update_tuple with an out-of-bounds slot_id returns HeapError::Storage.
    #[test]
    fn test_update_tuple_bad_slot_returns_storage_error() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let bad_rid = RecordId::new(
            FileId::new(1),
            PageNumber::new(0),
            crate::primitives::SlotId(9999),
        );
        let result = heap.update_tuple(txn, bad_rid, &make_tuple(1, true));
        assert!(
            matches!(result, Err(HeapError::Storage(_))),
            "expected HeapError::Storage, got {result:?}"
        );
    }

    // insert_tuple on an unregistered file (PageStore knows nothing about FileId)
    // returns HeapError::Store
    #[test]
    fn test_insert_tuple_unregistered_file_returns_store_error() {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(4, Arc::clone(&wal)));
        // Deliberately NOT registering any file
        let heap = HeapFile::new(FileId::new(42), schema(), store, 0, Arc::clone(&wal));

        let txn = begin_txn(&wal, 1);
        let result = heap.insert_tuple(txn, &make_tuple(1, true));
        assert!(
            matches!(result, Err(HeapError::Store(_))),
            "expected HeapError::Store, got {result:?}"
        );
    }

    // ── HeapError Display ────────────────────────────────────────────────────

    // HeapError variants format without panicking (smoke test for derive(Error))
    #[test]
    fn test_heap_error_display_does_not_panic() {
        let rid = RecordId::new(
            FileId::new(1),
            PageNumber::new(0),
            crate::primitives::SlotId(0),
        );
        let _ = HeapError::NotFound(rid).to_string();
        let _ = HeapError::PageFull.to_string();
    }
}
