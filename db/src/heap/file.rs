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
//! # Page allocation
//!
//! Pages are numbered starting at zero. [`HeapFile`] tracks the current page
//! count with an atomic counter so it knows which pages to probe during an
//! insert. When all existing pages are full a new page number is claimed with
//! a single atomic fetch-add, and the tuple lands there.
//!
//! # Concurrency
//!
//! Each mutating call acquires an *exclusive* page lock for the duration of the
//! call. [`HeapScan`] acquires *shared* page locks one page at a time and
//! releases each lock before moving to the next page.
//!
//! # WAL ordering
//!
//! Every mutating method writes a WAL record *before* calling
//! [`PageGuard::write`] so the page image is never newer than the log. On
//! recovery the WAL can therefore redo any update that was flushed to the log
//! but whose dirty page was evicted before a crash.

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use thiserror::Error;

use crate::{
    FileId, PageNumber, TransactionId,
    buffer_pool::{
        LockRequest,
        page_store::{PageGuard, PageStore, PageStoreError},
    },
    heap::page::HeapPage,
    primitives::{PageId, RecordId, SlotId},
    storage::{Page, StorageError},
    tuple::{Tuple, TupleSchema},
    wal::writer::{Wal, WalError},
};

/// Errors that can occur during heap file operations.
#[derive(Debug, Error)]
pub enum HeapError {
    /// The buffer-pool page store returned an error (e.g. unregistered file,
    /// lock contention, or I/O failure).
    #[error("page store: {0}")]
    Store(#[from] PageStoreError),

    /// A page-layout or slot-level error was returned by the storage layer
    /// (e.g. the requested slot index is out of range).
    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    /// Writing a WAL record failed.
    #[error("wal: {0}")]
    Wal(#[from] WalError),

    /// The slot referenced by the [`RecordId`] has no live tuple — it was
    /// never written or has already been deleted.
    #[error("record {0} not found")]
    NotFound(RecordId),

    /// A freshly allocated page has no room for the tuple being inserted.
    /// Under normal conditions this should never happen; it indicates the
    /// tuple is larger than an entire page.
    #[error("page is full")]
    PageFull,
}

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

    /// Returns the current page count with acquire ordering so the caller
    /// sees all pages initialized by prior atomic stores.
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
            let page_id = self.page_id(page_no);
            if let Some(record_id) = self.insert_into_page(transaction_id, page_id, tuple)? {
                return Ok(record_id);
            }
        }

        let new_page_no = self.num_pages.fetch_add(1, Ordering::AcqRel);
        let page_id = self.page_id(new_page_no);
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
        let page_id = self.page_id(record_id.page_no);
        let guard = self.guard(page_id, transaction_id, true)?;

        let mut page = self.h_page(&guard.read())?;
        page.delete_tuple(record_id.slot_id)?;
        let lsn = self.wal.log_page_delete(transaction_id, page_id, &page)?;
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
        let page_id = self.page_id(record_id.page_no);
        let guard = self.guard(page_id, transaction_id, true)?;
        let mut page = self.h_page(&guard.read())?;
        page.delete_tuple(record_id.slot_id)?;
        page.insert_tuple(tuple.clone())?;
        let lsn = self.wal.log_page_update(transaction_id, page_id, &page)?;
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
        let mut once = std::iter::once(tuple.clone());
        Ok(self
            .fill_page(transaction_id, page_id, &mut once)?
            .into_iter()
            .next())
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

    /// Inserts as many tuples from `tuples` into `page_id` as will fit.
    ///
    /// Acquires an exclusive lock on the page, calls [`HeapPage::insert_many`]
    /// to pack tuples until the page is full, then writes a single INSERT WAL
    /// record for the whole batch and flushes the page. Returns the number of
    /// tuples that were actually inserted.
    ///
    /// # Errors
    ///
    /// - [`HeapError::Store`] — the page could not be fetched or locked.
    /// - [`HeapError::Storage`] — an unexpected page-layout error occurred.
    /// - [`HeapError::Wal`] — the WAL record could not be written.
    fn fill_page<I>(
        &self,
        transaction_id: TransactionId,
        page_id: PageId,
        tuples: &mut I,
    ) -> Result<Vec<RecordId>, HeapError>
    where
        I: Iterator<Item = Tuple>,
    {
        let guard = self.guard(page_id, transaction_id, true)?;
        let mut page = self.h_page(&guard.read())?;

        let inserted = page.insert_many(tuples)?;
        if inserted.is_empty() {
            return Ok(Vec::new());
        }

        let lsn = self.wal.log_page_insert(transaction_id, page_id, &page)?;
        guard.write(&page.page_data(), lsn);
        self.num_pages
            .fetch_max(page_id.page_no.0 + 1, Ordering::AcqRel);

        let page_no = page_id.page_no;
        Ok(inserted
            .into_iter()
            .map(|slot_id| self.record_id(page_no, slot_id))
            .collect())
    }

    /// Inserts a batch of tuples as efficiently as possible, filling existing
    /// pages before allocating new ones.
    ///
    /// Iterates through pages `0..=num_pages` and calls [`Self::fill_page`] on
    /// each. If the iterator is not yet exhausted after all existing pages have
    /// been tried, new pages are claimed atomically until every tuple has been
    /// stored. Returns the total number of tuples inserted.
    ///
    /// This is more efficient than repeated [`Self::insert_tuple`] calls
    /// because it batches WAL writes: one WAL record is written per page
    /// rather than one per tuple.
    ///
    /// # Errors
    ///
    /// Propagates any [`HeapError`] returned by [`Self::fill_page`].
    pub fn bulk_insert<I>(&self, txn: TransactionId, tuples: I) -> Result<Vec<RecordId>, HeapError>
    where
        I: IntoIterator<Item = Tuple>,
    {
        let mut iter = tuples.into_iter().peekable();
        let mut record_ids = Vec::new();

        for page_no in 0..=self.num_pages() {
            if iter.peek().is_none() {
                break;
            }
            let page_id = self.page_id(page_no);
            record_ids.extend(self.fill_page(txn, page_id, &mut iter)?);
        }

        while iter.peek().is_some() {
            let page_no = self.num_pages.fetch_add(1, Ordering::AcqRel);
            let page_id = self.page_id(page_no);
            record_ids.extend(self.fill_page(txn, page_id, &mut iter)?);
        }

        Ok(record_ids)
    }

    /// Deletes every tuple for which `predicate` returns `true` and returns
    /// the number of tuples deleted.
    ///
    /// Performs a full scan to collect matching [`RecordId`]s, then deletes
    /// them one by one. The scan and the deletions run in the same transaction
    /// but each deletion acquires its own exclusive page lock, so other
    /// transactions interleaved between the scan and the deletions may observe
    /// a partially-deleted state.
    ///
    /// # Errors
    ///
    /// - [`HeapError::Store`] — a page could not be fetched or locked.
    /// - [`HeapError::Storage`] — a slot was out of range.
    /// - [`HeapError::Wal`] — a DELETE WAL record could not be written.
    pub fn bulk_delete<F>(
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

    /// Wraps raw page `data` in a [`HeapPage`] view bound to this file's schema.
    #[inline]
    fn h_page(&self, data: &[u8]) -> Result<HeapPage<'_>, StorageError> {
        HeapPage::new(data, &self.schema)
    }

    /// Creates a new `PageId` for the given page number.
    #[inline]
    fn page_id(&self, page_no: impl Into<PageNumber>) -> PageId {
        PageId::new(self.file_id, page_no.into())
    }

    /// Creates a new `RecordId` for the given page number and slot ID.
    #[inline]
    fn record_id(&self, page_no: impl Into<PageNumber>, slot_id: SlotId) -> RecordId {
        RecordId::new(self.file_id, page_no.into(), slot_id)
    }

    /// Fetches `page_id` from the buffer pool and returns a [`PageGuard`].
    ///
    /// Requests an exclusive lock when `exclusive` is `true`, or a shared lock
    /// otherwise. The guard keeps the page pinned in the buffer pool until it
    /// is dropped.
    fn guard(
        &self,
        page_id: PageId,
        transaction_id: TransactionId,
        exclusive: bool,
    ) -> Result<PageGuard<'_>, PageStoreError> {
        let req = if exclusive {
            LockRequest::exclusive(transaction_id, page_id)
        } else {
            LockRequest::shared(transaction_id, page_id)
        };
        self.buffer_pool.fetch_page(req)
    }

    /// Reads all live tuples from the given page number.
    ///
    /// # Errors
    ///
    /// - [`HeapError::Store`] — the page could not be fetched or locked.
    /// - [`HeapError::Storage`] — a slot was out of range.
    ///
    /// # Returns
    ///
    /// A vector of `(RecordId, Tuple)` pairs.
    pub(super) fn read_live_tuples(
        &self,
        page_no: impl Into<PageNumber> + Copy,
        transaction_id: TransactionId,
    ) -> Result<Vec<(RecordId, Tuple)>, HeapError> {
        let page_id = self.page_id(page_no);
        let guard = self.guard(page_id, transaction_id, false)?;
        let page = self.h_page(&guard.read())?;
        Ok(page
            .live_tuples()
            .map(|(slot_id, tuple)| (self.record_id(page_no, slot_id), tuple.clone()))
            .collect())
    }
}

/// Iterator over all live tuples in a `HeapFile`.
///
/// Tuples are materialized one page at a time — a `PageGuard` is pinned only
/// long enough to copy the tuples out, then released before moving on.
///
/// The lifetime `'a` ties this iterator to the [`HeapFile`] it was created
/// from, borrowing the file to access the buffer pool and schema without
/// cloning them.
pub struct HeapScan<'a> {
    /// The heap file being scanned.
    file: &'a HeapFile,
    /// Transaction under whose context pages are locked.
    txn: TransactionId,
    /// Index of the page that will be loaded on the next [`HeapScan::load_page`] call.
    ///
    /// Incremented each time the current `page_tuples` buffer is exhausted.
    current_page: u32,
    /// Tuples copied out of the most recently loaded page.
    ///
    /// Held in a `Vec` so the page lock can be released immediately after the
    /// copy, rather than being held for the lifetime of the iterator.
    page_tuples: Vec<(RecordId, Tuple)>,
    /// Position within `page_tuples` for the next call to [`Iterator::next`].
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

    /// Fetches the current page from the buffer pool and copies its live tuples into `page_tuples`.
    ///
    /// The shared page lock is held only for the duration of this call; the
    /// guard is dropped before returning, freeing the lock.
    fn load_page(&mut self) -> Result<(), HeapError> {
        self.page_tuples = self.file.read_live_tuples(self.current_page, self.txn)?;
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

            if self.current_page >= self.file.num_pages() || self.load_page().is_err() {
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

    // num_pages counter must reflect the highest page that's been written to,
    // regardless of which insert path (for-loop or fetch_add fallback) placed
    // the tuple. This guarantees scan sees pages that hold live tuples.
    #[test]
    fn test_num_pages_reflects_highest_written_page() {
        let (heap, wal, _dir) = make_heap_file(0);
        assert_eq!(heap.num_pages(), 0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_tuple(1, true)).unwrap();
        // Insert landed on page 0 via the for-loop path; counter must bump to 1
        // so HeapScan visits page 0.
        assert_eq!(heap.num_pages(), 1);
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

    // ── bulk_insert ──────────────────────────────────────────────────────────

    #[test]
    fn test_bulk_insert_empty_iterator_returns_zero() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let record_ids = heap.bulk_insert(txn, std::iter::empty()).unwrap();
        assert_eq!(record_ids.len(), 0);
        assert_eq!(heap.scan(txn).unwrap().count(), 0);
    }

    #[test]
    fn test_bulk_insert_returns_count_matching_input() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let tuples: Vec<Tuple> = (0..50).map(|i| make_tuple(i, i % 2 == 0)).collect();
        let count = heap.bulk_insert(txn, tuples).unwrap();
        assert_eq!(count.len(), 50);
    }

    #[test]
    fn test_bulk_insert_all_tuples_visible_in_scan() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let tuples: Vec<Tuple> = (0..25).map(|i| make_tuple(i, true)).collect();
        heap.bulk_insert(txn, tuples.clone()).unwrap();

        let scanned: Vec<Tuple> = heap.scan(txn).unwrap().map(|(_, t)| t).collect();
        assert_eq!(scanned.len(), tuples.len());
        for t in &tuples {
            assert!(scanned.contains(t), "missing tuple {t:?}");
        }
    }

    #[test]
    fn test_bulk_insert_fills_existing_page_before_allocating() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.insert_tuple(txn, &make_tuple(1, true)).unwrap();
        let before_pages = heap.num_pages();

        heap.bulk_insert(txn, vec![make_tuple(2, false), make_tuple(3, true)])
            .unwrap();

        // A couple of small tuples should fit on page 0, not force a new page.
        assert_eq!(heap.num_pages(), before_pages);
        assert_eq!(heap.scan(txn).unwrap().count(), 3);
    }

    #[test]
    fn test_bulk_insert_then_additional_inserts_coexist() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        heap.bulk_insert(txn, (0..10).map(|i| make_tuple(i, true)))
            .unwrap();
        heap.insert_tuple(txn, &make_tuple(100, false)).unwrap();
        assert_eq!(heap.scan(txn).unwrap().count(), 11);
    }

    // ── bulk_delete ──────────────────────────────────────────────────────────

    #[test]
    fn test_bulk_delete_empty_file_returns_zero() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let count = heap.bulk_delete(txn, |_| true).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_bulk_delete_no_matches_returns_zero() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for i in 0..5 {
            heap.insert_tuple(txn, &make_tuple(i, true)).unwrap();
        }
        let count = heap.bulk_delete(txn, |_| false).unwrap();
        assert_eq!(count, 0);
        assert_eq!(heap.scan(txn).unwrap().count(), 5);
    }

    #[test]
    fn test_bulk_delete_all_matches_clears_file() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for i in 0..8 {
            heap.insert_tuple(txn, &make_tuple(i, true)).unwrap();
        }
        let count = heap.bulk_delete(txn, |_| true).unwrap();
        assert_eq!(count, 8);
        assert_eq!(heap.scan(txn).unwrap().count(), 0);
    }

    #[test]
    fn test_bulk_delete_predicate_filters_correctly() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        for i in 0..10 {
            heap.insert_tuple(txn, &make_tuple(i, i % 2 == 0)).unwrap();
        }
        let count = heap
            .bulk_delete(txn, |t| matches!(t.get(1), Some(Value::Bool(true))))
            .unwrap();
        assert_eq!(count, 5);

        let remaining: Vec<Tuple> = heap.scan(txn).unwrap().map(|(_, t)| t).collect();
        assert_eq!(remaining.len(), 5);
        for t in &remaining {
            assert!(matches!(t.get(1), Some(Value::Bool(false))));
        }
    }

    #[test]
    fn test_bulk_insert_then_bulk_delete_round_trip() {
        let (heap, wal, _dir) = make_heap_file(0);
        let txn = begin_txn(&wal, 1);
        let inserted = heap
            .bulk_insert(txn, (0..50).map(|i| make_tuple(i, true)))
            .unwrap();
        let deleted = heap.bulk_delete(txn, |_| true).unwrap();
        assert_eq!(inserted.len(), usize::try_from(deleted).unwrap());
        assert_eq!(heap.scan(txn).unwrap().count(), 0);
    }

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
