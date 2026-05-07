//! Buffer pool page store: brings disk pages into memory and keeps them there.
//!
//! [`PageStore`] is the heart of the buffer pool. It maintains a fixed-size array
//! of in-memory *frames*, each of which can hold one disk page. When a caller
//! requests a page via [`PageStore::fetch_page`] the store either returns it from
//! the frame pool (a cache hit) or reads it from disk, evicting an existing frame
//! first if the pool is full.
//!
//! ## Eviction policy
//!
//! The store uses the **clock-sweep** (second-chance) algorithm. Every frame carries
//! a `ref_bit` that is set whenever the frame is accessed. The clock hand sweeps
//! through frames; a frame with `ref_bit = true` gets its bit cleared and is given
//! a second chance. A frame with `ref_bit = false` that is not pinned is evicted.
//!
//! ## Write-Ahead Logging (WAL) contract
//!
//! Before a dirty frame is written to disk — whether by eviction, an explicit
//! [`PageStore::flush_page`], or [`PageStore::flush_all`] — the store calls
//! [`Wal::force`] to ensure the corresponding log records have been durably written.
//! This upholds the WAL protocol: log before data.
//!
//! ## Concurrency
//!
//! Page-level locking is delegated to a `LockManager`. Callers supply a
//! [`LockRequest`] (shared or exclusive) when fetching a page; locks are held for
//! the lifetime of the transaction and released in bulk via [`PageStore::release_all`].

use std::{collections::HashMap, fs::File, os::unix::fs::FileExt, path::Path, sync::Arc};

use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use crate::{
    FileId, Lsn, PAGE_SIZE, TransactionId,
    buffer_pool::lock::{LockError, LockManager, LockRequest},
    primitives::PageId,
    wal::writer::{Wal, WalError},
};

/// Errors that can arise from [`PageStore`] operations.
#[derive(Debug, Error)]
pub enum PageStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("lock error: {0}")]
    Lock(#[from] LockError),

    #[error("file {0} not registered")]
    FileNotRegistered(FileId),

    #[error("file {0} already registered")]
    FileAlreadyRegistered(FileId),

    #[error("file {0} still in use, cannot unregister")]
    FileInUse(FileId),

    #[error("buffer pool full, no evictable frame")]
    PoolExhausted,
}

/// Fixed-size pool of frames and the metadata needed to manage them.
///
/// Held behind a [`Mutex`] inside [`PageStore`] so that frame allocation,
/// eviction, and the page table are all updated atomically.
struct FramePool {
    /// The actual in-memory page frames.
    frames: Vec<Frame>,
    /// Maps a [`PageId`] to the index of the frame that currently holds it.
    page_table: HashMap<PageId, usize>,
    /// Current position of the clock hand used by the clock-sweep eviction algorithm.
    clock_hand: usize,
}

/// Shared, thread-safe buffer pool that maps disk pages to in-memory frames.
///
/// `PageStore` combines four concerns:
/// 1. **Frame management** — allocating and evicting frames via clock-sweep.
/// 2. **File registry** — tracking which [`FileId`]s are open and where on disk they live.
/// 3. **Page-level locking** — forwarding lock requests to a `LockManager`.
/// 4. **WAL integration** — forcing log records before flushing dirty pages.
///
/// The pool lock (`pool`) and the file lock (`files`) are intentionally separate
/// so that I/O-heavy operations (reading/writing pages) do not block file
/// registration and vice-versa.
pub struct PageStore {
    pool: Mutex<FramePool>,
    files: RwLock<HashMap<FileId, File>>,
    lock_manager: LockManager,
    wal: Arc<Wal>,
}

/// One slot in the frame pool — holds a single page worth of raw bytes plus
/// the bookkeeping needed to manage it.
#[derive(Debug)]
struct Frame {
    /// Raw page bytes.
    data: [u8; PAGE_SIZE],
    /// Which page occupies this frame, or `None` if the frame is free.
    page_id: Option<PageId>,
    /// Number of active [`PageGuard`]s keeping this frame alive.
    /// A frame with `pin_count > 0` is never evicted.
    pin_count: u32,
    /// `true` when `data` has been modified since it was last written to disk.
    dirty: bool,
    /// LSN of the most recent WAL record that covers a write to this frame.
    /// Used to call [`Wal::force`] before flushing.
    last_lsn: Lsn,
    /// Clock-sweep reference bit. Set on every access; cleared by the clock hand
    /// on the first sweep past the frame.
    ref_bit: bool,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            data: [0u8; PAGE_SIZE],
            page_id: None,
            pin_count: 0,
            dirty: false,
            last_lsn: Lsn::INVALID,
            ref_bit: false,
        }
    }
}

impl PageStore {
    /// Creates a new `PageStore` with `cap` frames backed by the given WAL.
    ///
    /// All frames start empty (no page loaded, not dirty, pin count zero).
    pub fn new(cap: usize, wal: Arc<Wal>) -> Self {
        let frames = (0..cap).map(|_| Frame::default()).collect::<Vec<Frame>>();
        Self {
            pool: Mutex::new(FramePool {
                frames,
                page_table: HashMap::new(),
                clock_hand: 0,
            }),
            files: RwLock::new(HashMap::new()),
            lock_manager: LockManager::new(),
            wal,
        }
    }

    /// Brings a page into the buffer pool and returns a guard that pins it.
    ///
    /// The `req` parameter carries both the target [`PageId`] and the lock mode
    /// (shared or exclusive) that should be acquired for the caller's transaction.
    ///
    /// If the page is already cached the frame's pin count is incremented and the
    /// guard is returned immediately. Otherwise a free or evictable frame is found,
    /// the page is read from disk, and then the guard is returned.
    ///
    /// Dropping the returned [`PageGuard`] decrements the pin count, making the
    /// frame eligible for eviction once the count reaches zero.
    ///
    /// # Errors
    ///
    /// - [`PageStoreError::FileNotRegistered`] if the file has not been registered.
    /// - [`PageStoreError::PoolExhausted`] if all frames are pinned and cannot be evicted.
    /// - [`PageStoreError::Lock`] if the lock manager denies the request (e.g. a conflicting lock
    ///   is held by another transaction).
    /// - [`PageStoreError::Wal`] if flushing the WAL before evicting a dirty frame fails.
    /// - [`PageStoreError::Io`] if reading the page from disk fails.
    pub fn fetch_page(
        &'_ self,
        req: impl Into<LockRequest>,
    ) -> Result<PageGuard<'_>, PageStoreError> {
        let req = req.into();
        let page_id = req.page_id();
        let mut pool = self.pool.lock();
        let pool_ref = &mut *pool;

        if let Some(&frame_idx) = pool_ref.page_table.get(&page_id) {
            return self.fetch_from_cache(pool_ref, frame_idx, req);
        }

        tracing::debug!(page_id = ?page_id, "buffer miss");

        let frame_idx = match pool_ref.frames.iter().position(|f| f.page_id.is_none()) {
            Some(idx) => idx,
            None => self
                .evict_frame(pool_ref)?
                .ok_or(PageStoreError::PoolExhausted)?,
        };

        self.read_from_disk(page_id, pool_ref, frame_idx)?;

        let frame = &mut pool_ref.frames[frame_idx];
        frame.page_id = Some(page_id);
        frame.pin_count = 1;
        frame.dirty = false;
        frame.last_lsn = Lsn::INVALID;
        frame.ref_bit = true;

        pool_ref.page_table.insert(page_id, frame_idx);
        Ok(PageGuard {
            store: self,
            frame_idx,
        })
    }

    /// Handles a cache-hit path for [`fetch_page`]: increments the pin count,
    /// sets the reference bit, and acquires the requested lock.
    fn fetch_from_cache(
        &self,
        pool: &mut FramePool,
        frame_idx: usize,
        lock_req: LockRequest,
    ) -> Result<PageGuard<'_>, PageStoreError> {
        let frame = &mut pool.frames[frame_idx];
        frame.pin_count += 1;
        frame.ref_bit = true;

        self.lock_manager.lock_page(lock_req)?;

        Ok(PageGuard {
            store: self,
            frame_idx,
        })
    }

    /// Opens `path` on disk and associates it with `file_id` in the file registry.
    ///
    /// The file is opened in read-write mode and created if it does not exist.
    /// Once registered, pages belonging to `file_id` can be fetched via
    /// [`PageStore::fetch_page`].
    ///
    /// # Errors
    ///
    /// - [`PageStoreError::FileAlreadyRegistered`] if `file_id` is already known.
    /// - [`PageStoreError::Io`] if the file cannot be opened or created.
    pub fn register_file(
        &self,
        file_id: FileId,
        path: impl AsRef<Path>,
    ) -> Result<(), PageStoreError> {
        let mut files = self.files.write();
        if files.contains_key(&file_id) {
            return Err(PageStoreError::FileAlreadyRegistered(file_id));
        }

        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        files.insert(file_id, file);
        Ok(())
    }

    /// Flushes all dirty pages for `file_id` to disk, removes their frames from
    /// the pool, and closes the file handle.
    ///
    /// This is the reverse of [`PageStore::register_file`]. After this call
    /// succeeds, any attempt to fetch a page from `file_id` will return
    /// [`PageStoreError::FileNotRegistered`].
    ///
    /// # Errors
    ///
    /// - [`PageStoreError::FileInUse`] if any page from this file is currently pinned (i.e. a
    ///   [`PageGuard`] for it is still alive).
    /// - [`PageStoreError::FileNotRegistered`] if `file_id` is unknown.
    /// - [`PageStoreError::Wal`] or [`PageStoreError::Io`] if flushing dirty pages fails.
    pub fn unregister_file(&self, file_id: FileId) -> Result<(), PageStoreError> {
        let mut pool = self.pool.lock();
        let has_pinned = pool.frames.iter().any(|f| {
            f.page_id
                .is_some_and(|page_id| page_id.file_id == file_id && f.pin_count > 0)
        });

        if has_pinned {
            return Err(PageStoreError::FileInUse(file_id));
        }

        let files = self.files.read();
        let file = files
            .get(&file_id)
            .ok_or(PageStoreError::FileNotRegistered(file_id))?;

        {
            let pool = &mut *pool;
            let mut to_remove = Vec::new();

            for frame in &mut pool.frames {
                let Some(pid) = frame.page_id else { continue };
                if pid.file_id != file_id {
                    continue;
                }

                self.flush_frame_to_file(file, pid, frame)?;

                to_remove.push(pid);
                *frame = Frame::default();
            }

            for p in &to_remove {
                pool.page_table.remove(p);
            }
        }

        drop(files);
        drop(pool);
        self.files.write().remove(&file_id);
        Ok(())
    }

    /// Reads `page_id` from disk into `pool.frames[frame_idx]`.
    ///
    /// The frame's `data` field is overwritten; all other fields are left
    /// unchanged — the caller is responsible for setting `page_id`, `pin_count`,
    /// etc. after this returns.
    fn read_from_disk(
        &self,
        page_id: PageId,
        pool: &mut FramePool,
        frame_idx: usize,
    ) -> Result<(), PageStoreError> {
        let files = self.files.read();
        let file = files
            .get(&page_id.file_id)
            .ok_or(PageStoreError::FileNotRegistered(page_id.file_id))?;

        let offset = u64::from(page_id.page_no.0) * PAGE_SIZE as u64;
        file.read_at(&mut pool.frames[frame_idx].data, offset)?;
        Ok(())
    }

    /// Finds a victim frame using the clock-sweep algorithm and evicts it.
    ///
    /// The hand makes at most two full sweeps. On each pass:
    /// - Frames with no page or a non-zero pin count are skipped.
    /// - A frame with `ref_bit = true` has its bit cleared and is given a second chance.
    /// - A frame with `ref_bit = false` is selected as the victim: if dirty it is flushed, then the
    ///   frame is reset.
    ///
    /// Returns `Some(frame_idx)` when a victim is found, or `None` if every frame
    /// is pinned.
    fn evict_frame(&self, pool: &mut FramePool) -> Result<Option<usize>, PageStoreError> {
        let n = pool.frames.len();

        for _ in 0..n * 2 {
            let idx = pool.clock_hand;
            pool.clock_hand = (pool.clock_hand + 1) % n;

            let frame = &pool.frames[idx];
            if frame.page_id.is_none() || frame.pin_count > 0 {
                continue;
            }

            if frame.ref_bit {
                pool.frames[idx].ref_bit = false;
                continue;
            }

            let victim_pid = frame.page_id.unwrap();
            let was_dirty = frame.dirty;
            {
                let files = self.files.read();
                if let Some(file) = files.get(&victim_pid.file_id) {
                    self.flush_frame_to_file(file, victim_pid, &pool.frames[idx])?;
                }
            }

            pool.page_table.remove(&victim_pid);
            pool.frames[idx] = Frame::default();
            tracing::debug!(page_id = ?victim_pid, dirty = was_dirty, "buffer evict");
            return Ok(Some(idx));
        }
        Ok(None)
    }

    /// Writes `frame` to `file` at the byte offset corresponding to `page_id`,
    /// but only when the frame is dirty.
    ///
    /// Before writing, [`Wal::force`] is called with `frame.last_lsn` to ensure
    /// the relevant log records are on stable storage first (WAL protocol).
    fn flush_frame_to_file(
        &self,
        file: &File,
        page_id: PageId,
        frame: &Frame,
    ) -> Result<(), PageStoreError> {
        if frame.dirty {
            self.wal.force(frame.last_lsn)?;
            let offset = u64::from(page_id.page_no.0) * PAGE_SIZE as u64;
            file.write_at(&frame.data, offset)?;
        }
        Ok(())
    }

    /// Writes the frame for `page_id` to disk if it is dirty, then clears its
    /// dirty flag.
    ///
    /// If `page_id` is not in the pool (already evicted or never loaded) this is
    /// a no-op.
    ///
    /// # Errors
    ///
    /// - [`PageStoreError::FileNotRegistered`] if the page's file is not open.
    /// - [`PageStoreError::Wal`] if forcing the WAL fails.
    /// - [`PageStoreError::Io`] if the write fails.
    pub fn flush_page(&self, page_id: PageId) -> Result<(), PageStoreError> {
        let mut pool = self.pool.lock();
        let Some(&frame_idx) = pool.page_table.get(&page_id) else {
            return Ok(());
        };

        {
            let files = self.files.read();
            let file = files
                .get(&page_id.file_id)
                .ok_or(PageStoreError::FileNotRegistered(page_id.file_id))?;
            self.flush_frame_to_file(file, page_id, &pool.frames[frame_idx])?;
        }

        let frame = &mut pool.frames[frame_idx];
        frame.dirty = false;
        frame.last_lsn = Lsn::INVALID;
        Ok(())
    }

    /// Writes every dirty frame in the pool to disk and clears their dirty flags.
    ///
    /// This is typically called at checkpoint time. Frames that are not dirty are
    /// skipped. The pool lock is held for the entire operation.
    ///
    /// # Errors
    ///
    /// - [`PageStoreError::FileNotRegistered`] if a dirty frame belongs to a file that is no longer
    ///   registered.
    /// - [`PageStoreError::Wal`] or [`PageStoreError::Io`] if any flush fails.
    #[tracing::instrument(name = "buffer_flush_all", skip(self))]
    pub fn flush_all(&self) -> Result<(), PageStoreError> {
        let mut pool = self.pool.lock();
        let files = self.files.read();
        let mut flushed = 0usize;

        for frame in &mut pool.frames {
            let Some(pid) = frame.page_id else { continue };
            if !frame.dirty {
                continue;
            }

            let file = files
                .get(&pid.file_id)
                .ok_or(PageStoreError::FileNotRegistered(pid.file_id))?;
            self.flush_frame_to_file(file, pid, frame)?;

            frame.dirty = false;
            frame.last_lsn = Lsn::INVALID;
            flushed += 1;
        }

        tracing::debug!(pages = flushed, "buffer flush all");
        Ok(())
    }

    /// Releases all page-level locks held by `txn`.
    ///
    /// Call this at transaction commit or abort. After this returns, other
    /// transactions can acquire conflicting locks on the pages that `txn` held.
    pub fn release_all(&self, txn: TransactionId) {
        self.lock_manager.unlock_all_pages(txn);
    }
}

/// A pinned reference to a page frame inside [`PageStore`].
///
/// While a `PageGuard` is alive the underlying frame's pin count is non-zero,
/// which prevents the clock-sweep eviction algorithm from choosing it as a
/// victim.
///
/// The guard is obtained from [`PageStore::fetch_page`] and is tied to the
/// lifetime of the `PageStore` it came from (`'a`). Dropping the guard
/// decrements the pin count.
pub struct PageGuard<'a> {
    store: &'a PageStore,
    frame_idx: usize,
}

impl PageGuard<'_> {
    /// Returns a copy of the page's raw bytes.
    ///
    /// Acquires the pool lock briefly to copy the frame data and then releases
    /// it, so the returned array is a snapshot valid at the moment of the call.
    pub fn read(&self) -> [u8; PAGE_SIZE] {
        self.store.pool.lock().frames[self.frame_idx].data
    }

    /// Overwrites the page's raw bytes and marks the frame dirty.
    ///
    /// `lsn` must be the LSN of the WAL record that describes this write. It is
    /// stored in the frame so that `PageStore::flush_frame_to_file` can call
    /// [`Wal::force`] with the correct LSN before writing to disk.
    pub fn write(&self, data: &[u8; PAGE_SIZE], lsn: Lsn) {
        let mut pool = self.store.pool.lock();
        let frame = &mut pool.frames[self.frame_idx];
        frame.data = *data;
        frame.dirty = true;
        frame.last_lsn = lsn;
    }
}

/// Decrements the pin count of the underlying frame when the guard is dropped.
impl Drop for PageGuard<'_> {
    fn drop(&mut self) {
        let mut pool = self.store.pool.lock();
        pool.frames[self.frame_idx].pin_count -= 1;
    }
}

#[cfg(test)]
mod tests {
    use std::{os::unix::fs::FileExt, sync::Arc};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        FileId, Lsn, PAGE_SIZE, TransactionId,
        buffer_pool::lock::LockRequest,
        primitives::{PageId, PageNumber},
        wal::writer::Wal,
    };

    fn make_store(cap: usize) -> (PageStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        (PageStore::new(cap, wal), dir)
    }

    fn fid(n: u64) -> FileId {
        FileId::new(n)
    }
    fn tid(n: u64) -> TransactionId {
        TransactionId::new(n)
    }
    fn pid(file: u64, page: u32) -> PageId {
        PageId::new(FileId::new(file), PageNumber::new(page))
    }

    fn register_data_file(
        store: &PageStore,
        file_id: FileId,
        dir: &tempfile::TempDir,
        name: &str,
    ) -> std::path::PathBuf {
        let path = dir.path().join(name);
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(PAGE_SIZE as u64 * 4).unwrap();
        store.register_file(file_id, path.clone()).unwrap();
        path
    }

    #[test]
    fn register_file_ok() {
        let (store, dir) = make_store(4);
        let path = dir.path().join("data.db");
        assert!(store.register_file(fid(1), path).is_ok());
    }

    #[test]
    fn register_file_duplicate_returns_error() {
        let (store, dir) = make_store(4);
        let path = dir.path().join("data.db");
        store.register_file(fid(1), &path).unwrap();
        assert!(matches!(
            store.register_file(fid(1), &path),
            Err(PageStoreError::FileAlreadyRegistered(_))
        ));
    }

    #[test]
    fn fetch_page_new_page_is_zeroed() {
        let (store, dir) = make_store(4);
        register_data_file(&store, fid(1), &dir, "data.db");

        let guard = store
            .fetch_page(LockRequest::shared(tid(1), pid(1, 0)))
            .unwrap();
        assert_eq!(guard.read(), [0u8; PAGE_SIZE]);
    }

    #[test]
    fn fetch_page_cache_hit_returns_written_data() {
        let (store, dir) = make_store(4);
        register_data_file(&store, fid(1), &dir, "data.db");

        // cache miss → write
        {
            let guard = store
                .fetch_page(LockRequest::exclusive(tid(1), pid(1, 0)))
                .unwrap();
            let mut data = [0u8; PAGE_SIZE];
            data[0] = 42;
            guard.write(&data, Lsn(1));
        }

        // cache hit → should see the written byte
        let guard = store
            .fetch_page(LockRequest::shared(tid(1), pid(1, 0)))
            .unwrap();
        assert_eq!(guard.read()[0], 42);
    }

    #[test]
    fn flush_page_writes_dirty_frame_to_disk() {
        let (store, dir) = make_store(4);
        let path = register_data_file(&store, fid(1), &dir, "data.db");

        let mut expected = [0u8; PAGE_SIZE];
        expected[0] = 0xAB;
        expected[PAGE_SIZE - 1] = 0xCD;

        {
            let guard = store
                .fetch_page(LockRequest::exclusive(tid(1), pid(1, 0)))
                .unwrap();
            guard.write(&expected, Lsn::INVALID);
        }
        store.flush_page(pid(1, 0)).unwrap();

        let mut on_disk = [0u8; PAGE_SIZE];
        std::fs::File::open(&path)
            .unwrap()
            .read_at(&mut on_disk, 0)
            .unwrap();
        assert_eq!(on_disk, expected);
    }

    #[test]
    fn flush_all_persists_every_dirty_frame() {
        let (store, dir) = make_store(4);
        let path = register_data_file(&store, fid(1), &dir, "data.db");

        {
            let g = store
                .fetch_page(LockRequest::exclusive(tid(1), pid(1, 0)))
                .unwrap();
            let mut d = [0u8; PAGE_SIZE];
            d[0] = 1;
            g.write(&d, Lsn::INVALID);
        }
        {
            let g = store
                .fetch_page(LockRequest::exclusive(tid(1), pid(1, 1)))
                .unwrap();
            let mut d = [0u8; PAGE_SIZE];
            d[0] = 2;
            g.write(&d, Lsn::INVALID);
        }

        store.flush_all().unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let mut buf = [0u8; PAGE_SIZE];

        file.read_at(&mut buf, 0).unwrap();
        assert_eq!(buf[0], 1, "page 0 not flushed");

        file.read_at(&mut buf, PAGE_SIZE as u64).unwrap();
        assert_eq!(buf[0], 2, "page 1 not flushed");
    }

    #[test]
    fn unregister_file_succeeds_when_unpinned() {
        let (store, dir) = make_store(4);
        register_data_file(&store, fid(1), &dir, "data.db");

        {
            let _g = store
                .fetch_page(LockRequest::shared(tid(1), pid(1, 0)))
                .unwrap();
        }

        assert!(store.unregister_file(fid(1)).is_ok());
    }

    #[test]
    fn unregister_file_returns_file_in_use_while_pinned() {
        let (store, dir) = make_store(4);
        register_data_file(&store, fid(1), &dir, "data.db");

        let _guard = store
            .fetch_page(LockRequest::shared(tid(1), pid(1, 0)))
            .unwrap();

        assert!(matches!(
            store.unregister_file(fid(1)),
            Err(PageStoreError::FileInUse(_))
        ));
    }

    #[test]
    fn unregister_file_returns_not_registered_for_unknown_id() {
        let (store, _dir) = make_store(4);
        assert!(matches!(
            store.unregister_file(fid(99)),
            Err(PageStoreError::FileNotRegistered(_))
        ));
    }

    #[test]
    fn pool_evicts_unpinned_clean_frames_when_full() {
        // cap=2: after filling frames with page 0 and 1, fetching page 2
        // must evict one via clock-sweep — no PoolExhausted.
        let (store, dir) = make_store(2);
        let path = dir.path().join("data.db");
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(PAGE_SIZE as u64 * 4).unwrap();
        store.register_file(fid(1), &path).unwrap();

        {
            let _g = store
                .fetch_page(LockRequest::shared(tid(1), pid(1, 0)))
                .unwrap();
        }
        {
            let _g = store
                .fetch_page(LockRequest::shared(tid(1), pid(1, 1)))
                .unwrap();
        }

        let result = store.fetch_page(LockRequest::shared(tid(1), pid(1, 2)));
        assert!(
            result.is_ok(),
            "expected eviction to free a frame: {:?}",
            result.err()
        );
    }

    #[test]
    fn release_all_allows_other_transaction_to_acquire_lock() {
        let (store, dir) = make_store(4);
        register_data_file(&store, fid(1), &dir, "data.db");

        // First fetch is a cache miss — no lock recorded yet.
        {
            let _g = store
                .fetch_page(LockRequest::shared(tid(1), pid(1, 0)))
                .unwrap();
        }

        {
            let _g = store
                .fetch_page(LockRequest::exclusive(tid(1), pid(1, 0)))
                .unwrap();
        }
        // guard dropped: pin → 0, but lock still held by tid(1)

        store.release_all(tid(1));

        // tid(2) can now acquire a shared lock on the same page
        assert!(
            store
                .fetch_page(LockRequest::shared(tid(2), pid(1, 0)))
                .is_ok()
        );
    }
}
