use std::os::unix::fs::FileExt;
use std::{collections::HashMap, fs::File, path::Path, sync::Arc};

use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use crate::TransactionId;
use crate::buffer_pool::lock::{LockRequest, LockType};
use crate::{
    FileId, Lsn, PAGE_SIZE,
    buffer_pool::lock::{LockError, LockManager},
    primitives::PageId,
    wal::writer::{Wal, WalError},
};

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

struct FramePool {
    frames: Vec<Frame>,
    page_table: HashMap<PageId, usize>,
    clock_hand: usize,
}

pub struct PageStore {
    pool: Mutex<FramePool>,               // tightly coupled, one lock
    files: RwLock<HashMap<FileId, File>>, // independent, separate lock
    lock_manager: LockManager,
    wal: Arc<Wal>,
}

#[derive(Debug)]
struct Frame {
    data: [u8; PAGE_SIZE],
    page_id: Option<PageId>,
    pin_count: u32,
    dirty: bool,
    last_lsn: Lsn,
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

    pub fn fetch_page(
        &'_ self,
        transaction_id: TransactionId,
        page_id: PageId,
        lock_type: LockType,
    ) -> Result<PageGuard<'_>, PageStoreError> {
        let mut pool = self.pool.lock();
        let pool_ref = &mut *pool;

        if let Some(&frame_idx) = pool_ref.page_table.get(&page_id) {
            return self.fetch_from_cache(
                pool_ref,
                frame_idx,
                (transaction_id, page_id, lock_type).into(),
            );
        }

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
            {
                let files = self.files.read();
                if let Some(file) = files.get(&victim_pid.file_id) {
                    self.flush_frame_to_file(file, victim_pid, &pool.frames[idx])?;
                }
            }

            pool.page_table.remove(&victim_pid);
            pool.frames[idx] = Frame::default();
            return Ok(Some(idx));
        }
        Ok(None)
    }

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

    pub fn flush_all(&self) -> Result<(), PageStoreError> {
        let mut pool = self.pool.lock();
        let files = self.files.read();

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
        }

        Ok(())
    }

    pub fn release_all(&self, txn: TransactionId) {
        self.lock_manager.unlock_all_pages(txn);
    }
}

pub struct PageGuard<'a> {
    store: &'a PageStore,
    frame_idx: usize,
}

impl PageGuard<'_> {
    pub fn read(&self) -> [u8; PAGE_SIZE] {
        self.store.pool.lock().frames[self.frame_idx].data
    }

    pub fn write(&self, data: &[u8; PAGE_SIZE], lsn: Lsn) {
        let mut pool = self.store.pool.lock();
        let frame = &mut pool.frames[self.frame_idx];
        frame.data = *data;
        frame.dirty = true;
        frame.last_lsn = lsn;
    }
}

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
        buffer_pool::lock::LockType,
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

    /// Create a zeroed data file large enough for 4 pages, register it, and return its path.
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
            .fetch_page(tid(1), pid(1, 0), LockType::Shared)
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
                .fetch_page(tid(1), pid(1, 0), LockType::Exclusive)
                .unwrap();
            let mut data = [0u8; PAGE_SIZE];
            data[0] = 42;
            guard.write(&data, Lsn(1));
        }

        // cache hit → should see the written byte
        let guard = store
            .fetch_page(tid(1), pid(1, 0), LockType::Shared)
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
                .fetch_page(tid(1), pid(1, 0), LockType::Exclusive)
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
                .fetch_page(tid(1), pid(1, 0), LockType::Exclusive)
                .unwrap();
            let mut d = [0u8; PAGE_SIZE];
            d[0] = 1;
            g.write(&d, Lsn::INVALID);
        }
        {
            let g = store
                .fetch_page(tid(1), pid(1, 1), LockType::Exclusive)
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
                .fetch_page(tid(1), pid(1, 0), LockType::Shared)
                .unwrap();
        }

        assert!(store.unregister_file(fid(1)).is_ok());
    }

    #[test]
    fn unregister_file_returns_file_in_use_while_pinned() {
        let (store, dir) = make_store(4);
        register_data_file(&store, fid(1), &dir, "data.db");

        let _guard = store
            .fetch_page(tid(1), pid(1, 0), LockType::Shared)
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
                .fetch_page(tid(1), pid(1, 0), LockType::Shared)
                .unwrap();
        }
        {
            let _g = store
                .fetch_page(tid(1), pid(1, 1), LockType::Shared)
                .unwrap();
        }

        let result = store.fetch_page(tid(1), pid(1, 2), LockType::Shared);
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
                .fetch_page(tid(1), pid(1, 0), LockType::Shared)
                .unwrap();
        }

        {
            let _g = store
                .fetch_page(tid(1), pid(1, 0), LockType::Exclusive)
                .unwrap();
        }
        // guard dropped: pin → 0, but lock still held by tid(1)

        store.release_all(tid(1));

        // tid(2) can now acquire a shared lock on the same page
        assert!(
            store
                .fetch_page(tid(2), pid(1, 0), LockType::Shared)
                .is_ok()
        );
    }
}
