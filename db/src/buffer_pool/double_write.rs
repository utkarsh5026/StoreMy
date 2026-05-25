//! Double-write buffer: prevents permanent data loss from torn pages.
//!
//! ## The problem
//!
//! A disk write of one 4 096-byte page touches several 512-byte sectors.
//! If the OS is interrupted (power loss, kernel panic) part-way through, some
//! sectors contain the new version of the page and the rest still hold the old
//! one.  The resulting *torn page* fails its CRC32 checksum and is permanently
//! unreadable — unless a redundant copy exists.
//!
//! ## The solution (InnoDB-style)
//!
//! Before writing a dirty page to its real heap-file location the page is
//! first written to this **double-write buffer file** (`double_write.bin`) and
//! synced to disk.  Only then is the real write performed.
//!
//! ```text
//! 1. WAL forced         (log before data — already done by PageStore)
//! 2. DWB slot written   ← new step (this module)
//! 3. DWB file fsynced   ← new step (this module)
//! 4. Real heap write
//! 5. DWB slot released  ← new step (this module)
//! ```
//!
//! If the process dies between steps 3 and 5, the next startup finds the
//! occupied slot, checks the real page's CRC32, and overwrites the torn real
//! page with the DWB copy.  If the process dies before step 3 the DWB slot is
//! either absent or its checksum is bad; the real page was never touched, so no
//! repair is needed.
//!
//! ## On-disk layout
//!
//! `double_write.bin` is a flat array of fixed-size *slots*.  Each slot is:
//!
//! ```text
//! ┌────────────────────────────────────────── SLOT_HEADER_SIZE bytes (512) ──┐
//! │ occupied  (u8)  — 0 = free, 1 = written-but-not-yet-real-flushed      │
//! │ _pad      (7 B)                                                         │
//! │ file_id   (u64 LE)  — which heap file                                  │
//! │ page_no   (u32 LE)  — which page within that file                      │
//! │ path_len  (u32 LE)  — byte length of the stored file path              │
//! │ path      (256 B)   — UTF-8 file path, zero-padded                     │
//! │ _reserved (232 B)   — padding to SLOT_HEADER_SIZE                         │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ┌────────────────────────────────────────── PAGE_SIZE bytes (4096) ──────┐
//! │ verbatim copy of the dirty page (with its CRC32 already stamped)        │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! The header is 512 bytes (one disk sector) so the slot boundary is always
//! sector-aligned.  The file path is stored directly in the header so that
//! [`DoubleWriteBuffer::recover_torn_pages`] is **self-contained**: it does not
//! need the catalog or a running `PageStore` to know which real file to repair.

use std::{
    fs::File,
    io::{self, Cursor, Read, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use thiserror::Error;

use crate::{
    PAGE_SIZE,
    codec::{CodecError, Decode, Encode, ReadLeExt, WriteLeExt},
    primitives::PageId,
    storage::{self, page_crc_valid},
};

/// Size of the metadata prefix stored before every page copy in the DWB file.
/// 512 bytes = one disk sector, keeping every slot sector-aligned.
/// Total bytes for one slot: the 512-byte slot header plus one page-worth of data.
pub const SLOT_SIZE: usize = SlotHeader::SIZE + PAGE_SIZE;

#[derive(Debug, Error)]
pub enum DwbError {
    #[error("I/O error in double-write buffer: {0}")]
    Io(#[from] io::Error),

    #[error("double-write buffer is full: all {0} slots are occupied")]
    BufferFull(usize),

    #[error("file path is too long for double-write buffer ({got} bytes, max {max})")]
    PathTooLong { got: usize, max: usize },

    #[error("codec error in double-write buffer: {0}")]
    Codec(#[from] CodecError),
}

/// On-disk header for one double-write-buffer slot.
///
/// On-disk header for one double-write-buffer slot.
///
/// Encodes and decodes to exactly [`SlotHeader::SIZE`] bytes so that every
/// slot in `double_write.bin` stays sector-aligned regardless of the path
/// stored inside.  The layout mirrors the diagram in the module doc:
///
/// ```text
/// occupied (1) + pad (7) + file_id (8) + page_no (4) +
/// path_len (4) + path (256) + reserved (232) = 512 bytes
/// ```
#[derive(Debug)]
struct SlotHeader {
    /// `true` when this slot holds a page pending a real write.
    occupied: bool,
    /// Identifies the source page (file + page number).
    page_id: PageId,
    /// Absolute filesystem path to the real heap file.
    path: PathBuf,
}

impl SlotHeader {
    /// Encoded size in bytes (one disk sector), keeping every slot sector-aligned.
    pub const SIZE: usize = 512;

    /// Maximum byte length of a file path that fits inside a slot header.
    pub const MAX_PATH_BYTES: usize = 256;

    /// Reserved tail bytes that pad the header out to [`Self::SIZE`].
    ///
    /// Layout: 1 + 7 + 8 + 4 + 4 + 256 + 232 = 512
    const RESERVED: usize = 232;

    /// On-disk `occupied` byte when the slot has no pending page copy.
    const OCCUPIED_FREE: u8 = 0;
    /// On-disk `occupied` byte when a page is written but not yet flushed to the real file.
    const OCCUPIED_SET: u8 = 1;

    const _SIZE_CHECK: () = assert!(
        1 + 7 + 8 + 4 + 4 + Self::MAX_PATH_BYTES + Self::RESERVED == Self::SIZE,
        "SlotHeader field sizes must sum to SlotHeader::SIZE",
    );
}

impl Encode for SlotHeader {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_u8(if self.occupied {
            Self::OCCUPIED_SET
        } else {
            Self::OCCUPIED_FREE
        })?;
        writer.write_all(&[0u8; 7])?;

        self.page_id.encode(writer)?;

        // path_len (4) + zero-padded path bytes (256)
        let path_bytes = self.path.as_os_str().as_encoded_bytes();
        let path_len = path_bytes.len().min(Self::MAX_PATH_BYTES);
        let path_len_u32 = u32::try_from(path_len).map_err(|_| CodecError::NumericDoesNotFit {
            value: u64::try_from(path_len).unwrap_or(u64::MAX),
            target: "u32",
        })?;
        writer.write_le_u32(path_len_u32)?;

        let mut path_buf = [0u8; Self::MAX_PATH_BYTES];
        path_buf[..path_len].copy_from_slice(&path_bytes[..path_len]);
        writer.write_all(&path_buf)?;

        writer.write_all(&[0u8; Self::RESERVED])?;
        Ok(())
    }
}

impl Decode for SlotHeader {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        // occupied (1) + alignment pad (7)
        let occupied = reader.read_u8()? == Self::OCCUPIED_SET;
        let mut pad = [0u8; 7];
        reader.read_exact(&mut pad)?;

        let page_id = PageId::decode(reader)?;

        let path_len = (reader.read_le_u32()? as usize).min(Self::MAX_PATH_BYTES);
        let mut path_buf = [0u8; Self::MAX_PATH_BYTES];
        reader.read_exact(&mut path_buf)?;
        let path_str = std::str::from_utf8(&path_buf[..path_len])?;
        let path = PathBuf::from(path_str);

        // skip reserved tail
        let mut reserved = [0u8; Self::RESERVED];
        reader.read_exact(&mut reserved)?;

        Ok(Self {
            occupied,
            page_id,
            path,
        })
    }
}

/// A redundant write buffer that guards against torn pages on power failure.
///
/// `DoubleWriteBuffer` wraps a single file (`double_write.bin`) divided into
/// fixed-size *slots*.  Each slot holds one page copy plus enough metadata
/// to locate the real page on disk.
///
/// All page types (heap, B-tree, hash) share the same 5-byte on-disk
/// envelope — `PageKind` byte + CRC32 at offset 1..5 — so the DWB can
/// verify any page with the shared `page_crc_valid` helper in `storage::envelope`.
pub struct DoubleWriteBuffer {
    file: File,
    capacity: usize,
    /// In-memory shadow of which slots are currently occupied.
    /// Rebuilt from the on-disk headers inside [`DoubleWriteBuffer::open`].
    used: Vec<bool>,
}

impl DoubleWriteBuffer {
    /// Opens (or creates) the DWB file at `path` with space for `capacity` slots.
    ///
    /// If the file already exists its headers are scanned to rebuild the
    /// in-memory `used` bitmap — occupied slots from a previous run will be
    /// repaired by [`Self::recover_torn_pages`].
    ///
    /// # Errors
    ///
    /// Returns [`DwbError::Io`] if the file cannot be opened or pre-allocated.
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> Result<Self, DwbError> {
        let file = storage::open_persistent_file(path)?;
        let needed = (capacity * SLOT_SIZE) as u64;
        if file.metadata()?.len() < needed {
            file.set_len(needed)?;
        }

        // Read the occupied flags from the file into the in-memory bitmap.
        // The occupied flag is always the very first byte of a slot.
        let used = (0..capacity)
            .map(|slot| {
                let mut occupied_byte = [0u8; 1];
                file.read_at(&mut occupied_byte, Self::slot_offset(slot))?;
                Ok(occupied_byte[0] == SlotHeader::OCCUPIED_SET)
            })
            .collect::<Result<Vec<bool>, DwbError>>()?;

        Ok(Self {
            file,
            capacity,
            used,
        })
    }

    /// Copies `page` into the next free slot and fsyncs the DWB file.
    ///
    /// `file_id` and `page_no` identify the source page; `path` is the
    /// absolute filesystem path to the real heap file.  All three are written
    /// into the slot header so that [`Self::recover_torn_pages`] is self-contained.
    ///
    /// The caller must call [`Self::release`] with the returned slot index **after**
    /// the real heap-file write completes — that marks the slot available for
    /// reuse and prevents spurious repair on the next startup.
    ///
    /// # Errors
    ///
    /// - [`DwbError::BufferFull`] — no free slot exists; increase `capacity`.
    /// - [`DwbError::PathTooLong`] — `path` exceeds 256 bytes.
    /// - [`DwbError::Io`] — any I/O failure.
    pub fn write_page(
        &mut self,
        page: &[u8; PAGE_SIZE],
        page_id: PageId,
        path: &Path,
    ) -> Result<usize, DwbError> {
        let path_bytes = path.as_os_str().as_encoded_bytes();
        if path_bytes.len() > SlotHeader::MAX_PATH_BYTES {
            return Err(DwbError::PathTooLong {
                got: path_bytes.len(),
                max: SlotHeader::MAX_PATH_BYTES,
            });
        }

        let slot_pos = self
            .used
            .iter()
            .position(|&used| !used)
            .ok_or(DwbError::BufferFull(self.capacity))?;

        let header = SlotHeader {
            occupied: true,
            page_id,
            path: path.to_path_buf(),
        };
        let mut header_bytes = [0u8; SlotHeader::SIZE];
        header.encode(&mut Cursor::new(&mut header_bytes[..]))?;

        let slot_offset = Self::slot_offset(slot_pos);
        self.file.write_at(&header_bytes, slot_offset)?;
        self.file
            .write_at(page, slot_offset + SlotHeader::SIZE as u64)?;
        self.file.sync_all()?;

        self.used[slot_pos] = true;
        Ok(slot_pos)
    }

    /// Marks a slot as free after the real page write has completed.
    ///
    /// Clears the `occupied` byte on disk and updates the in-memory bitmap.
    /// The fsync is best-effort: if it fails the slot will be re-examined on
    /// the next startup and found to contain a page whose real copy already has
    /// a valid checksum, so it will be released harmlessly.
    ///
    /// # Errors
    ///
    /// Returns [`DwbError::Io`] if clearing the occupied byte fails.  (The
    /// subsequent `sync_all` failure is silently ignored as described above.)
    pub fn release(&mut self, slot: usize) -> Result<(), DwbError> {
        if slot >= self.capacity {
            return Ok(());
        }
        let slot_offset = Self::slot_offset(slot);
        self.file
            .write_at(&[SlotHeader::OCCUPIED_FREE], slot_offset)?;
        let _ = self.file.sync_all();
        self.used[slot] = false;
        Ok(())
    }

    /// Returns the byte offset of the given slot in the DWB file.
    #[inline]
    const fn slot_offset(slot: usize) -> u64 {
        (slot * SLOT_SIZE) as u64
    }

    pub fn recover_torn_pages(&mut self) -> Result<(), DwbError> {
        let mut header_buf = [0u8; SlotHeader::SIZE];
        let mut dwb_page = [0u8; PAGE_SIZE];

        for slot in 0..self.capacity {
            if !self.used[slot] {
                continue;
            }

            let slot_offset = Self::slot_offset(slot);
            self.file.read_at(&mut header_buf, slot_offset)?;

            let header = match SlotHeader::decode(&mut Cursor::new(&header_buf[..])) {
                Ok(h) => h,
                Err(CodecError::InvalidUtf8(_)) => {
                    tracing::warn!(slot, "DWB slot has non-UTF-8 path — skipping");
                    self.release(slot)?;
                    continue;
                }
                Err(e) => return Err(DwbError::Codec(e)),
            };

            if !header.occupied {
                self.used[slot] = false;
                continue;
            }

            let page_offset = slot_offset + SlotHeader::SIZE as u64;
            self.file.read_at(&mut dwb_page, page_offset)?;
            self.check_and_repair_torn_page(slot, &header, &dwb_page)?;
        }

        Ok(())
    }

    /// Opens the real heap file, compares the on-disk page to `dwb_page`, and
    /// overwrites it when the CRC32 check fails (torn page).
    ///
    /// A fully-zeroed real page is treated as freshly allocated and left alone.
    /// Releases the DWB slot on success. Logs at `warn` when a repair is
    /// performed, `debug` when the real page is already intact, and `error`
    /// before returning an I/O failure.
    fn check_and_repair_torn_page(
        &mut self,
        slot: usize,
        header: &SlotHeader,
        dwb_page: &[u8; PAGE_SIZE],
    ) -> Result<(), DwbError> {
        let path = &header.path;
        let PageId { page_no, .. } = header.page_id;
        let real_offset = u64::from(page_no) * PAGE_SIZE as u64;

        let repair_result = (|| -> Result<bool, DwbError> {
            let file = File::options().read(true).write(true).open(path)?;
            let mut real_page = [0u8; PAGE_SIZE];
            file.read_at(&mut real_page, real_offset)?;

            // Freshly allocated — never checksummed; the DWB write happened before
            // the first real write reached this page, so it is not torn.
            if real_page.iter().all(|&b| b == 0) || page_crc_valid(&real_page) {
                return Ok(false);
            }

            file.write_at(dwb_page, real_offset)?;
            file.sync_all()?;
            Ok(true)
        })();

        match repair_result {
            Ok(repaired) => {
                if repaired {
                    tracing::warn!(
                        slot,
                        page_no = page_no.get(),
                        path = %path.display(),
                        "torn page detected and repaired from double-write buffer",
                    );
                } else {
                    tracing::debug!(
                        slot,
                        page_no = page_no.get(),
                        path = %path.display(),
                        "real page intact — releasing DWB slot",
                    );
                }
                self.release(slot)
            }
            Err(e) => {
                tracing::error!(
                    slot,
                    page_no = page_no.get(),
                    path = %path.display(),
                    error = %e,
                    "failed to repair torn page — storage may be damaged",
                );
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::FileExt};

    use tempfile::tempdir;

    use super::*;
    use crate::{
        FileId,
        primitives::{PageId, PageNumber},
        storage::{PageKind, page_crc_valid, stamp_page_crc},
    };

    fn fid(n: u64) -> FileId {
        FileId::new(n)
    }
    fn pno(n: u32) -> PageNumber {
        PageNumber::new(n)
    }

    /// Build a page with a valid CRC32 stamped in the standard envelope format
    /// (kind byte at 0, CRC at 1..5). `seed` fills the non-header bytes so
    /// each call produces a distinct, non-blank page for identity checks.
    fn valid_page(seed: u8) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        page[0] = PageKind::Heap as u8;
        // Put a non-zero byte outside the header so it isn't treated
        // as "blank" by the DWB recovery logic.
        page[PAGE_SIZE - 1] = seed;
        stamp_page_crc(&mut page);
        page
    }

    /// Returns a page that fails the CRC32 check (simulates a torn page).
    fn torn_page(seed: u8) -> [u8; PAGE_SIZE] {
        let mut p = valid_page(seed);
        p[PAGE_SIZE - 1] ^= 0xFF; // flip a bit without re-stamping the checksum
        p
    }

    #[test]
    fn open_creates_file_of_correct_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dwb.bin");
        let capacity = 4;
        DoubleWriteBuffer::open(&path, capacity).unwrap();
        let len = fs::metadata(&path).unwrap().len();
        assert_eq!(len, (capacity * SLOT_SIZE) as u64);
    }

    #[test]
    fn open_is_idempotent_for_existing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dwb.bin");
        DoubleWriteBuffer::open(&path, 4).unwrap();
        // Second open must not truncate or error.
        DoubleWriteBuffer::open(&path, 4).unwrap();
    }

    // ── write_page / release ───────────────────────────────────────────────

    #[test]
    fn write_page_returns_a_slot_index() {
        let dir = tempdir().unwrap();
        let heap = dir.path().join("heap.db");
        fs::write(&heap, vec![0u8; PAGE_SIZE]).unwrap();

        let mut dwb = DoubleWriteBuffer::open(dir.path().join("dwb.bin"), 4).unwrap();
        let page = valid_page(1);
        let slot = dwb
            .write_page(&page, PageId::new(fid(1), pno(0)), &heap)
            .unwrap();
        assert!(slot < 4, "slot {slot} must be within capacity");
    }

    #[test]
    fn write_page_marks_slot_occupied_on_disk() {
        let dir = tempdir().unwrap();
        let heap = dir.path().join("heap.db");
        fs::write(&heap, vec![0u8; PAGE_SIZE]).unwrap();
        let dwb_path = dir.path().join("dwb.bin");

        let mut dwb = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        let page = valid_page(1);
        let slot = dwb
            .write_page(&page, PageId::new(fid(1), pno(0)), &heap)
            .unwrap();

        // Re-open and check the occupied byte without using the in-memory bitmap.
        let raw = fs::read(&dwb_path).unwrap();
        let slot_start = usize::try_from(DoubleWriteBuffer::slot_offset(slot))
            .expect("test slot offset fits in usize");
        assert_eq!(
            raw[slot_start], // occupied byte is always the first byte of a slot header
            SlotHeader::OCCUPIED_SET,
            "occupied byte must be set"
        );
    }

    #[test]
    fn release_clears_slot_on_disk() {
        let dir = tempdir().unwrap();
        let heap = dir.path().join("heap.db");
        fs::write(&heap, vec![0u8; PAGE_SIZE]).unwrap();
        let dwb_path = dir.path().join("dwb.bin");

        let mut dwb = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        let page = valid_page(1);
        let slot = dwb
            .write_page(&page, PageId::new(fid(1), pno(0)), &heap)
            .unwrap();
        dwb.release(slot).unwrap();

        let raw = fs::read(&dwb_path).unwrap();
        let slot_start = usize::try_from(DoubleWriteBuffer::slot_offset(slot))
            .expect("test slot offset fits in usize");
        assert_eq!(
            raw[slot_start], // occupied byte is always first
            SlotHeader::OCCUPIED_FREE,
            "occupied byte must be cleared after release"
        );
    }

    #[test]
    fn buffer_full_error_when_all_slots_used() {
        let dir = tempdir().unwrap();
        let heap = dir.path().join("heap.db");
        fs::write(&heap, vec![0u8; PAGE_SIZE * 4]).unwrap();

        let mut dwb = DoubleWriteBuffer::open(dir.path().join("dwb.bin"), 2).unwrap();
        let page = valid_page(1);

        dwb.write_page(&page, PageId::new(fid(1), pno(0)), &heap)
            .unwrap();
        dwb.write_page(&page, PageId::new(fid(1), pno(1)), &heap)
            .unwrap();

        let err = dwb
            .write_page(&page, PageId::new(fid(1), pno(2)), &heap)
            .unwrap_err();
        assert!(
            matches!(err, DwbError::BufferFull(_)),
            "expected BufferFull, got {err:?}"
        );
    }

    #[test]
    fn released_slot_can_be_reused() {
        let dir = tempdir().unwrap();
        let heap = dir.path().join("heap.db");
        fs::write(&heap, vec![0u8; PAGE_SIZE * 2]).unwrap();

        // capacity = 1: write, release, write again — must succeed.
        let mut dwb = DoubleWriteBuffer::open(dir.path().join("dwb.bin"), 1).unwrap();
        let page = valid_page(1);
        let slot = dwb
            .write_page(&page, PageId::new(fid(1), pno(0)), &heap)
            .unwrap();
        dwb.release(slot).unwrap();
        let slot2 = dwb
            .write_page(&page, PageId::new(fid(1), pno(1)), &heap)
            .unwrap();
        assert_eq!(slot, slot2, "released slot should be reused");
    }

    /// Happy path: the real write completed before the crash.
    /// The real page has a valid checksum — recovery must skip it and release
    /// the slot without overwriting anything.
    #[test]
    fn recovery_skips_intact_real_page() {
        let dir = tempdir().unwrap();
        let heap = dir.path().join("heap.db");
        let dwb_path = dir.path().join("dwb.bin");

        let page = valid_page(42);

        // Pre-write a good real page.
        fs::write(&heap, &page[..]).unwrap();
        // Simulate: DWB slot still occupied (crash before release).
        let mut dwb = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        dwb.write_page(&page, PageId::new(fid(1), pno(0)), &heap)
            .unwrap();
        // Deliberately do NOT call release — simulates a crash before step 5.

        // Re-open as if restarting the database.
        let mut dwb2 = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        dwb2.recover_torn_pages().unwrap();

        // The slot must be free now (released by recovery).
        assert!(
            !dwb2.used.iter().any(|&u| u),
            "all slots must be free after recovery"
        );

        // The real page must be unchanged.
        let on_disk = fs::read(&heap).unwrap();
        assert_eq!(
            &on_disk[..PAGE_SIZE],
            &page[..],
            "intact page must not be overwritten"
        );
    }

    /// Torn-page path: the real write was interrupted mid-page.
    /// Recovery must detect the bad checksum and restore the page from the DWB.
    #[test]
    fn recovery_repairs_torn_real_page() {
        let dir = tempdir().unwrap();
        let heap = dir.path().join("heap.db");
        let dwb_path = dir.path().join("dwb.bin");

        let good_page = valid_page(7);

        // Write the good page to the DWB, but put a torn page at the real location.
        let mut dwb = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        dwb.write_page(&good_page, PageId::new(fid(1), pno(0)), &heap)
            .unwrap();

        // Simulate a torn write: create a real file with bad bytes.
        let torn = torn_page(7);
        // Make sure the path exists with the right size.
        fs::write(&heap, &torn[..]).unwrap();
        // Confirm it actually fails the checksum.
        assert!(!page_crc_valid(&torn));

        // Recovery.
        let mut dwb2 = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        dwb2.recover_torn_pages().unwrap();

        // The slot must be released.
        assert!(!dwb2.used.iter().any(|&u| u));

        // The real page must now equal the DWB copy (good page).
        let mut on_disk = [0u8; PAGE_SIZE];
        File::open(&heap).unwrap().read_at(&mut on_disk, 0).unwrap();
        assert_eq!(on_disk, good_page, "torn page must be restored from DWB");
    }

    /// Blank-page path: the real page is all zeros (freshly allocated, never
    /// written by a WAL record).  Recovery must not try to overwrite it.
    #[test]
    fn recovery_skips_blank_real_page() {
        let dir = tempdir().unwrap();
        let heap = dir.path().join("heap.db");
        let dwb_path = dir.path().join("dwb.bin");

        let page = valid_page(99);
        // Real file is entirely zero (blank / never written).
        fs::write(&heap, vec![0u8; PAGE_SIZE]).unwrap();

        let mut dwb = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        dwb.write_page(&page, PageId::new(fid(1), pno(0)), &heap)
            .unwrap();

        let mut dwb2 = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        dwb2.recover_torn_pages().unwrap();

        // Real page must still be zeros.
        let on_disk = fs::read(&heap).unwrap();
        assert!(
            on_disk.iter().all(|&b| b == 0),
            "blank page must not be overwritten during recovery"
        );
    }

    /// Simulate a crash between steps 2 and 4 (DWB written, real write never
    /// happened) with a zero real page: recovery treats it as blank and skips.
    #[test]
    fn recovery_no_op_when_real_file_is_missing_page_offset() {
        let dir = tempdir().unwrap();
        // Heap file exists but is only 0 bytes — page_no 0 at offset 0 will
        // read back as zeros (the OS zero-extends the read). This exercises the
        // blank-page shortcut in check_and_repair_torn_page.
        let heap = dir.path().join("heap.db");
        fs::write(&heap, vec![0u8; PAGE_SIZE]).unwrap();
        let dwb_path = dir.path().join("dwb.bin");

        let page = valid_page(3);
        let mut dwb = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        dwb.write_page(&page, PageId::new(fid(1), pno(0)), &heap)
            .unwrap();

        let mut dwb2 = DoubleWriteBuffer::open(&dwb_path, 4).unwrap();
        dwb2.recover_torn_pages().unwrap();
        // No panic, no error — that's the assertion.
    }
}
