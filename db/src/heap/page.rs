//! Heap page layout and management for slotted tuple storage.
//!
//! A [`HeapPage`] wraps a raw `PAGE_SIZE`-byte buffer and interprets it as a
//! bidirectional slotted page: a fixed header followed by a slot-pointer array
//! growing from the low end, and tuple data growing from the high end toward
//! the middle.
//!
//! ## In-memory layout  (one `PAGE_SIZE`-byte buffer)
//!
//! ```text
//!  byte 0                                                          PAGE_SIZE
//!  |                                                                        |
//!  +--------+------------------+---------...---------+--------------------+
//!  | header | slot pointers -> |      free space     | <- tuple data      |
//!  +--------+------------------+---------...---------+--------------------+
//!  ^        ^                  ^                     ^                    ^
//!  0    PAGE_HDR_SIZE   slot_array_end          tuple_start            PAGE_SIZE
//!
//!
//!  Fixed header (first PAGE_HDR_SIZE bytes):
//!
//!  +------- byte 0 -------+------ byte 2 --------+
//!  |   num_slots (u16 LE) |  tuple_start (u16 LE)|
//!  +----------------------+----------------------+
//!        |                        |
//!        |                        +-- left edge of the tuple region; tuples
//!        |                            occupy [tuple_start .. PAGE_SIZE)
//!        +-- number of slot pointers that follow the header
//!
//!
//!  Slot i occupies bytes [PAGE_HDR_SIZE + i*4 .. PAGE_HDR_SIZE + i*4 + 4):
//!
//!  +---- offset ---------+---- length ----------+
//!  |        (u16 LE)     |         (u16 LE)     |
//!  +---------------------+----------------------+
//!         |                      |
//!         |                      +-- 0  =>  slot is empty (deleted or unused)
//!         +-- byte position of this tuple's first byte within the page
//! ```
//!
//! The slot array grows forward as new slots are allocated; the tuple region
//! grows backward as new tuples are written. The page is full when inserting
//! would make the two regions meet. Deletion marks a slot empty (tombstone)
//! without reclaiming its bytes — the hole sits in the middle of the tuple
//! region until a future compaction pass rewrites the page.
//!
//! A freshly zeroed buffer (`num_slots = 0`, `tuple_start = 0`) is treated as
//! an empty page with `tuple_start = PAGE_SIZE`.
//!
//! The page holds a before-image copy of its raw bytes at construction time
//! so that callers can produce WAL undo records when needed.

use std::vec;

use byteorder::{ByteOrder, LittleEndian};

use crate::{
    primitives::SlotId,
    storage::{MAX_TUPLE_SIZE, PAGE_SIZE, Page, StorageError},
    tuple::{Tuple, TupleSchema},
};

/// Size of the fixed page header: `num_slots` (u16) + `tuple_start` (u16).
const PAGE_HDR_SIZE: usize = 4;

/// Number of bytes occupied by one slot pointer.
///
/// Each slot pointer is two `u16` fields: offset and length.
const SLOT_POINTER_SIZE: usize = 4;

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize, "PAGE_SIZE must fit in u16");
const _: () = assert!(
    MAX_TUPLE_SIZE <= u16::MAX as usize,
    "MAX_TUPLE_SIZE must fit in u16"
);

/// Location and size of one tuple inside a [`HeapPage`].
///
/// `offset` is the byte position of the tuple's first byte within the page
/// buffer. `length` is the number of bytes the serialized tuple occupies.
/// A slot pointer whose `length` is `0` marks an empty (deleted or never-used)
/// slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
struct SlotPointer {
    /// Byte offset of the tuple data from the start of the page buffer.
    offset: u16,
    /// Byte length of the serialized tuple. Zero means the slot is empty.
    length: u16,
}

impl SlotPointer {
    /// Returns `true` if the slot pointer marks the slot as empty (tombstone).
    ///
    /// In the slotted page design, a slot is considered empty (either never
    /// used or deleted) when its `length` field is zero. This function allows
    /// slot scanning and tuple management code to efficiently skip over
    /// unused or deleted slots.
    #[inline]
    pub(super) fn is_tombstone(self) -> bool {
        self.length == 0
    }
}

/// A fixed-size page that stores tuples according to a given schema.
///
/// `HeapPage` manages a `PAGE_SIZE`-byte region of storage as a bidirectional
/// slotted page. The slot-pointer array grows forward from the end of the
/// header; tuple data grows backward from the end of the buffer. The number
/// of slots is persisted in the page header, so slots can be added
/// dynamically as tuples are inserted.
///
/// The page is parameterized by a schema lifetime `'a`; the [`TupleSchema`]
/// must outlive the page.
pub struct HeapPage<'a> {
    schema: &'a TupleSchema,
    tuples: Vec<Option<Tuple>>,
    slot_pointers: Vec<SlotPointer>,
    /// Left edge of the tuple region. Tuples occupy `[tuple_start .. PAGE_SIZE)`.
    /// Shrinks on insert; unchanged on delete (holes are left in place until
    /// a future compaction rewrites the page).
    tuple_start: usize,
    old_data: [u8; PAGE_SIZE],
}

impl<'a> HeapPage<'a> {
    /// Loads a heap page from a raw byte slice, decoding the page header, slot
    /// pointers, and stored tuples.
    ///
    /// `data` must be exactly `PAGE_SIZE` bytes. The first [`PAGE_HDR_SIZE`]
    /// bytes hold `num_slots` and `tuple_start`; the next
    /// `num_slots × SLOT_POINTER_SIZE` bytes are interpreted as slot pointers;
    /// the tuple bytes they reference are deserialized according to `schema`.
    ///
    /// A freshly zeroed buffer (e.g. `[0u8; PAGE_SIZE]`) produces an empty
    /// page ready for inserts: `num_slots = 0`, `tuple_start = PAGE_SIZE`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidPageSize`] if `data.len() != PAGE_SIZE`.
    ///
    /// Returns [`StorageError::ParseError`] if the header, any slot pointer,
    /// or any tuple data is malformed or out of bounds.
    pub fn new(data: &[u8], schema: &'a TupleSchema) -> Result<Self, StorageError> {
        if data.len() != PAGE_SIZE {
            return Err(StorageError::InvalidPageSize { got: data.len() });
        }

        let num_slots = LittleEndian::read_u16(&data[0..]) as usize;
        let stored_tuple_start = LittleEndian::read_u16(&data[2..]) as usize;

        let tuple_start = if num_slots == 0 && stored_tuple_start == 0 {
            PAGE_SIZE
        } else {
            stored_tuple_start
        };

        let slot_end = Self::slot_array_end(num_slots);
        if tuple_start < slot_end || tuple_start > PAGE_SIZE {
            return Err(StorageError::ParseError(format!(
                "invalid tuple_start={tuple_start}, slot_end={slot_end}"
            )));
        }

        let mut hp = Self {
            schema,
            old_data: data.try_into().unwrap(),
            tuples: vec![None; num_slots],
            slot_pointers: vec![SlotPointer::default(); num_slots],
            tuple_start,
        };

        hp.parse_data(data)?;
        Ok(hp)
    }

    /// End byte (exclusive) of the slot-pointer array given a slot count.
    #[inline]
    fn slot_array_end(n_slots: usize) -> usize {
        PAGE_HDR_SIZE + n_slots * SLOT_POINTER_SIZE
    }

    /// Bytes currently free between the end of the slot array and the start
    /// of the tuple region.
    ///
    /// Saturates to 0 on underflow — a "full" page reads as 0, never panics.
    #[inline]
    fn free_bytes(&self) -> usize {
        self.tuple_start
            .saturating_sub(Self::slot_array_end(self.slot_pointers.len()))
    }

    /// Current number of slots on this page (both occupied and empty).
    #[inline]
    fn num_slots(&self) -> u16 {
        u16::try_from(self.slot_pointers.len()).unwrap_or(u16::MAX)
    }

    /// Checks that `slot_id` refers to an existing slot on this page.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::SlotOutOfBounds`] when `slot_id >= num_slots`.
    fn check_slot_bounds(&self, slot_id: SlotId) -> Result<(), StorageError> {
        let n = self.num_slots();
        if u16::from(slot_id) >= n {
            return Err(StorageError::slot_out_of_bounds(slot_id, n));
        }
        Ok(())
    }

    /// Returns an iterator over every occupied slot, yielding `(SlotId, &Tuple)` pairs.
    ///
    /// Empty (deleted) slots are skipped. The order of iteration follows slot
    /// number from lowest to highest.
    pub(crate) fn live_tuples(&self) -> impl Iterator<Item = (SlotId, &Tuple)> {
        self.tuples.iter().enumerate().filter_map(|(i, slot)| {
            let slot_id = u16::try_from(i).ok()?;
            slot.as_ref().map(|tuple| (SlotId(slot_id), tuple))
        })
    }

    /// Returns the number of existing slots that currently hold no tuple.
    ///
    /// These are tombstoned slots that can be reused by a future insert
    /// without growing the slot array. This is *not* the total remaining
    /// capacity of the page — for that see [`HeapPage::remaining_capacity`].
    pub fn empty_slots(&self) -> usize {
        self.slot_pointers
            .iter()
            .filter(|sp| sp.is_tombstone())
            .count()
    }

    /// Returns the number of tuples that can be inserted into this page
    /// before running out of space, taking into account both reusable (empty)
    /// slots and the space required for new slot entries.
    ///
    /// - First, reuses space from any existing empty (tombstoned) slots; reusing such a slot only
    ///   needs to allocate tuple space.
    /// - Any remaining space after that must be split for both a new slot entry
    ///   (`SLOT_POINTER_SIZE`) and the tuple blob.
    ///
    /// Returns 0 if the tuple size is 0 (should not occur in real use).
    pub fn remaining_capacity(&self) -> usize {
        let tup = self.schema.serialized_size();
        if tup == 0 {
            return 0;
        }

        let free = self.free_bytes();
        let empty = self.empty_slots();

        let reuse = empty.min(free / tup);
        let free_after_reuse = free - reuse * tup;

        let grow = free_after_reuse / (tup + SLOT_POINTER_SIZE);
        reuse + grow
    }

    /// Writes `tuple` into an available slot and returns its [`SlotId`].
    ///
    /// An empty tombstoned slot is reused if one exists; otherwise a new slot
    /// is appended to the slot array. The tuple bytes are placed at the high
    /// end of the free region, and `tuple_start` is moved downward.
    ///
    /// Call [`Page::page_data`] to obtain the updated raw bytes for flushing
    /// to disk.
    ///
    /// # Errors
    ///
    /// - [`StorageError::SchemaMismatch`] — tuple does not match this page's schema.
    /// - [`StorageError::PageFull`] — the slot array and tuple region cannot both grow to
    ///   accommodate this tuple.
    /// - [`StorageError::TupleTooLarge`] — the serialized tuple exceeds `MAX_TUPLE_SIZE`.
    pub(crate) fn insert_tuple(&mut self, tuple: Tuple) -> Result<SlotId, StorageError> {
        self.schema
            .validate(&tuple)
            .map_err(|_| StorageError::SchemaMismatch)?;

        let tup_size = self.schema.serialized_size();
        if tup_size > MAX_TUPLE_SIZE {
            return Err(StorageError::TupleTooLarge {
                size: tup_size,
                max: MAX_TUPLE_SIZE,
            });
        }

        let reused = self.slot_pointers.iter().position(|sp| sp.is_tombstone());
        let needs_new_slot = reused.is_none();

        let slot_growth = if needs_new_slot { SLOT_POINTER_SIZE } else { 0 };
        let new_slot_end = Self::slot_array_end(self.slot_pointers.len()) + slot_growth;
        let new_tuple_start = self
            .tuple_start
            .checked_sub(tup_size)
            .ok_or(StorageError::PageFull)?;
        if new_slot_end > new_tuple_start {
            return Err(StorageError::PageFull);
        }

        self.tuple_start = new_tuple_start;
        let offset = u16::try_from(new_tuple_start)
            .map_err(|_| StorageError::ParseError("tuple_start overflowed u16".to_string()))?;
        let length =
            u16::try_from(tup_size).map_err(|_| StorageError::tuple_too_large(tup_size))?;
        let sp = SlotPointer { offset, length };

        let slot_index = if let Some(i) = reused {
            self.slot_pointers[i] = sp;
            self.tuples[i] = Some(tuple);
            i
        } else {
            self.slot_pointers.push(sp);
            self.tuples.push(Some(tuple));
            self.slot_pointers.len() - 1
        };

        u16::try_from(slot_index)
            .map(SlotId)
            .map_err(|_| StorageError::SlotOutOfBounds {
                slot: u16::MAX,
                num_slots: self.num_slots(),
            })
    }

    /// Marks the tuple at `slot_id` as deleted by zeroing its slot pointer.
    ///
    /// The slot becomes available for future inserts. The tuple bytes in the
    /// page buffer are not zeroed out and `tuple_start` is not moved — the
    /// hole sits in the tuple region until a future compaction rewrites the
    /// page. New inserts go to a fresh position at `tuple_start - size`.
    ///
    /// # Errors
    ///
    /// - [`StorageError::SlotOutOfBounds`] — `slot_id` is beyond the last slot.
    /// - [`StorageError::SlotAlreadyEmpty`] — the slot is already empty.
    pub(crate) fn delete_tuple(&mut self, slot_id: SlotId) -> Result<(), StorageError> {
        self.check_slot_bounds(slot_id)?;

        if self.is_slot_empty(slot_id)? {
            return Err(StorageError::slot_already_empty(slot_id));
        }

        let index = usize::from(slot_id);
        self.slot_pointers[index] = SlotPointer::default();
        self.tuples[index] = None;
        Ok(())
    }

    /// Returns `true` if the slot at `slot_id` holds no tuple.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::SlotOutOfBounds`] if `slot_id` is out of range.
    fn is_slot_empty(&self, slot_id: SlotId) -> Result<bool, StorageError> {
        self.check_slot_bounds(slot_id)?;
        Ok(self.slot_pointers[usize::from(slot_id)].is_tombstone())
    }

    /// Reads all slot pointers and tuple data from the raw `data` buffer into
    /// `self`.
    ///
    /// Called once from [`HeapPage::new`] after the struct is initialized.
    /// `self.tuple_start` must already be set from the page header.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::ParseError`] if a slot pointer or its
    /// referenced tuple data falls outside `data` or the tuple region.
    fn parse_data(&mut self, data: &[u8]) -> Result<(), StorageError> {
        for i in 0..self.slot_pointers.len() {
            let off = PAGE_HDR_SIZE + i * SLOT_POINTER_SIZE;
            if off + SLOT_POINTER_SIZE > data.len() {
                return Err(StorageError::ParseError(format!(
                    "slot pointer {i} out of bounds",
                )));
            }

            let offset = LittleEndian::read_u16(&data[off..]);
            let length = LittleEndian::read_u16(&data[off + 2..]);
            self.slot_pointers[i] = SlotPointer { offset, length };
            self.tuples[i] = self.parse_tuple(data, i)?;
        }

        Ok(())
    }

    /// Deserializes the tuple for `slot_index` using [`Self::slot_pointers`]
    /// and the raw page bytes in `data`.
    ///
    /// An empty slot (`length == 0`) yields [`None`]. Otherwise the slot
    /// pointer's `[offset, offset + length)` range must lie within `data` and
    /// must not start before [`Self::tuple_start`] (tuple bytes live in the
    /// lower-address region of the page).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::ParseError`] if the slot byte range overflows,
    /// extends past `data`, starts before the tuple region, or if
    /// [`Tuple::deserialize`] fails for the slice.
    fn parse_tuple(&self, data: &[u8], slot_index: usize) -> Result<Option<Tuple>, StorageError> {
        let slot = self.slot_pointers[slot_index];

        if slot.is_tombstone() {
            return Ok(None);
        }

        let start = slot.offset as usize;
        let end = start
            .checked_add(slot.length as usize)
            .ok_or_else(|| StorageError::ParseError("slot range overflow".to_string()))?;

        if end > data.len() || start < self.tuple_start {
            return Err(StorageError::ParseError(format!(
                "tuple data for slot {slot_index} out of range [{start},{end})"
            )));
        }

        let tuple_data = &data[start..end];
        Tuple::deserialize(self.schema, tuple_data)
            .map(Some)
            .map_err(|e| StorageError::ParseError(format!("failed to parse tuple: {e}")))
    }

    /// Marks multiple slots as deleted by zeroing their slot pointers.
    ///
    /// The slots become available for future inserts. The tuple bytes in the
    /// page buffer are not zeroed out, but they will be overwritten on the next
    /// insert that claims the slot.
    ///
    /// # Errors
    ///
    /// - [`StorageError::SlotOutOfBounds`] — any of the `slots` is out of bounds.
    /// - [`StorageError::SlotAlreadyEmpty`] — any of the `slots` is already empty.
    ///
    /// # Returns
    ///
    /// The number of slots that were deleted.
    pub fn delete_many(&mut self, slots: &[SlotId]) -> Result<u32, StorageError> {
        let mut count = 0u32;
        for &slot_id in slots {
            self.delete_tuple(slot_id)?;
            count += 1;
        }
        Ok(count)
    }

    /// Inserts multiple tuples into available slots and returns their [`SlotId`]s.
    ///
    /// Takes at most [`HeapPage::remaining_capacity`] tuples from the iterator,
    /// so the caller can keep using the iterator for any tuples that didn't fit.
    ///
    /// # Errors
    ///
    /// - [`StorageError::SchemaMismatch`] — any of the `tuples` does not match this page's schema.
    /// - [`StorageError::PageFull`] — insert fails mid-batch due to space.
    /// - [`StorageError::TupleTooLarge`] — the serialized tuple exceeds `MAX_TUPLE_SIZE`.
    pub fn insert_many<I>(&mut self, tuples: &mut I) -> Result<Vec<SlotId>, StorageError>
    where
        I: Iterator<Item = Tuple>,
    {
        let mut inserted = Vec::new();
        let cap = self.remaining_capacity();
        for tuple in tuples.take(cap) {
            let slot_id = self.insert_tuple(tuple)?;
            inserted.push(slot_id);
        }
        Ok(inserted)
    }

    /// Compacts the page by rewriting live tuples to remove holes and recover space.
    ///
    /// Iterates over the slot pointers and corresponding tuple entries, moving all
    /// present (live) tuples to the lowest contiguous region at the end of the page buffer.
    /// Empty slots are zeroed out (set to tombstone).
    ///
    /// This operation updates slot pointers and `tuple_start` so that all live tuples
    /// are packed tightly, making free space available for future inserts.
    ///
    /// # Errors
    ///
    /// - [`StorageError::ParseError`] if moving tuples would underflow or overflow the offset.
    pub fn compact(&mut self) -> Result<(), StorageError> {
        let mut cursor = PAGE_SIZE;
        for (sp, tup) in self.slot_pointers.iter_mut().zip(self.tuples.iter()) {
            if tup.is_none() {
                *sp = SlotPointer::default();
                continue;
            }
            let len = sp.length as usize;
            cursor = cursor
                .checked_sub(len)
                .ok_or_else(|| StorageError::ParseError("compact underflow".into()))?;

            sp.offset = u16::try_from(cursor)
                .map_err(|_| StorageError::ParseError("offset overflow".to_string()))?;
        }
        self.tuple_start = cursor;
        Ok(())
    }
}

impl Page for HeapPage<'_> {
    /// Serializes the current page state into a fresh `PAGE_SIZE`-byte array.
    ///
    /// Writes the page header (`num_slots`, `tuple_start`), the slot-pointer
    /// array, and every live tuple at the byte range its slot pointer
    /// describes.
    ///
    /// # Panics
    ///
    /// Panics if serializing any tuple fails — this should not happen for
    /// tuples that were already validated on insert.
    fn page_data(&self) -> [u8; PAGE_SIZE] {
        let mut bytes = [0u8; PAGE_SIZE];
        let mut write_u16 = |offset: usize, values: &[u16]| {
            for (i, value) in values.iter().enumerate() {
                LittleEndian::write_u16(&mut bytes[offset + i * 2..], *value);
            }
        };

        write_u16(0, &[
            self.num_slots(),
            u16::try_from(self.tuple_start).expect("tuple_start <= PAGE_SIZE <= u16::MAX"),
        ]);

        self.slot_pointers.iter().copied().enumerate().for_each(
            |(i, SlotPointer { offset, length })| {
                write_u16(PAGE_HDR_SIZE + i * SLOT_POINTER_SIZE, &[offset, length]);
            },
        );

        for (sp, tup) in self.slot_pointers.iter().zip(self.tuples.iter()) {
            if let Some(t) = tup {
                let start = usize::from(sp.offset);
                let end = start + usize::from(sp.length);
                t.serialize(self.schema, &mut bytes[start..end])
                    .expect("failed to serialize tuple");
            }
        }

        bytes
    }

    /// Returns the raw bytes the page had when it was first constructed.
    ///
    /// Used as the WAL before-image for undo logging.
    fn before_image(&self) -> Option<[u8; PAGE_SIZE]> {
        self.old_data.into()
    }

    /// Updates the before-image to the current page state.
    fn set_before_image(&mut self) {
        self.old_data = self.page_data();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tuple::{Field, TupleSchema},
        types::{Type, Value},
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

    fn empty_page(schema: &TupleSchema) -> HeapPage<'_> {
        HeapPage::new(&[0u8; PAGE_SIZE], schema).unwrap()
    }

    #[test]
    fn new_empty_page_has_no_tuples() {
        let s = schema();
        let page = empty_page(&s);
        assert_eq!(page.live_tuples().count(), 0);
    }

    #[test]
    fn new_empty_page_has_zero_slots() {
        let s = schema();
        let page = empty_page(&s);
        assert_eq!(page.num_slots(), 0);
        assert_eq!(page.tuple_start, PAGE_SIZE);
    }

    #[test]
    fn new_rejects_wrong_size_data() {
        let s = schema();
        let result = HeapPage::new(&[0u8; 100], &s);
        assert!(matches!(result, Err(StorageError::InvalidPageSize { .. })));
    }

    #[test]
    fn insert_returns_slot_id() {
        let s = schema();
        let mut page = empty_page(&s);
        let slot = page.insert_tuple(make_tuple(1, true));
        assert!(slot.is_ok());
    }

    #[test]
    fn insert_grows_slot_array() {
        let s = schema();
        let mut page = empty_page(&s);
        assert_eq!(page.num_slots(), 0);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        assert_eq!(page.num_slots(), 1);
        page.insert_tuple(make_tuple(2, false)).unwrap();
        assert_eq!(page.num_slots(), 2);
    }

    #[test]
    fn insert_moves_tuple_start_downward() {
        let s = schema();
        let mut page = empty_page(&s);
        let before = page.tuple_start;
        page.insert_tuple(make_tuple(1, true)).unwrap();
        assert!(page.tuple_start < before);
        assert_eq!(before - page.tuple_start, s.serialized_size());
    }

    #[test]
    fn inserted_tuple_is_visible() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(42, false)).unwrap();
        assert_eq!(page.live_tuples().count(), 1);
    }

    #[test]
    fn multiple_inserts_all_visible() {
        let s = schema();
        let mut page = empty_page(&s);
        for i in 0..5 {
            page.insert_tuple(make_tuple(i, i % 2 == 0)).unwrap();
        }
        assert_eq!(page.live_tuples().count(), 5);
    }

    #[test]
    fn insert_schema_mismatch_is_rejected() {
        let s = schema();
        let mut page = empty_page(&s);
        let wrong = Tuple::new(vec![Value::Int32(1)]); // missing field
        assert!(matches!(
            page.insert_tuple(wrong),
            Err(StorageError::SchemaMismatch)
        ));
    }

    #[test]
    fn delete_removes_tuple() {
        let s = schema();
        let mut page = empty_page(&s);
        let slot = page.insert_tuple(make_tuple(7, true)).unwrap();
        page.delete_tuple(slot).unwrap();
        assert_eq!(page.live_tuples().count(), 0);
    }

    #[test]
    fn delete_does_not_shrink_slot_array() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        let slot = page.insert_tuple(make_tuple(2, false)).unwrap();
        assert_eq!(page.num_slots(), 2);
        page.delete_tuple(slot).unwrap();
        // slot array keeps its size; the deleted slot becomes a tombstone.
        assert_eq!(page.num_slots(), 2);
        assert_eq!(page.empty_slots(), 1);
    }

    #[test]
    fn delete_out_of_bounds_slot_errors() {
        let s = schema();
        let mut page = empty_page(&s);
        let bad_slot = SlotId(999);
        assert!(matches!(
            page.delete_tuple(bad_slot),
            Err(StorageError::SlotOutOfBounds { .. })
        ));
    }

    #[test]
    fn delete_already_empty_slot_errors() {
        let s = schema();
        let mut page = empty_page(&s);
        let slot = page.insert_tuple(make_tuple(1, false)).unwrap();
        page.delete_tuple(slot).unwrap();
        assert!(matches!(
            page.delete_tuple(slot),
            Err(StorageError::SlotAlreadyEmpty { .. })
        ));
    }

    #[test]
    fn page_data_roundtrip() {
        let s = schema();
        let mut page = empty_page(&s);
        let t1 = make_tuple(10, true);
        let t2 = make_tuple(20, false);
        page.insert_tuple(t1.clone()).unwrap();
        page.insert_tuple(t2.clone()).unwrap();

        let bytes = page.page_data();
        let restored = HeapPage::new(&bytes, &s).unwrap();

        let tuples: Vec<&Tuple> = restored.live_tuples().map(|(_, t)| t).collect();
        assert_eq!(tuples.len(), 2);
        assert!(tuples.contains(&&t1));
        assert!(tuples.contains(&&t2));
    }

    #[test]
    fn page_data_roundtrip_preserves_num_slots_and_tuple_start() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        page.insert_tuple(make_tuple(2, false)).unwrap();
        page.insert_tuple(make_tuple(3, true)).unwrap();

        let bytes = page.page_data();
        let restored = HeapPage::new(&bytes, &s).unwrap();

        assert_eq!(restored.num_slots(), page.num_slots());
        assert_eq!(restored.tuple_start, page.tuple_start);
    }

    #[test]
    fn page_data_does_not_overflow_when_page_is_full() {
        let s = schema();
        let mut page = empty_page(&s);
        let capacity = page.remaining_capacity();

        let mut tuples = (0i32..)
            .take(capacity)
            .map(|i| make_tuple(i, i % 2 == 0))
            .collect::<Vec<_>>()
            .into_iter();

        page.insert_many(&mut tuples).unwrap();
        assert_eq!(page.remaining_capacity(), 0);

        // Must not panic.
        let bytes = page.page_data();

        let restored = HeapPage::new(&bytes, &s).unwrap();
        assert_eq!(restored.live_tuples().count(), capacity);
    }

    #[test]
    fn insert_tuple_full_page_roundtrip_preserves_all_data() {
        let s = schema();
        let mut page = empty_page(&s);
        let capacity = page.remaining_capacity();

        let input: Vec<_> = (0i32..)
            .take(capacity)
            .map(|i| make_tuple(i, i % 2 == 0))
            .collect();

        for t in &input {
            page.insert_tuple(t.clone()).unwrap();
        }

        let bytes = page.page_data();
        let restored = HeapPage::new(&bytes, &s).unwrap();

        let live: Vec<&Tuple> = restored.live_tuples().map(|(_, t)| t).collect();
        assert_eq!(live.len(), capacity);
        for t in &input {
            assert!(live.contains(&t), "missing tuple after roundtrip: {t:?}");
        }
    }

    #[test]
    fn delete_leaves_hole_then_roundtrips() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        let _s1 = page.insert_tuple(make_tuple(2, false)).unwrap();
        let s2 = page.insert_tuple(make_tuple(3, true)).unwrap();

        page.delete_tuple(s0).unwrap();
        page.delete_tuple(s2).unwrap();

        let bytes = page.page_data();
        let restored = HeapPage::new(&bytes, &s).unwrap();

        // One tuple survives, in the middle slot.
        assert_eq!(restored.live_tuples().count(), 1);
        assert_eq!(restored.num_slots(), 3);
        assert_eq!(restored.empty_slots(), 2);
    }

    #[test]
    fn remaining_capacity_decreases_on_insert() {
        let s = schema();
        let mut page = empty_page(&s);
        let before = page.remaining_capacity();
        page.insert_tuple(make_tuple(1, true)).unwrap();
        assert_eq!(page.remaining_capacity(), before - 1);
    }

    #[test]
    fn empty_slots_recovers_on_delete() {
        let s = schema();
        let mut page = empty_page(&s);
        let slot = page.insert_tuple(make_tuple(1, true)).unwrap();
        assert_eq!(page.empty_slots(), 0);
        page.delete_tuple(slot).unwrap();
        assert_eq!(page.empty_slots(), 1);
    }

    // --- insert_many: happy path ---

    #[test]
    fn insert_many_inserts_all_when_capacity_sufficient() {
        let s = schema();
        let mut page = empty_page(&s);
        let mut tuples = vec![
            make_tuple(1, true),
            make_tuple(2, false),
            make_tuple(3, true),
        ]
        .into_iter();

        let ids = page.insert_many(&mut tuples).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(page.live_tuples().count(), 3);
    }

    #[test]
    fn insert_many_returns_distinct_in_bounds_slot_ids() {
        let s = schema();
        let mut page = empty_page(&s);
        let mut tuples = vec![make_tuple(10, true), make_tuple(20, false)].into_iter();

        let ids = page.insert_many(&mut tuples).unwrap();
        for id in &ids {
            assert!(u16::from(*id) < page.num_slots());
        }
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len());
    }

    #[test]
    fn insert_many_all_inserted_tuples_are_visible() {
        let s = schema();
        let mut page = empty_page(&s);
        let input = vec![
            make_tuple(5, true),
            make_tuple(6, false),
            make_tuple(7, true),
        ];
        let mut iter = input.clone().into_iter();

        page.insert_many(&mut iter).unwrap();

        let live: Vec<&Tuple> = page.live_tuples().map(|(_, t)| t).collect();
        for t in &input {
            assert!(live.contains(&t));
        }
    }

    // --- insert_many: edge cases ---

    #[test]
    fn insert_many_empty_iterator_returns_empty_vec() {
        let s = schema();
        let mut page = empty_page(&s);
        let mut empty: std::vec::IntoIter<Tuple> = vec![].into_iter();

        let ids = page.insert_many(&mut empty).unwrap();
        assert!(ids.is_empty());
        assert_eq!(page.live_tuples().count(), 0);
    }

    #[test]
    fn insert_many_single_tuple_works() {
        let s = schema();
        let mut page = empty_page(&s);
        let mut single = vec![make_tuple(42, true)].into_iter();

        let ids = page.insert_many(&mut single).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(page.live_tuples().count(), 1);
    }

    #[test]
    fn insert_many_caps_at_remaining_capacity() {
        let s = schema();
        let mut page = empty_page(&s);

        let initial_capacity = page.remaining_capacity();
        let extras = 5;
        let mut tuples = (0i32..)
            .take(initial_capacity + extras)
            .map(|i| make_tuple(i, i % 2 == 0))
            .collect::<Vec<_>>()
            .into_iter();

        let ids = page.insert_many(&mut tuples).unwrap();
        assert_eq!(ids.len(), initial_capacity);
        assert_eq!(page.live_tuples().count(), initial_capacity);
    }

    #[test]
    fn insert_many_does_not_exhaust_iterator_past_capacity() {
        let s = schema();
        let mut page = empty_page(&s);

        let capacity = page.remaining_capacity();
        let all_tuples: Vec<Tuple> = (0i32..)
            .take(capacity + 3)
            .map(|i| make_tuple(i, true))
            .collect();
        let mut iter = all_tuples.into_iter();

        page.insert_many(&mut iter).unwrap();

        let remaining: Vec<Tuple> = iter.collect();
        assert_eq!(remaining.len(), 3);
    }

    #[test]
    fn insert_many_on_full_page_returns_empty_vec() {
        let s = schema();
        let mut page = empty_page(&s);

        let cap = page.remaining_capacity();
        let mut fill = (0i32..)
            .take(cap)
            .map(|i| make_tuple(i, true))
            .collect::<Vec<_>>()
            .into_iter();
        page.insert_many(&mut fill).unwrap();
        assert_eq!(page.remaining_capacity(), 0);

        let mut more = vec![make_tuple(999, false)].into_iter();
        let ids = page.insert_many(&mut more).unwrap();
        assert!(ids.is_empty());
    }

    #[test]
    fn insert_many_fills_reclaimed_slots_after_delete() {
        let s = schema();
        let mut page = empty_page(&s);

        let mut first_batch = vec![make_tuple(1, true), make_tuple(2, false)].into_iter();
        let slot_ids = page.insert_many(&mut first_batch).unwrap();
        let before = page.live_tuples().count();

        page.delete_tuple(slot_ids[0]).unwrap();
        assert_eq!(page.live_tuples().count(), before - 1);

        let mut second_batch = vec![make_tuple(3, true)].into_iter();
        let new_ids = page.insert_many(&mut second_batch).unwrap();
        assert_eq!(new_ids.len(), 1);
        assert_eq!(page.live_tuples().count(), before);
        // The reclaimed slot should have been reused — num_slots unchanged.
        assert_eq!(page.num_slots(), 2);
    }

    // --- insert_many: error paths ---

    #[test]
    fn insert_many_schema_mismatch_returns_error() {
        let s = schema();
        let mut page = empty_page(&s);
        let mut bad = vec![Tuple::new(vec![Value::Int32(1)])].into_iter();
        assert!(matches!(
            page.insert_many(&mut bad),
            Err(StorageError::SchemaMismatch)
        ));
    }

    #[test]
    fn insert_many_schema_mismatch_mid_batch_leaves_prior_inserts() {
        let s = schema();
        let mut page = empty_page(&s);

        let good1 = make_tuple(1, true);
        let good2 = make_tuple(2, false);
        let bad = Tuple::new(vec![Value::Int32(99)]); // missing Bool field

        let mut batch = vec![good1, good2, bad].into_iter();
        let result = page.insert_many(&mut batch);

        assert!(result.is_err());
        // The two valid tuples were already committed — no rollback
        assert_eq!(page.live_tuples().count(), 2);
    }

    // --- delete_many ---

    #[test]
    fn delete_many_removes_all_specified_slots() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        let s1 = page.insert_tuple(make_tuple(2, false)).unwrap();
        let s2 = page.insert_tuple(make_tuple(3, true)).unwrap();

        let count = page.delete_many(&[s0, s2]).unwrap();
        assert_eq!(count, 2);
        assert_eq!(page.live_tuples().count(), 1);
        let remaining: Vec<(SlotId, &Tuple)> = page.live_tuples().collect();
        assert_eq!(remaining[0].0, s1);
    }

    #[test]
    fn delete_many_empty_slice_deletes_nothing() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();

        let count = page.delete_many(&[]).unwrap();
        assert_eq!(count, 0);
        assert_eq!(page.live_tuples().count(), 1);
    }

    #[test]
    fn delete_many_single_slot() {
        let s = schema();
        let mut page = empty_page(&s);
        let slot = page.insert_tuple(make_tuple(5, false)).unwrap();

        let count = page.delete_many(&[slot]).unwrap();
        assert_eq!(count, 1);
        assert_eq!(page.live_tuples().count(), 0);
    }

    #[test]
    fn delete_many_out_of_bounds_errors() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();

        let result = page.delete_many(&[SlotId(9999)]);
        assert!(matches!(result, Err(StorageError::SlotOutOfBounds { .. })));
    }

    #[test]
    fn delete_many_already_empty_slot_errors() {
        let s = schema();
        let mut page = empty_page(&s);
        let slot = page.insert_tuple(make_tuple(1, true)).unwrap();
        page.delete_tuple(slot).unwrap();

        let result = page.delete_many(&[slot]);
        assert!(matches!(result, Err(StorageError::SlotAlreadyEmpty { .. })));
    }

    #[test]
    fn delete_many_stops_on_first_error_partial() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        page.insert_tuple(make_tuple(2, false)).unwrap();

        // s0 is valid, SlotId(9999) is out of bounds — first delete succeeds, second fails
        let result = page.delete_many(&[s0, SlotId(9999)]);
        assert!(result.is_err());
        // s0 was already deleted before the error
        assert_eq!(page.live_tuples().count(), 1);
    }

    #[test]
    fn delete_many_reclaims_slots_for_reuse() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        let s1 = page.insert_tuple(make_tuple(2, false)).unwrap();
        let empty_before = page.empty_slots();

        page.delete_many(&[s0, s1]).unwrap();
        assert_eq!(page.empty_slots(), empty_before + 2);
    }

    // --- set_before_image ---

    #[test]
    fn before_image_captures_construction_state() {
        let s = schema();
        let mut page = empty_page(&s);
        let before = page.before_image().unwrap();

        page.insert_tuple(make_tuple(1, true)).unwrap();
        // before_image still returns the original empty page
        assert_eq!(page.before_image().unwrap(), before);
    }

    #[test]
    fn set_before_image_snapshots_current_state() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();

        let after_insert = page.page_data();
        page.set_before_image();

        // before_image now matches the post-insert state
        assert_eq!(page.before_image().unwrap(), after_insert);
    }

    #[test]
    fn set_before_image_then_mutate_preserves_snapshot() {
        let s = schema();
        let mut page = empty_page(&s);
        let slot = page.insert_tuple(make_tuple(1, true)).unwrap();

        page.set_before_image();
        let snapshot = page.before_image().unwrap();

        page.delete_tuple(slot).unwrap();
        // before_image is still the pre-delete snapshot
        assert_eq!(page.before_image().unwrap(), snapshot);
        // but page_data has changed
        assert_ne!(page.page_data(), snapshot);
    }

    #[test]
    fn compact_on_empty_page_is_noop() {
        let s = schema();
        let mut page = empty_page(&s);
        let tuple_start_before = page.tuple_start;
        page.compact().unwrap();
        assert_eq!(page.tuple_start, tuple_start_before);
        assert_eq!(page.live_tuples().count(), 0);
        assert_eq!(page.num_slots(), 0);
    }

    #[test]
    fn compact_with_no_holes_is_noop() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        page.insert_tuple(make_tuple(2, false)).unwrap();
        page.insert_tuple(make_tuple(3, true)).unwrap();
        let tuple_start_before = page.tuple_start;

        page.compact().unwrap();

        assert_eq!(page.tuple_start, tuple_start_before);
        assert_eq!(page.live_tuples().count(), 3);
    }

    #[test]
    fn compact_recovers_space_after_middle_delete() {
        let s = schema();
        let mut page = empty_page(&s);
        let _s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        let s1 = page.insert_tuple(make_tuple(2, false)).unwrap();
        let _s2 = page.insert_tuple(make_tuple(3, true)).unwrap();
        let tup_size = s.serialized_size();
        let before = page.tuple_start;

        page.delete_tuple(s1).unwrap();
        // delete does not move tuple_start
        assert_eq!(page.tuple_start, before);

        page.compact().unwrap();
        // one tuple's worth of bytes recovered
        assert_eq!(page.tuple_start, before + tup_size);
    }

    #[test]
    fn compact_recovers_space_after_head_delete() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        page.insert_tuple(make_tuple(2, false)).unwrap();
        let tup_size = s.serialized_size();
        let before = page.tuple_start;

        page.delete_tuple(s0).unwrap();
        page.compact().unwrap();
        assert_eq!(page.tuple_start, before + tup_size);
    }

    #[test]
    fn compact_recovers_space_after_tail_delete() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        let s1 = page.insert_tuple(make_tuple(2, false)).unwrap();
        let tup_size = s.serialized_size();
        let before = page.tuple_start;

        page.delete_tuple(s1).unwrap();
        page.compact().unwrap();
        assert_eq!(page.tuple_start, before + tup_size);
    }

    #[test]
    fn compact_preserves_slot_ids_for_live_tuples() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(10, true)).unwrap();
        let s1 = page.insert_tuple(make_tuple(20, false)).unwrap();
        let s2 = page.insert_tuple(make_tuple(30, true)).unwrap();

        page.delete_tuple(s1).unwrap();
        page.compact().unwrap();

        let live: Vec<(SlotId, &Tuple)> = page.live_tuples().collect();
        assert_eq!(live.len(), 2);
        let ids: Vec<SlotId> = live.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&s0));
        assert!(ids.contains(&s2));
        // tombstone in the middle remains — slot identity preserved
        assert_eq!(page.num_slots(), 3);
        assert_eq!(page.empty_slots(), 1);
    }

    #[test]
    fn compact_preserves_tuple_values() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(10, true)).unwrap();
        let _s1 = page.insert_tuple(make_tuple(20, false)).unwrap();
        let s2 = page.insert_tuple(make_tuple(30, true)).unwrap();

        page.delete_tuple(s0).unwrap();
        page.compact().unwrap();

        let live: std::collections::HashMap<SlotId, &Tuple> = page.live_tuples().collect();
        assert_eq!(live[&s2], &make_tuple(30, true));
    }

    #[test]
    fn compact_roundtrips_through_page_data() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        let s1 = page.insert_tuple(make_tuple(2, false)).unwrap();
        page.insert_tuple(make_tuple(3, true)).unwrap();
        page.delete_tuple(s0).unwrap();
        page.delete_tuple(s1).unwrap();

        page.compact().unwrap();
        let bytes = page.page_data();
        let restored = HeapPage::new(&bytes, &s).unwrap();

        assert_eq!(restored.live_tuples().count(), 1);
        assert_eq!(restored.num_slots(), 3);
        assert_eq!(restored.empty_slots(), 2);
        assert_eq!(restored.tuple_start, page.tuple_start);
    }

    #[test]
    fn compact_then_insert_uses_recovered_space() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        page.insert_tuple(make_tuple(2, false)).unwrap();
        let cap_before_delete = page.remaining_capacity();

        page.delete_tuple(s0).unwrap();
        page.compact().unwrap();

        // Reclaimed both the slot and its bytes — capacity is strictly higher
        // than it was before the delete (delete alone leaks the bytes).
        assert!(page.remaining_capacity() > cap_before_delete);
    }

    #[test]
    fn compact_is_idempotent() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        page.insert_tuple(make_tuple(2, false)).unwrap();
        page.delete_tuple(s0).unwrap();

        page.compact().unwrap();
        let bytes_after_first = page.page_data();
        let tuple_start_after_first = page.tuple_start;

        page.compact().unwrap();
        assert_eq!(page.page_data(), bytes_after_first);
        assert_eq!(page.tuple_start, tuple_start_after_first);
    }

    #[test]
    fn compact_moves_tuple_start_to_pagesize_when_all_deleted() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        let s1 = page.insert_tuple(make_tuple(2, false)).unwrap();

        page.delete_tuple(s0).unwrap();
        page.delete_tuple(s1).unwrap();
        page.compact().unwrap();

        assert_eq!(page.tuple_start, PAGE_SIZE);
        assert_eq!(page.live_tuples().count(), 0);
        // tombstones still present — slot identity preserved
        assert_eq!(page.num_slots(), 2);
    }

    #[test]
    fn compact_packs_tuples_contiguously_against_pagesize() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        page.insert_tuple(make_tuple(2, false)).unwrap();
        page.insert_tuple(make_tuple(3, true)).unwrap();
        let tup_size = s.serialized_size();

        page.delete_tuple(s0).unwrap();
        page.compact().unwrap();

        let mut live_offsets: Vec<usize> = page
            .slot_pointers
            .iter()
            .filter(|sp| !sp.is_tombstone())
            .map(|sp| sp.offset as usize)
            .collect();
        live_offsets.sort_unstable();
        assert_eq!(live_offsets.len(), 2);
        assert_eq!(live_offsets[1] + tup_size, PAGE_SIZE);
        assert_eq!(live_offsets[0] + tup_size, live_offsets[1]);
        assert_eq!(page.tuple_start, live_offsets[0]);
    }
}
