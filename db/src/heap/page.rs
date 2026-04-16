//! Heap page layout and management for fixed-schema tuple storage.
//!
//! A [`HeapPage`] wraps a raw `PAGE_SIZE`-byte buffer and interprets it as a
//! slotted page: a compact header of [`SlotPointer`] entries at the front,
//! followed by tuple data growing upward from the end of the header.
//!
//! Each slot pointer records where a tuple lives inside the page (byte offset
//! and byte length). A slot whose `length` is zero is considered empty and can
//! be reused by the next insert.
//!
//! The page holds a before-image copy of its raw bytes at construction time
//! so that callers can produce WAL undo records when needed.

use std::vec;

use byteorder::{ByteOrder, LittleEndian};

use crate::{
    primitives::{PageId, SlotId},
    storage::{MAX_TUPLE_SIZE, PAGE_SIZE, Page, StorageError},
    tuple::{Tuple, TupleSchema},
};

/// Number of bytes occupied by one slot pointer in the page header.
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
struct SlotPointer {
    /// Byte offset of the tuple data from the start of the page buffer.
    offset: u16,
    /// Byte length of the serialized tuple. Zero means the slot is empty.
    length: u16,
}

/// A fixed-size page that stores tuples according to a given schema.
///
/// `HeapPage` manages a `PAGE_SIZE`-byte region of storage as a slotted page.
/// The front of the buffer holds a dense array of [`SlotPointer`] entries (one
/// per logical slot). Tuple data is written immediately after the header and
/// grows toward the end of the buffer.
///
/// The page is parameterized by a schema lifetime `'a`; the [`TupleSchema`]
/// must outlive the page.
pub struct HeapPage<'a> {
    /// Identifier of this page within its heap file.
    page_id: PageId,
    /// Schema that describes the shape of every tuple on this page.
    schema: &'a TupleSchema,
    /// In-memory decoded tuples, indexed by slot number. `None` means the
    /// slot is empty.
    tuples: Vec<Option<Tuple>>,
    /// Parallel array of slot pointers that mirrors `tuples`.
    slot_pointers: Vec<SlotPointer>,
    /// Byte offset of the next free byte available for tuple data.
    free_space_offset: usize,
    /// Snapshot of the raw page bytes taken when the page was first loaded,
    /// used as the WAL before-image.
    old_data: [u8; PAGE_SIZE],
    /// Total number of slots (both occupied and empty) on this page.
    num_slots: u16,
}

impl<'a> HeapPage<'a> {
    /// Loads a heap page from a raw byte slice, decoding all slot pointers and
    /// stored tuples.
    ///
    /// `data` must be exactly `PAGE_SIZE` bytes. The first
    /// `num_slots × SLOT_POINTER_SIZE` bytes are interpreted as slot pointers;
    /// the tuple bytes they reference are deserialized according to `schema`.
    ///
    /// A freshly zeroed buffer (e.g. `[0u8; PAGE_SIZE]`) produces an empty
    /// page ready for inserts.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidPageSize`] if `data.len() != PAGE_SIZE`.
    ///
    /// Returns [`StorageError::ParseError`] if any slot pointer or tuple data
    /// is malformed or out of bounds.
    pub fn new(
        page_id: PageId,
        data: &[u8],
        schema: &'a TupleSchema,
    ) -> Result<Self, StorageError> {
        if data.len() != PAGE_SIZE {
            return Err(StorageError::InvalidPageSize { got: data.len() });
        }

        let num_tuples = data.len() / (schema.serialized_size() + SLOT_POINTER_SIZE);
        let tuples = vec![None; num_tuples];
        let slot_pointers = vec![SlotPointer::default(); num_tuples];

        let mut hp = Self {
            page_id,
            schema,
            old_data: data.try_into().unwrap(),
            tuples,
            slot_pointers,
            free_space_offset: num_tuples * SLOT_POINTER_SIZE,
            num_slots: num_tuples.try_into().unwrap(),
        };

        hp.parse_data(data)?;
        Ok(hp)
    }

    /// Checks that `slot_id` refers to an existing slot on this page.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::SlotOutOfBounds`] when `slot_id >= num_slots`.
    fn check_slot_bounds(&self, slot_id: SlotId) -> Result<(), StorageError> {
        if u16::from(slot_id) >= self.num_slots {
            return Err(StorageError::slot_out_of_bounds(slot_id, self.num_slots));
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

    /// Returns the number of slots that currently hold no tuple.
    ///
    /// This is the number of slots available for future inserts without
    /// growing the page.
    pub fn empty_slots(&self) -> usize {
        self.slot_pointers
            .iter()
            .filter(|sp| sp.length == 0)
            .count()
    }

    /// Writes `tuple` into the first available empty slot and returns its [`SlotId`].
    ///
    /// The tuple is validated against the page schema before insertion. The
    /// slot pointer is updated in memory; call [`Page::page_data`] to obtain
    /// the updated raw bytes for flushing to disk.
    ///
    /// # Errors
    ///
    /// - [`StorageError::SchemaMismatch`] — tuple does not match this page's schema.
    /// - [`StorageError::PageFull`] — no empty slots remain, or the remaining
    ///   free bytes in the page buffer are insufficient.
    /// - [`StorageError::TupleTooLarge`] — the serialized tuple exceeds
    ///   `MAX_TUPLE_SIZE`.
    pub(crate) fn insert_tuple(&mut self, tuple: Tuple) -> Result<SlotId, StorageError> {
        self.schema
            .validate(&tuple)
            .map_err(|_| StorageError::SchemaMismatch)?;

        let empty_slot = self
            .slot_pointers
            .iter()
            .position(|sp| sp.length == 0)
            .ok_or(StorageError::PageFull)?;

        let tup_size = self.schema.serialized_size();
        if tup_size > MAX_TUPLE_SIZE {
            return Err(StorageError::TupleTooLarge {
                size: tup_size,
                max: MAX_TUPLE_SIZE,
            });
        }

        if self.free_space_offset + tup_size > PAGE_SIZE {
            return Err(StorageError::PageFull);
        }

        self.free_space_offset += tup_size;

        let offset = u16::try_from(self.free_space_offset).map_err(|_| {
            StorageError::ParseError("free space offset overflowed u16".to_string())
        })?;
        let length =
            u16::try_from(tup_size).map_err(|_| StorageError::tuple_too_large(tup_size))?;

        self.slot_pointers[empty_slot] = SlotPointer { offset, length };

        self.tuples[empty_slot] = Some(tuple);
        empty_slot
            .try_into()
            .map_err(|_| StorageError::SlotOutOfBounds {
                slot: u16::try_from(empty_slot).unwrap_or(u16::MAX),
                num_slots: self.num_slots,
            })
    }

    /// Marks the tuple at `slot_id` as deleted by zeroing its slot pointer.
    ///
    /// The slot becomes available for future inserts. The tuple bytes in the
    /// page buffer are not zeroed out, but they will be overwritten on the next
    /// insert that claims the slot.
    ///
    /// # Errors
    ///
    /// - [`StorageError::SlotOutOfBounds`] — `slot_id` is beyond the last slot.
    /// - [`StorageError::SlotAlreadyEmpty`] — the slot is already empty.
    pub(crate) fn delete_tuple(&mut self, slot_id: SlotId) -> Result<(), StorageError> {
        self.check_slot_bounds(slot_id)?;

        self.is_slot_empty(slot_id)
            .map_err(|_| StorageError::slot_already_empty(slot_id))?;

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
        Ok(self.slot_pointers[usize::from(slot_id)].length == 0)
    }

    /// Reads all slot pointers and tuple data from the raw `data` buffer into
    /// `self`.
    ///
    /// Called once from [`HeapPage::new`] after the struct is initialized.
    /// Updates `free_space_offset` to account for any existing tuple data.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::ParseError`] if a slot pointer or its
    /// referenced tuple data falls outside `data`.
    fn parse_data(&mut self, data: &[u8]) -> Result<(), StorageError> {
        for i in 0..usize::from(self.num_slots) {
            let data_offset = i * SLOT_POINTER_SIZE;
            if data_offset + SLOT_POINTER_SIZE > data.len() {
                return Err(StorageError::ParseError(format!(
                    "slot pointer {i} out of bounds",
                )));
            }

            let offset = LittleEndian::read_u16(&data[data_offset..]);
            let length = LittleEndian::read_u16(&data[data_offset + 2..]);

            self.slot_pointers[i] = SlotPointer { offset, length };
            self.free_space_offset = self.free_space_offset.max(usize::from(offset + length));
        }

        for (i, sp) in self.slot_pointers.iter().enumerate() {
            if sp.length == 0 {
                continue;
            }

            if usize::from(sp.length + sp.offset) > data.len() {
                return Err(StorageError::ParseError(format!(
                    "tuple data for slot with offset {} and length {} out of bounds",
                    sp.offset, sp.length
                )));
            }

            let start = usize::from(sp.offset);
            let end = start + usize::from(sp.length);
            let tuple_data = &data[start..end];
            let tup = Tuple::deserialize(self.schema, tuple_data)
                .map_err(|e| StorageError::ParseError(format!("failed to parse tuple: {e}")))?;
            self.tuples[i] = Some(tup);
        }

        Ok(())
    }
}

impl Page for HeapPage<'_> {
    /// Serializes the current page state into a fresh `PAGE_SIZE`-byte array.
    ///
    /// The slot pointer header is written first (little-endian `u16` pairs),
    /// followed by each live tuple serialized at the byte range its slot
    /// pointer describes.
    ///
    /// # Panics
    ///
    /// Panics if serializing any tuple fails — this should not happen for
    /// tuples that were already validated on insert.
    fn page_data(&self) -> [u8; PAGE_SIZE] {
        let mut bytes = [0u8; PAGE_SIZE];
        for (i, sp) in self.slot_pointers.iter().enumerate() {
            let offset = i * SLOT_POINTER_SIZE;
            LittleEndian::write_u16(&mut bytes[offset..], sp.offset);
            LittleEndian::write_u16(&mut bytes[offset + 2..], sp.length);
        }

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

    fn set_before_image(&mut self) {
        // No-op since we don't track before images for heap pages
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        primitives::{FileId, PageId, PageNumber},
        tuple::{Field, TupleSchema},
        types::{Type, Value},
    };

    fn schema() -> TupleSchema {
        TupleSchema::new(vec![
            Field::new("id", Type::Int32),
            Field::new("flag", Type::Bool),
        ])
    }

    fn page_id() -> PageId {
        PageId::new(FileId(1), PageNumber(0))
    }

    fn make_tuple(id: i32, flag: bool) -> Tuple {
        Tuple::new(vec![Value::Int32(id), Value::Bool(flag)])
    }

    fn empty_page(schema: &TupleSchema) -> HeapPage<'_> {
        HeapPage::new(page_id(), &[0u8; PAGE_SIZE], schema).unwrap()
    }

    #[test]
    fn new_empty_page_has_no_tuples() {
        let s = schema();
        let page = empty_page(&s);
        assert_eq!(page.live_tuples().count(), 0);
    }

    #[test]
    fn new_rejects_wrong_size_data() {
        let s = schema();
        let result = HeapPage::new(page_id(), &[0u8; 100], &s);
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
        let restored = HeapPage::new(page_id(), &bytes, &s).unwrap();

        let tuples: Vec<&Tuple> = restored.live_tuples().map(|(_, t)| t).collect();
        assert_eq!(tuples.len(), 2);
        assert!(tuples.contains(&&t1));
        assert!(tuples.contains(&&t2));
    }

    #[test]
    fn empty_slots_decreases_on_insert() {
        let s = schema();
        let mut page = empty_page(&s);
        let before = page.empty_slots();
        page.insert_tuple(make_tuple(1, true)).unwrap();
        assert_eq!(page.empty_slots(), before - 1);
    }

    #[test]
    fn empty_slots_recovers_on_delete() {
        let s = schema();
        let mut page = empty_page(&s);
        let before = page.empty_slots();
        let slot = page.insert_tuple(make_tuple(1, true)).unwrap();
        page.delete_tuple(slot).unwrap();
        assert_eq!(page.empty_slots(), before);
    }
}
