//! Heap page layout and management for slotted tuple storage.
//!
//! A [`HeapPage`] wraps a raw `PAGE_SIZE`-byte buffer and interprets it as a
//! bidirectional slotted page: a fixed header followed by a slot-pointer array
//! growing from the low end, and tuple data growing from the high end toward
//! the middle.
//!
//! ## On-disk layout  (one `PAGE_SIZE`-byte buffer)
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
//!  Fixed header (first PAGE_HDR_SIZE = 17 bytes):
//!
//!  byte 0       byte 1..5          byte 5..13        byte 13..15     byte 15..17
//!  +------------+------------------+-----------------+---------------+---------------+
//!  | kind (u8)  | CRC32 (u32 LE)   | page_lsn (u64)  | num_slots(u16)| tuple_start(u16)|
//!  +------------+------------------+-----------------+---------------+---------------+
//!
//!  `num_slots` and `tuple_start` are the first two fields of [`HeapTypedBody`].
//!  [`HeapTypedHeader`] is a zero-byte unit type so that [`Decode`] for the body
//!  sees `num_slots` as its first field and can parse the slot array without any
//!  external context.
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

use std::{
    io::{Read, Write},
    sync::Arc,
};

use byteorder::{ByteOrder, LittleEndian};

use crate::{
    codec::{CodecError, Decode, Encode},
    primitives::{Lsn, SlotId},
    storage::{
        MAX_TUPLE_SIZE, PAGE_SIZE, Page, PageKind, StorageError, TypedPage, UNIVERSAL_HEADER_SIZE,
    },
    tuple::{Tuple, TupleSchema},
};

/// Number of bytes before the slot array.
///
/// Universal header (13) + the `num_slots`/`tuple_start` prefix at the start
/// of [`HeapTypedBody`] (4) = 17.  [`HeapTypedHeader`] encodes zero bytes, so
/// all 4 of those bytes come from the body prefix.
const PAGE_HDR_SIZE: usize = UNIVERSAL_HEADER_SIZE + BODY_PREFIX_SIZE;

/// Number of bytes occupied by one slot pointer.
const SLOT_POINTER_SIZE: usize = 4;

/// Bytes at the start of [`HeapTypedBody`] before the slot array:
/// `num_slots` (u16 LE) + `tuple_start` (u16 LE).
const BODY_PREFIX_SIZE: usize = 4;

/// Bytes available for heap payload after the universal header.
/// [`HeapTypedHeader`] is a zero-byte unit type, so the body gets all
/// remaining space: `PAGE_SIZE - UNIVERSAL_HEADER_SIZE`.
const HEAP_TYPED_BODY_SIZE: usize = PAGE_SIZE - UNIVERSAL_HEADER_SIZE; // 4083

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize, "PAGE_SIZE must fit in u16");
const _: () = assert!(
    MAX_TUPLE_SIZE <= u16::MAX as usize,
    "MAX_TUPLE_SIZE must fit in u16"
);
const _: () = assert!(
    PAGE_HDR_SIZE == UNIVERSAL_HEADER_SIZE + BODY_PREFIX_SIZE,
    "heap header offsets must align with typed page layout"
);

/// Zero-byte type-specific header for a heap page.
///
/// All structural metadata (`num_slots`, `tuple_start`) lives at the start of
/// [`HeapTypedBody`] so that [`Decode`] for the body sees those fields first
/// and can parse the slot-pointer array without external context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapTypedHeader;

impl Encode for HeapTypedHeader {
    fn encode<W: Write>(&self, _writer: &mut W) -> Result<(), CodecError> {
        Ok(())
    }
}

impl Decode for HeapTypedHeader {
    fn decode<R: Read>(_reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self)
    }
}

/// Location and size of one tuple inside a [`HeapPage`].
///
/// A slot pointer whose `length` is `0` marks an empty (deleted or never-used)
/// slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
struct SlotPointer {
    /// Byte offset of the tuple data from the start of the **page** buffer.
    offset: u16,
    /// Byte length of the serialized tuple. Zero means the slot is empty.
    length: u16,
}

impl SlotPointer {
    #[inline]
    pub(super) fn is_tombstone(self) -> bool {
        self.length == 0
    }
}

/// In-memory body of a heap page.
///
/// On disk the body begins with `num_slots` (u16) and `tuple_start` (u16),
/// followed by the slot-pointer array and the tuple data region.  Placing
/// those two fields inside the body (rather than a separate header) means
/// [`Decode`] can read `num_slots` as its first field and parse the full slot
/// array without any external context.
///
/// Tuple deserialization still requires a [`TupleSchema`], which a context-free
/// [`Decode`] cannot carry. A second hydration pass with the schema completes
/// initialization once the schema is known; `schema` is `None` until that pass
/// returns `Ok`.
pub struct HeapTypedBody {
    slot_pointers: Vec<SlotPointer>,
    /// `None` until hydration with a schema completes.
    schema: Option<Arc<TupleSchema>>,
    /// Parallel to `slot_pointers`; `None` for tombstones and un-hydrated slots.
    tuples: Vec<Option<Tuple>>,
    /// Page-absolute byte offset of the first tuple byte.  Equals `PAGE_SIZE`
    /// for an empty page.
    tuple_start: usize,
    /// Raw body bytes retained after [`Decode`] so hydration can extract
    /// tuple data without re-reading from disk.
    raw: [u8; HEAP_TYPED_BODY_SIZE],
}

impl HeapTypedBody {
    /// Returns the schema.
    ///
    /// # Panics
    ///
    /// Panics if called before [`HeapTypedBody::hydrate`].
    fn schema(&self) -> &Arc<TupleSchema> {
        self.schema
            .as_ref()
            .expect("HeapPage must be hydrated with a schema before use")
    }

    /// Second-pass initialization: parses tuple data from `self.raw` using
    /// `schema` and populates `self.tuples`.
    ///
    /// Must be called exactly once, immediately after [`Decode`].
    fn hydrate(&mut self, schema: Arc<TupleSchema>) -> Result<(), StorageError> {
        let ts_body = self.tuple_start.saturating_sub(UNIVERSAL_HEADER_SIZE);

        for i in 0..self.slot_pointers.len() {
            let sp = self.slot_pointers[i];
            if sp.is_tombstone() {
                continue;
            }
            let start = usize::from(sp.offset)
                .checked_sub(UNIVERSAL_HEADER_SIZE)
                .ok_or_else(|| StorageError::ParseError("slot offset underflow".into()))?;
            let end = start
                .checked_add(usize::from(sp.length))
                .ok_or_else(|| StorageError::ParseError("slot range overflow".into()))?;

            if end > self.raw.len() || start < ts_body {
                return Err(StorageError::ParseError(format!(
                    "tuple data for slot {i} out of range [{start},{end})"
                )));
            }

            self.tuples[i] = Some(
                Tuple::deserialize(schema.as_ref(), &self.raw[start..end])
                    .map_err(|e| StorageError::ParseError(format!("slot {i}: {e}")))?,
            );
        }

        self.schema = Some(schema);
        Ok(())
    }
}

impl Encode for HeapTypedBody {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let mut buf = [0u8; HEAP_TYPED_BODY_SIZE];

        let num_slots = u16::try_from(self.slot_pointers.len())
            .map_err(|_| CodecError::Io(std::io::Error::other("num_slots overflowed u16")))?;
        let tuple_start = u16::try_from(self.tuple_start)
            .map_err(|_| CodecError::Io(std::io::Error::other("tuple_start overflowed u16")))?;
        buf[0..2].copy_from_slice(&num_slots.to_le_bytes());
        buf[2..4].copy_from_slice(&tuple_start.to_le_bytes());

        for (i, &SlotPointer { offset, length }) in self.slot_pointers.iter().enumerate() {
            let off = BODY_PREFIX_SIZE + i * SLOT_POINTER_SIZE;
            LittleEndian::write_u16(&mut buf[off..off + 2], offset);
            LittleEndian::write_u16(&mut buf[off + 2..off + 4], length);
        }

        let schema = self
            .schema
            .as_deref()
            .expect("schema must be set before encoding");

        for (sp, tup) in self.slot_pointers.iter().zip(self.tuples.iter()) {
            if let Some(t) = tup {
                let body_start = usize::from(sp.offset)
                    .checked_sub(UNIVERSAL_HEADER_SIZE)
                    .ok_or_else(|| CodecError::Io(std::io::Error::other("offset underflow")))?;
                let body_end = body_start + usize::from(sp.length);
                t.serialize(schema, &mut buf[body_start..body_end])
                    .map_err(|e| CodecError::Io(std::io::Error::other(e.to_string())))?;
            }
        }

        writer.write_all(&buf)?;
        Ok(())
    }
}

impl Decode for HeapTypedBody {
    /// Reads the raw body bytes and decodes what it can without a schema:
    /// `num_slots`, `tuple_start`, and the full slot-pointer array.
    ///
    /// `tuples` is left as a `vec![None; num_slots]` and `schema` is `None`.
    /// Hydrate with the table schema to complete initialization.
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut raw = [0u8; HEAP_TYPED_BODY_SIZE];
        reader.read_exact(&mut raw)?;

        let num_slots = u16::from_le_bytes(raw[0..2].try_into().unwrap());
        let raw_tuple_start = u16::from_le_bytes(raw[2..4].try_into().unwrap());

        let tuple_start = if num_slots == 0 && raw_tuple_start == 0 {
            PAGE_SIZE
        } else {
            usize::from(raw_tuple_start)
        };

        let mut slot_pointers = Vec::with_capacity(usize::from(num_slots));
        for i in 0..usize::from(num_slots) {
            let off = BODY_PREFIX_SIZE + i * SLOT_POINTER_SIZE;
            if off + SLOT_POINTER_SIZE > raw.len() {
                return Err(CodecError::Io(std::io::Error::other(format!(
                    "slot pointer {i} out of bounds"
                ))));
            }
            let offset = u16::from_le_bytes(raw[off..off + 2].try_into().unwrap());
            let length = u16::from_le_bytes(raw[off + 2..off + 4].try_into().unwrap());
            slot_pointers.push(SlotPointer { offset, length });
        }

        let num = usize::from(num_slots);
        Ok(Self {
            slot_pointers,
            schema: None,
            tuples: vec![None; num],
            tuple_start,
            raw,
        })
    }
}

/// A fixed-size page that stores tuples according to a given schema.
///
/// `HeapPage` is a [`TypedPage`] whose body holds all structural metadata,
/// slot pointers, and deserialized tuples in memory.  The [`Page`] trait
/// (CRC, LSN, before-image) is implemented automatically by [`TypedPage`].
pub type HeapPage = TypedPage<HeapTypedHeader, HeapTypedBody>;

impl TypedPage<HeapTypedHeader, HeapTypedBody> {
    /// Loads a heap page from a raw byte slice, decoding slot pointers and
    /// deserializing stored tuples using `schema`.
    ///
    /// A freshly zeroed buffer is accepted as an empty page.
    ///
    /// # Errors
    ///
    /// - [`StorageError::InvalidPageSize`] if `data.len() != PAGE_SIZE`.
    /// - [`StorageError::ChecksumMismatch`] if the stored CRC does not match.
    /// - [`StorageError::ParseError`] if the slot array or any tuple is malformed.
    pub fn heap_new(data: &[u8], schema: Arc<TupleSchema>) -> Result<Self, StorageError> {
        if data.len() != PAGE_SIZE {
            return Err(StorageError::InvalidPageSize { got: data.len() });
        }

        let bytes: &[u8; PAGE_SIZE] = data.try_into().expect("validated PAGE_SIZE");
        let is_blank = bytes.iter().all(|&b| b == 0);

        let mut page = if is_blank {
            Self::new_empty(schema)
        } else {
            let mut p: TypedPage<HeapTypedHeader, HeapTypedBody> =
                TypedPage::from_page_bytes(bytes)?;
            if p.kind != PageKind::Heap {
                return Err(StorageError::ParseError(format!(
                    "expected heap page kind, got {:?}",
                    p.kind
                )));
            }
            let slot_end = Self::slot_array_end(p.body.slot_pointers.len());
            if p.body.tuple_start < slot_end || p.body.tuple_start > PAGE_SIZE {
                return Err(StorageError::ParseError(format!(
                    "invalid tuple_start={}, slot_end={slot_end}",
                    p.body.tuple_start
                )));
            }
            p.body.hydrate(schema)?;
            p
        };

        page.set_before_image();
        Ok(page)
    }

    /// Constructs an in-memory empty heap page with no on-disk bytes to parse.
    fn new_empty(schema: Arc<TupleSchema>) -> Self {
        TypedPage::new(
            PageKind::Heap,
            Lsn::INVALID,
            HeapTypedHeader,
            HeapTypedBody {
                slot_pointers: vec![],
                schema: Some(schema),
                tuples: vec![],
                tuple_start: PAGE_SIZE,
                raw: [0u8; HEAP_TYPED_BODY_SIZE],
            },
        )
    }

    /// Page-absolute end of the slot-pointer array for `n_slots` slots.
    #[inline]
    fn slot_array_end(n_slots: usize) -> usize {
        PAGE_HDR_SIZE + n_slots * SLOT_POINTER_SIZE
    }

    /// Bytes currently free between the slot array and the tuple region.
    #[inline]
    fn free_bytes(&self) -> usize {
        self.body
            .tuple_start
            .saturating_sub(Self::slot_array_end(self.body.slot_pointers.len()))
    }

    /// Current number of slots (occupied and empty).
    #[inline]
    fn num_slots(&self) -> u16 {
        u16::try_from(self.body.slot_pointers.len()).unwrap_or(u16::MAX)
    }

    /// Checks whether the given `slot_id` is within the valid range of slot indexes for this page.
    ///
    /// # Arguments
    ///
    /// * `slot_id` - The slot identifier to check.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::SlotOutOfBounds`] if `slot_id` is greater than or equal to the
    /// current number of slots.
    fn check_slot_bounds(&self, slot_id: SlotId) -> Result<(), StorageError> {
        let n = self.num_slots();
        if u16::from(slot_id) >= n {
            return Err(StorageError::slot_out_of_bounds(slot_id, n));
        }
        Ok(())
    }

    /// Returns the live tuple at `slot_id`, or `None` when the slot is empty.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::SlotOutOfBounds`] when `slot_id` is not valid.
    pub(crate) fn tuple_at(&self, slot_id: SlotId) -> Result<Option<&Tuple>, StorageError> {
        self.check_slot_bounds(slot_id)?;
        Ok(self.body.tuples[usize::from(slot_id)].as_ref())
    }

    /// Returns an iterator over every occupied slot, yielding `(SlotId, &Tuple)`.
    pub(crate) fn live_tuples(&self) -> impl Iterator<Item = (SlotId, &Tuple)> {
        self.body.tuples.iter().enumerate().filter_map(|(i, slot)| {
            let slot_id = u16::try_from(i).ok()?;
            slot.as_ref().map(|tuple| (SlotId(slot_id), tuple))
        })
    }

    /// Returns the number of existing slots that currently hold no tuple.
    pub fn empty_slots(&self) -> usize {
        self.body
            .slot_pointers
            .iter()
            .filter(|sp| sp.is_tombstone())
            .count()
    }

    /// Returns the number of tuples that can still be inserted before the page
    /// is full.
    pub fn remaining_capacity(&self) -> usize {
        let tup = self.body.schema().serialized_size();
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
    /// # Errors
    ///
    /// - [`StorageError::SchemaMismatch`] — tuple fields do not match this page's schema.
    /// - [`StorageError::PageFull`] — no room for the slot pointer and tuple bytes.
    /// - [`StorageError::TupleTooLarge`] — serialized size exceeds `MAX_TUPLE_SIZE`.
    pub(crate) fn insert_tuple(&mut self, tuple: Tuple) -> Result<SlotId, StorageError> {
        let schema = self.body.schema().clone();
        schema
            .validate(&tuple)
            .map_err(|_| StorageError::SchemaMismatch)?;

        let tup_size = schema.serialized_size();
        if tup_size > MAX_TUPLE_SIZE {
            return Err(StorageError::TupleTooLarge {
                size: tup_size,
                max: MAX_TUPLE_SIZE,
            });
        }

        let reused = self
            .body
            .slot_pointers
            .iter()
            .position(|sp| sp.is_tombstone());
        let needs_new_slot = reused.is_none();
        let slot_growth = if needs_new_slot { SLOT_POINTER_SIZE } else { 0 };
        let new_slot_end = Self::slot_array_end(self.body.slot_pointers.len()) + slot_growth;
        let new_tuple_start = self
            .body
            .tuple_start
            .checked_sub(tup_size)
            .ok_or(StorageError::PageFull)?;
        if new_slot_end > new_tuple_start {
            return Err(StorageError::PageFull);
        }

        self.body.tuple_start = new_tuple_start;
        let offset = u16::try_from(new_tuple_start)
            .map_err(|_| StorageError::ParseError("tuple_start overflowed u16".to_string()))?;
        let length =
            u16::try_from(tup_size).map_err(|_| StorageError::tuple_too_large(tup_size))?;
        let sp = SlotPointer { offset, length };

        let slot_index = if let Some(i) = reused {
            self.body.slot_pointers[i] = sp;
            self.body.tuples[i] = Some(tuple);
            i
        } else {
            self.body.slot_pointers.push(sp);
            self.body.tuples.push(Some(tuple));
            self.body.slot_pointers.len() - 1
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
        self.body.slot_pointers[index] = SlotPointer::default();
        self.body.tuples[index] = None;
        Ok(())
    }

    /// Checks if the slot at `slot_id` is empty (deleted or unused).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::SlotOutOfBounds`] if `slot_id` is not valid.
    fn is_slot_empty(&self, slot_id: SlotId) -> Result<bool, StorageError> {
        self.check_slot_bounds(slot_id)?;
        Ok(self.body.slot_pointers[usize::from(slot_id)].is_tombstone())
    }

    /// Marks multiple slots as deleted.
    ///
    /// Stops and returns an error on the first out-of-bounds or already-empty slot.
    pub fn delete_many(&mut self, slots: &[SlotId]) -> Result<u32, StorageError> {
        let mut count = 0u32;
        for &slot_id in slots {
            self.delete_tuple(slot_id)?;
            count += 1;
        }
        Ok(count)
    }

    /// Inserts up to [`remaining_capacity`] tuples from `tuples`, returning
    /// their [`SlotId`]s.
    ///
    /// [`remaining_capacity`]: Self::remaining_capacity
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

    /// Compacts the page by packing live tuples contiguously at the high end,
    /// recovering the space left by deleted slots.
    ///
    /// # Errors
    ///
    /// - [`StorageError::ParseError`] if the repack would underflow the offset.
    pub fn compact(&mut self) -> Result<(), StorageError> {
        let mut cursor = PAGE_SIZE;
        for (sp, tup) in self
            .body
            .slot_pointers
            .iter_mut()
            .zip(self.body.tuples.iter())
        {
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
        self.body.tuple_start = cursor;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tuple::{Field, TupleSchema},
        types::{Type, Value},
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

    fn empty_page(schema: &TupleSchema) -> HeapPage {
        HeapPage::heap_new(&[0u8; PAGE_SIZE], Arc::new(schema.clone())).unwrap()
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
        assert_eq!(page.body.tuple_start, PAGE_SIZE);
    }

    #[test]
    fn new_rejects_wrong_size_data() {
        let s = schema();
        let result = HeapPage::heap_new(&[0u8; 100], Arc::new(s.clone()));
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
        let before = page.body.tuple_start;
        page.insert_tuple(make_tuple(1, true)).unwrap();
        assert!(page.body.tuple_start < before);
        assert_eq!(before - page.body.tuple_start, s.serialized_size());
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
        let wrong = Tuple::new(vec![Value::Int32(1)]);
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
        let restored = HeapPage::heap_new(&bytes, Arc::new(s.clone())).unwrap();

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
        let restored = HeapPage::heap_new(&bytes, Arc::new(s.clone())).unwrap();

        assert_eq!(restored.num_slots(), page.num_slots());
        assert_eq!(restored.body.tuple_start, page.body.tuple_start);
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

        let bytes = page.page_data();
        let restored = HeapPage::heap_new(&bytes, Arc::new(s.clone())).unwrap();
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
        let restored = HeapPage::heap_new(&bytes, Arc::new(s.clone())).unwrap();

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
        let restored = HeapPage::heap_new(&bytes, Arc::new(s.clone())).unwrap();

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
        assert_eq!(page.num_slots(), 2);
    }

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
        let bad = Tuple::new(vec![Value::Int32(99)]);

        let mut batch = vec![good1, good2, bad].into_iter();
        let result = page.insert_many(&mut batch);

        assert!(result.is_err());
        assert_eq!(page.live_tuples().count(), 2);
    }

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

        let result = page.delete_many(&[s0, SlotId(9999)]);
        assert!(result.is_err());
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

    #[test]
    fn before_image_captures_construction_state() {
        let s = schema();
        let mut page = empty_page(&s);
        let before = page.before_image().unwrap();

        page.insert_tuple(make_tuple(1, true)).unwrap();
        assert_eq!(page.before_image().unwrap(), before);
    }

    #[test]
    fn set_before_image_snapshots_current_state() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();

        let after_insert = page.page_data();
        page.set_before_image();

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
        assert_eq!(page.before_image().unwrap(), snapshot);
        assert_ne!(page.page_data(), snapshot);
    }

    #[test]
    fn compact_on_empty_page_is_noop() {
        let s = schema();
        let mut page = empty_page(&s);
        let tuple_start_before = page.body.tuple_start;
        page.compact().unwrap();
        assert_eq!(page.body.tuple_start, tuple_start_before);
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
        let tuple_start_before = page.body.tuple_start;

        page.compact().unwrap();

        assert_eq!(page.body.tuple_start, tuple_start_before);
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
        let before = page.body.tuple_start;

        page.delete_tuple(s1).unwrap();
        assert_eq!(page.body.tuple_start, before);

        page.compact().unwrap();
        assert_eq!(page.body.tuple_start, before + tup_size);
    }

    #[test]
    fn compact_recovers_space_after_head_delete() {
        let s = schema();
        let mut page = empty_page(&s);
        let s0 = page.insert_tuple(make_tuple(1, true)).unwrap();
        page.insert_tuple(make_tuple(2, false)).unwrap();
        let tup_size = s.serialized_size();
        let before = page.body.tuple_start;

        page.delete_tuple(s0).unwrap();
        page.compact().unwrap();
        assert_eq!(page.body.tuple_start, before + tup_size);
    }

    #[test]
    fn compact_recovers_space_after_tail_delete() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        let s1 = page.insert_tuple(make_tuple(2, false)).unwrap();
        let tup_size = s.serialized_size();
        let before = page.body.tuple_start;

        page.delete_tuple(s1).unwrap();
        page.compact().unwrap();
        assert_eq!(page.body.tuple_start, before + tup_size);
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
        let restored = HeapPage::heap_new(&bytes, Arc::new(s.clone())).unwrap();

        assert_eq!(restored.live_tuples().count(), 1);
        assert_eq!(restored.num_slots(), 3);
        assert_eq!(restored.empty_slots(), 2);
        assert_eq!(restored.body.tuple_start, page.body.tuple_start);
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
        let tuple_start_after_first = page.body.tuple_start;

        page.compact().unwrap();
        assert_eq!(page.page_data(), bytes_after_first);
        assert_eq!(page.body.tuple_start, tuple_start_after_first);
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

        assert_eq!(page.body.tuple_start, PAGE_SIZE);
        assert_eq!(page.live_tuples().count(), 0);
        assert_eq!(page.num_slots(), 2);
    }

    #[test]
    fn page_data_includes_valid_checksum() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        let bytes = page.page_data();
        assert!(HeapPage::heap_new(&bytes, Arc::new(s.clone())).is_ok());
    }

    #[test]
    fn corrupted_tuple_byte_is_rejected() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(42, true)).unwrap();
        let mut bytes = page.page_data();
        bytes[PAGE_SIZE - 1] ^= 0x01;
        assert!(matches!(
            HeapPage::heap_new(&bytes, Arc::new(s.clone())),
            Err(StorageError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn corrupted_header_byte_is_rejected() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        let mut bytes = page.page_data();
        bytes[0] ^= 0xFF;
        assert!(matches!(
            HeapPage::heap_new(&bytes, Arc::new(s.clone())),
            Err(StorageError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn corrupted_checksum_byte_is_rejected() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        let mut bytes = page.page_data();
        bytes[1] ^= 0xFF;
        assert!(matches!(
            HeapPage::heap_new(&bytes, Arc::new(s.clone())),
            Err(StorageError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn empty_zeroed_buffer_still_loads() {
        let s = schema();
        let page = HeapPage::heap_new(&[0u8; PAGE_SIZE], Arc::new(s.clone())).unwrap();
        assert_eq!(page.live_tuples().count(), 0);
    }

    #[test]
    fn fresh_page_has_invalid_page_lsn() {
        let s = schema();
        let page = empty_page(&s);
        assert_eq!(page.page_lsn(), Lsn::INVALID);
    }

    #[test]
    fn set_page_lsn_is_visible_through_getter() {
        let s = schema();
        let mut page = empty_page(&s);
        let lsn = Lsn(42);
        page.set_page_lsn(lsn);
        assert_eq!(page.page_lsn(), lsn);
    }

    #[test]
    fn page_lsn_survives_serialise_deserialise_roundtrip() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();

        let lsn = Lsn(999);
        page.set_page_lsn(lsn);

        let bytes = page.page_data();
        let restored = HeapPage::heap_new(&bytes, Arc::new(s.clone())).unwrap();
        assert_eq!(restored.page_lsn(), lsn);
    }

    #[test]
    fn page_lsn_zero_on_fresh_page_survives_roundtrip() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(7, false)).unwrap();

        let bytes = page.page_data();
        let restored = HeapPage::heap_new(&bytes, Arc::new(s.clone())).unwrap();
        assert_eq!(restored.page_lsn(), Lsn::INVALID);
    }

    #[test]
    fn page_lsn_is_covered_by_checksum() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(1, true)).unwrap();
        page.set_page_lsn(Lsn(1));

        let mut bytes = page.page_data();
        bytes[5] ^= 0x01;
        assert!(
            matches!(
                HeapPage::heap_new(&bytes, Arc::new(s.clone())),
                Err(StorageError::ChecksumMismatch { .. })
            ),
            "mutating page_lsn bytes must break checksum verification"
        );
    }

    #[test]
    fn page_lsn_increases_monotonically_across_mutations() {
        let s = schema();
        let mut page = empty_page(&s);

        page.set_page_lsn(Lsn(10));
        assert_eq!(page.page_lsn(), Lsn(10));
        page.set_page_lsn(Lsn(20));
        assert!(page.page_lsn() > Lsn(10));
        page.set_page_lsn(Lsn(30));
        assert!(page.page_lsn() > Lsn(20));
    }

    #[test]
    fn set_page_lsn_does_not_disturb_tuple_data() {
        let s = schema();
        let mut page = empty_page(&s);
        page.insert_tuple(make_tuple(99, true)).unwrap();
        page.insert_tuple(make_tuple(100, false)).unwrap();

        let before = page.page_data();
        page.set_page_lsn(Lsn(512));
        let after = page.page_data();

        assert_eq!(before[PAGE_HDR_SIZE..], after[PAGE_HDR_SIZE..]);
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
            .body
            .slot_pointers
            .iter()
            .filter(|sp| !sp.is_tombstone())
            .map(|sp| sp.offset as usize)
            .collect();
        live_offsets.sort_unstable();
        assert_eq!(live_offsets.len(), 2);
        assert_eq!(live_offsets[1] + tup_size, PAGE_SIZE);
        assert_eq!(live_offsets[0] + tup_size, live_offsets[1]);
        assert_eq!(page.body.tuple_start, live_offsets[0]);
    }
}
