use std::vec;

use byteorder::{ByteOrder, LittleEndian};

use crate::{
    primitives::{PageId, SlotId},
    storage::{MAX_TUPLE_SIZE, PAGE_SIZE, Page, StorageError},
    tuple::{Tuple, TupleSchema},
};

const SLOT_POINTER_SIZE: usize = 4;

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize, "PAGE_SIZE must fit in u16");
const _: () = assert!(
    MAX_TUPLE_SIZE <= u16::MAX as usize,
    "MAX_TUPLE_SIZE must fit in u16"
);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
struct SlotPointer {
    offset: u16,
    length: u16,
}

pub struct HeapPage {
    page_id: PageId,
    schema: TupleSchema,
    tuples: Vec<Option<Tuple>>,
    slot_pointers: Vec<SlotPointer>,
    free_space_offset: usize,
    old_data: [u8; PAGE_SIZE],
    num_slots: u16,
}

impl HeapPage {
    pub fn new(page_id: PageId, data: &[u8], schema: TupleSchema) -> Result<Self, StorageError> {
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

    pub(crate) fn tuples(&self) -> impl Iterator<Item = &Tuple> {
        self.tuples.iter().filter_map(|t| t.as_ref())
    }

    pub(crate) fn empty_slots(&self) -> usize {
        self.slot_pointers
            .iter()
            .filter(|sp| sp.length == 0)
            .count()
    }

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
        self.slot_pointers[empty_slot] = SlotPointer {
            offset: u16::try_from(self.free_space_offset).unwrap_or(0),
            length: tup_size as u16,
        };

        self.tuples[empty_slot] = Some(tuple);
        empty_slot
            .try_into()
            .map_err(|_| StorageError::SlotOutOfBounds {
                slot: empty_slot as u16,
                num_slots: self.num_slots.try_into().unwrap(),
            })
    }

    pub(crate) fn delete_tuple(&mut self, slot_id: SlotId) -> Result<(), StorageError> {
        if u16::from(slot_id) >= self.num_slots {
            return Err(StorageError::slot_out_of_bounds(slot_id, self.num_slots));
        }

        if self.is_slot_empty(slot_id)? {
            return Err(StorageError::slot_already_empty(slot_id));
        }

        let index = usize::from(slot_id);
        self.slot_pointers[index] = SlotPointer::default();
        self.tuples[index] = None;
        Ok(())
    }

    fn is_slot_empty(&self, slot_id: SlotId) -> Result<bool, StorageError> {
        if u16::from(slot_id) >= self.num_slots {
            return Err(StorageError::slot_out_of_bounds(slot_id, self.num_slots));
        }

        Ok(self.slot_pointers[usize::from(slot_id)].length == 0)
    }

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
            let tup = Tuple::deserialize(&self.schema, tuple_data)
                .map_err(|e| StorageError::ParseError(format!("failed to parse tuple: {e}")))?;
            self.tuples[i] = Some(tup);
        }

        Ok(())
    }
}

impl Page for HeapPage {
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
                t.serialize(&self.schema, &mut bytes[start..end])
                    .expect("failed to serialize tuple");
            }
        }

        bytes
    }

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

    fn empty_page() -> HeapPage {
        HeapPage::new(page_id(), &[0u8; PAGE_SIZE], schema()).unwrap()
    }

    // ── construction ────────────────────────────────────────────────────────

    #[test]
    fn new_empty_page_has_no_tuples() {
        let page = empty_page();
        assert_eq!(page.tuples().count(), 0);
    }

    #[test]
    fn new_rejects_wrong_size_data() {
        let result = HeapPage::new(page_id(), &[0u8; 100], schema());
        assert!(matches!(result, Err(StorageError::InvalidPageSize { .. })));
    }

    // ── insert ───────────────────────────────────────────────────────────────

    #[test]
    fn insert_returns_slot_id() {
        let mut page = empty_page();
        let slot = page.insert_tuple(make_tuple(1, true));
        assert!(slot.is_ok());
    }

    #[test]
    fn inserted_tuple_is_visible() {
        let mut page = empty_page();
        page.insert_tuple(make_tuple(42, false)).unwrap();
        assert_eq!(page.tuples().count(), 1);
    }

    #[test]
    fn multiple_inserts_all_visible() {
        let mut page = empty_page();
        for i in 0..5 {
            page.insert_tuple(make_tuple(i, i % 2 == 0)).unwrap();
        }
        assert_eq!(page.tuples().count(), 5);
    }

    #[test]
    fn insert_schema_mismatch_is_rejected() {
        let mut page = empty_page();
        let wrong = Tuple::new(vec![Value::Int32(1)]); // missing field
        assert!(matches!(
            page.insert_tuple(wrong),
            Err(StorageError::SchemaMismatch)
        ));
    }

    // ── delete ───────────────────────────────────────────────────────────────

    #[test]
    fn delete_removes_tuple() {
        let mut page = empty_page();
        let slot = page.insert_tuple(make_tuple(7, true)).unwrap();
        page.delete_tuple(slot).unwrap();
        assert_eq!(page.tuples().count(), 0);
    }

    #[test]
    fn delete_out_of_bounds_slot_errors() {
        let mut page = empty_page();
        let bad_slot = SlotId(999);
        assert!(matches!(
            page.delete_tuple(bad_slot),
            Err(StorageError::SlotOutOfBounds { .. })
        ));
    }

    #[test]
    fn delete_already_empty_slot_errors() {
        let mut page = empty_page();
        let slot = page.insert_tuple(make_tuple(1, false)).unwrap();
        page.delete_tuple(slot).unwrap();
        assert!(matches!(
            page.delete_tuple(slot),
            Err(StorageError::SlotAlreadyEmpty { .. })
        ));
    }

    // ── round-trip ───────────────────────────────────────────────────────────

    #[test]
    fn page_data_roundtrip() {
        let mut page = empty_page();
        let t1 = make_tuple(10, true);
        let t2 = make_tuple(20, false);
        page.insert_tuple(t1.clone()).unwrap();
        page.insert_tuple(t2.clone()).unwrap();

        let bytes = page.page_data();
        let restored = HeapPage::new(page_id(), &bytes, schema()).unwrap();

        let tuples: Vec<&Tuple> = restored.tuples().collect();
        assert_eq!(tuples.len(), 2);
        assert!(tuples.contains(&&t1));
        assert!(tuples.contains(&&t2));
    }

    // ── empty_slots ──────────────────────────────────────────────────────────

    #[test]
    fn empty_slots_decreases_on_insert() {
        let mut page = empty_page();
        let before = page.empty_slots();
        page.insert_tuple(make_tuple(1, true)).unwrap();
        assert_eq!(page.empty_slots(), before - 1);
    }

    #[test]
    fn empty_slots_recovers_on_delete() {
        let mut page = empty_page();
        let before = page.empty_slots();
        let slot = page.insert_tuple(make_tuple(1, true)).unwrap();
        page.delete_tuple(slot).unwrap();
        assert_eq!(page.empty_slots(), before);
    }
}
