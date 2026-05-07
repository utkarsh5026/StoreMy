//! Heap-page dumps for the visualization endpoint.
//!
//! Produces the JSON shape consumed by the React `HeapInspector` panel. The
//! shape stays close to the spec in `tools/viz/FORMAT.md`, with one
//! deliberate difference: that doc lists a 5-byte header ending in
//! `format_version`, but the code in [`crate::heap::page`] currently writes
//! an 8-byte header (`num_slots: u16` | `tuple_start: u16` | `checksum: u32`)
//! with no version byte. We emit what the code actually writes; the schema
//! version below is for the *dump*, not the page format.
//!
//! All multi-byte integers in the page bytes are little-endian.

use byteorder::{ByteOrder, LittleEndian};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::{
    PAGE_SIZE, codec::Decode, tuple::TupleSchema, types::Value, web::dto::query::value_to_json,
};

const PAGE_HDR_SIZE: usize = 8;
const SLOT_POINTER_SIZE: usize = 4;

/// Top-level `GET /api/heap/{table}` response.
#[derive(Debug, Serialize)]
pub struct HeapDumpDto {
    pub schema_version: u8,
    pub page_size: usize,
    pub page_count: usize,
    pub field_count: usize,
    pub field_names: Vec<String>,
    pub table: String,
    pub pages: Vec<HeapPageDto>,
}

/// One heap page in a [`HeapDumpDto`].
#[derive(Debug, Serialize)]
pub struct HeapPageDto {
    pub page_no: usize,
    pub header: PageHeaderDto,
    /// First byte after the slot-pointer array — i.e. start of the free region.
    pub slot_array_end: usize,
    /// `tuple_start - slot_array_end` (or 0 if the page is over-full).
    pub free_bytes: usize,
    /// `PAGE_SIZE - tuple_start` — bytes occupied by tuple data (including
    /// holes from tombstoned slots).
    pub used_bytes: usize,
    pub slots: Vec<HeapSlotDto>,
    /// `Some(message)` when the page bytes are inconsistent (e.g. a slot
    /// pointer goes outside the tuple region). The dump is still useful in
    /// that case — the React side can render a warning banner.
    pub page_error: Option<String>,
    /// Whether the page is a freshly-allocated all-zero buffer (no checksum,
    /// no slots). Useful so the UI can label it "empty (never written)".
    pub blank: bool,
}

#[derive(Debug, Serialize)]
pub struct PageHeaderDto {
    pub num_slots: u16,
    pub tuple_start: u16,
    pub checksum: u32,
    /// First `PAGE_HDR_SIZE` bytes of the page in lower-case hex.
    pub raw_hex: String,
}

/// Tagged union: `status` discriminates the variant.
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum HeapSlotDto {
    /// Live slot: `length > 0` and the (offset, length) range lies inside
    /// `[tuple_start, PAGE_SIZE)`.
    Live {
        slot_id: u16,
        offset: u16,
        length: u16,
        raw_hex: String,
        decode: SlotDecodeDto,
    },
    /// `length == 0` — the slot is reusable but currently empty.
    Tombstone {
        slot_id: u16,
        offset: u16,
        length: u16,
    },
    /// `length > 0` but the (offset, length) range is invalid for this page.
    OutOfRange {
        slot_id: u16,
        offset: u16,
        length: u16,
        error: String,
    },
}

/// Per-slot decode result. `ok=false` lets the UI render the raw bytes plus
/// a friendly error rather than failing the whole page.
#[derive(Debug, Serialize)]
#[serde(tag = "ok")]
pub enum SlotDecodeDto {
    #[serde(rename = "true")]
    Ok {
        null_bitmap_hex: String,
        fields: Vec<DecodedFieldDto>,
    },
    #[serde(rename = "false")]
    Err {
        error: String,
        null_bitmap_hex: Option<String>,
        fields_so_far: Vec<DecodedFieldDto>,
    },
}

#[derive(Debug, Serialize)]
pub struct DecodedFieldDto {
    pub index: usize,
    pub name: String,
    /// Type name as seen in catalog (`"INT"`, `"VARCHAR"`, …) or `"NULL"`.
    pub r#type: String,
    pub value: JsonValue,
}

impl HeapDumpDto {
    /// Builds a complete dump from per-page raw byte buffers.
    ///
    /// `pages[i]` must be the raw bytes of page `i`; the caller is responsible
    /// for fetching them under a shared lock.
    pub fn build(table: &str, schema: &TupleSchema, pages: Vec<[u8; PAGE_SIZE]>) -> Self {
        let field_names: Vec<String> = schema
            .fields()
            .map(|f| f.name.as_str().to_owned())
            .collect();
        let field_count = field_names.len();
        let pages = pages
            .into_iter()
            .enumerate()
            .map(|(i, bytes)| HeapPageDto::parse(i, &bytes, schema))
            .collect();
        Self {
            schema_version: 1,
            page_size: PAGE_SIZE,
            page_count: field_count.max(0), // placeholder, fixed below
            field_count,
            field_names,
            table: table.to_string(),
            pages,
        }
    }

    /// Convenience constructor that fixes up `page_count`.
    pub fn build_full(table: &str, schema: &TupleSchema, pages: Vec<[u8; PAGE_SIZE]>) -> Self {
        let mut out = Self::build(table, schema, pages);
        out.page_count = out.pages.len();
        out
    }
}

impl HeapPageDto {
    fn parse(page_no: usize, bytes: &[u8; PAGE_SIZE], schema: &TupleSchema) -> Self {
        // Convention from heap/page.rs: a freshly zeroed buffer is treated as
        // an empty page that's never been written. Rather than synthesising a
        // header, surface that explicitly to the UI.
        if bytes.iter().all(|&b| b == 0) {
            return HeapPageDto::blank(page_no);
        }

        let num_slots = LittleEndian::read_u16(&bytes[0..2]);
        let tuple_start = LittleEndian::read_u16(&bytes[2..4]);
        let checksum = LittleEndian::read_u32(&bytes[4..8]);
        let header = PageHeaderDto {
            num_slots,
            tuple_start,
            checksum,
            raw_hex: hex(&bytes[..PAGE_HDR_SIZE]),
        };
        let slot_array_end = PAGE_HDR_SIZE + usize::from(num_slots) * SLOT_POINTER_SIZE;

        // Defensive: a torn / inconsistent header should not panic the
        // handler. Surface page_error and emit no slot details.
        let mut page_error: Option<String> = None;
        if slot_array_end > PAGE_SIZE {
            page_error = Some(format!(
                "slot array extends past page: end={slot_array_end}, page_size={PAGE_SIZE}"
            ));
        }
        if usize::from(tuple_start) > PAGE_SIZE {
            page_error.get_or_insert_with(|| {
                format!("tuple_start={tuple_start} exceeds PAGE_SIZE={PAGE_SIZE}")
            });
        }
        if usize::from(tuple_start) < slot_array_end && page_error.is_none() {
            page_error = Some(format!(
                "tuple_start={tuple_start} overlaps slot array (ends at {slot_array_end})"
            ));
        }

        let used_bytes = PAGE_SIZE.saturating_sub(usize::from(tuple_start));
        let free_bytes = usize::from(tuple_start).saturating_sub(slot_array_end);

        let slots = if page_error.is_some() {
            Vec::new()
        } else {
            (0..num_slots)
                .map(|i| {
                    // Slot i lives in [PAGE_HDR_SIZE + i*4 .. PAGE_HDR_SIZE + i*4 + 4).
                    let off = PAGE_HDR_SIZE + usize::from(i) * SLOT_POINTER_SIZE;
                    let slot_offset = LittleEndian::read_u16(&bytes[off..off + 2]);
                    let slot_length = LittleEndian::read_u16(&bytes[off + 2..off + 4]);
                    decode_slot(i, slot_offset, slot_length, tuple_start, bytes, schema)
                })
                .collect()
        };

        HeapPageDto {
            page_no,
            header,
            slot_array_end,
            free_bytes,
            used_bytes,
            slots,
            page_error,
            blank: false,
        }
    }

    fn blank(page_no: usize) -> Self {
        HeapPageDto {
            page_no,
            header: PageHeaderDto {
                num_slots: 0,
                tuple_start: 0,
                checksum: 0,
                raw_hex: "00".repeat(PAGE_HDR_SIZE),
            },
            slot_array_end: PAGE_HDR_SIZE,
            free_bytes: PAGE_SIZE - PAGE_HDR_SIZE,
            used_bytes: 0,
            slots: Vec::new(),
            page_error: None,
            blank: true,
        }
    }
}

fn decode_slot(
    slot_id: u16,
    offset: u16,
    length: u16,
    tuple_start: u16,
    bytes: &[u8; PAGE_SIZE],
    schema: &TupleSchema,
) -> HeapSlotDto {
    if length == 0 {
        return HeapSlotDto::Tombstone {
            slot_id,
            offset,
            length,
        };
    }
    let off = usize::from(offset);
    let len = usize::from(length);
    let end = off.saturating_add(len);
    if off < usize::from(tuple_start) || end > PAGE_SIZE {
        return HeapSlotDto::OutOfRange {
            slot_id,
            offset,
            length,
            error: format!(
                "tuple bytes [{off}..{end}) outside region [{tuple_start}..{PAGE_SIZE})"
            ),
        };
    }
    let body = &bytes[off..end];
    let raw_hex = hex(body);
    let decode = decode_tuple_bytes(body, schema);
    HeapSlotDto::Live {
        slot_id,
        offset,
        length,
        raw_hex,
        decode,
    }
}

fn decode_tuple_bytes(body: &[u8], schema: &TupleSchema) -> SlotDecodeDto {
    let bitmap_size = schema.physical_num_fields().div_ceil(8);
    if body.len() < bitmap_size {
        return SlotDecodeDto::Err {
            error: format!(
                "tuple body too short for null bitmap: have {}, need >= {}",
                body.len(),
                bitmap_size
            ),
            null_bitmap_hex: None,
            fields_so_far: Vec::new(),
        };
    }
    let bitmap = &body[..bitmap_size];
    let bitmap_hex = hex(bitmap);
    let mut cursor = std::io::Cursor::new(&body[bitmap_size..]);
    let mut fields = Vec::with_capacity(schema.physical_num_fields());

    for (i, field) in schema.fields().enumerate() {
        let is_null = (bitmap[i / 8] & (1 << (i % 8))) != 0;
        if is_null {
            fields.push(DecodedFieldDto {
                index: i,
                name: field.name.as_str().to_owned(),
                r#type: "NULL".to_string(),
                value: JsonValue::Null,
            });
            continue;
        }
        match Value::decode(&mut cursor) {
            Ok(v) => {
                let type_name = v
                    .get_type()
                    .map_or_else(|| "NULL".to_string(), |t| t.to_string());
                fields.push(DecodedFieldDto {
                    index: i,
                    name: field.name.as_str().to_owned(),
                    r#type: type_name,
                    value: value_to_json(&v),
                });
            }
            Err(e) => {
                return SlotDecodeDto::Err {
                    error: format!("field {i} ('{}') decode failed: {e}", field.name),
                    null_bitmap_hex: Some(bitmap_hex),
                    fields_so_far: fields,
                };
            }
        }
    }

    SlotDecodeDto::Ok {
        null_bitmap_hex: bitmap_hex,
        fields,
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing directly into the existing String avoids allocating a
        // temporary formatted String for each byte.
        write!(&mut s, "{b:02x}").expect("writing into a String cannot fail");
    }
    s
}
