# StoreMy on-disk formats (Python-side spec)

This file mirrors what the Rust writer produces. If the Rust side changes, update
this file **and** the Python parser in the same commit, or the visualizer will
silently lie. All integers are little-endian.

## Primitives

| Type            | Size | Layout    |
|-----------------|------|-----------|
| `u8`            | 1    | byte      |
| `u32`           | 4    | LE        |
| `u64`           | 8    | LE        |
| `Lsn`           | 8    | `u64`     |
| `TransactionId` | 8    | `u64`     |
| `FileId`        | 8    | `u64`     |
| `PageNumber`    | 4    | `u32`     |
| `PageId`        | 12   | `FileId` + `PageNumber` |

Sentinel values:

- `Lsn::INVALID` = `u64::MAX` (`0xFFFF_FFFF_FFFF_FFFF`)
- `TransactionId::INVALID` = `u64::MAX`

## WAL — `LogRecordHeader` (41 bytes, fixed)

| Offset | Size | Field         | Type |
|--------|------|---------------|------|
| 0      | 1    | `record_type` | `u8` |
| 1      | 8    | `lsn`         | `u64` |
| 9      | 8    | `prev_lsn`    | `u64` |
| 17     | 8    | `tid`         | `u64` |
| 25     | 8    | `timestamp`   | `u64` (unix seconds) |
| 33     | 4    | `body_len`    | `u32` |
| 37     | 4    | `checksum`    | `u32` (CRC32 of body bytes only) |

## WAL — `LogRecordType` discriminants

| Value | Variant            | Has body? |
|-------|--------------------|-----------|
| 0     | `Begin`            | no        |
| 1     | `Commit`           | no        |
| 2     | `Abort`            | no        |
| 3     | `Update`           | yes       |
| 4     | `Insert`           | yes       |
| 5     | `Delete`           | yes       |
| 6     | `CheckpointBegin`  | no        |
| 7     | `CheckpointEnd`    | no        |
| 8     | `Clr`              | yes       |

## WAL — `LogRecordBody`

Length-prefixed images use a `u32` length followed by that many bytes. A length
of `0` means "no image".

```
Update  : PageId | image(before) | image(after)
Insert  : PageId | image(after)
Delete  : PageId | image(before)
Clr     : PageId | image(after) | Lsn(undo_next_lsn)
```

All other variants have zero-byte bodies.

## WAL file

A WAL file is a bare sequential stream of `LogRecord`s starting at offset 0.
There is **no** file header or magic bytes. The LSN of a record equals its
byte offset in the file — LSNs double as file positions.

## Heap file

A heap file is a concatenation of `PAGE_SIZE` (4096-byte) pages with no file
header. Page `n` lives at byte offset `n * PAGE_SIZE`.

### Heap page header (5 bytes, fixed)

| Offset | Size | Field         | Type | Notes                                     |
|--------|------|---------------|------|-------------------------------------------|
| 0      | 1    | `version`     | `u8` | `HEAP_PAGE_VERSION` (currently `1`).      |
| 1      | 2    | `num_slots`   | `u16` | count of slot pointers that follow.      |
| 3      | 2    | `tuple_start` | `u16` | left edge of the tuple region.            |

A page whose bytes are entirely zero is treated as an empty page at the
current version (this is how the buffer pool-extended-but-never-written
page looks). Any non-blank page whose `version` byte does not match
`HEAP_PAGE_VERSION` MUST be rejected.

### Slot pointer array

Immediately after the header, `num_slots` entries of 4 bytes each:

| Offset (in entry) | Size | Field    | Type  |
|-------------------|------|----------|-------|
| 0                 | 2    | `offset` | `u16` |
| 2                 | 2    | `length` | `u16` |

`length == 0` marks the slot as **tombstoned** (empty, reusable). Otherwise
the tuple occupies `page[offset .. offset + length)`, which must lie within
`[tuple_start, PAGE_SIZE)`.

### Page regions

```
[0 .. 5)                         header
[5 .. 5 + num_slots*4)           slot pointer array (grows forward)
[5 + num_slots*4 .. tuple_start) free space
[tuple_start .. PAGE_SIZE)       tuple data (grows backward)
```

### Tuple encoding (same as WAL image bytes)

```
[null bitmap: ceil(field_count / 8) bytes]   bit i = 1 means field i is NULL
[for each non-null field: 1-byte type tag + payload]
```

Type tags (must stay in sync with `impl Encode for Value` in
`db/src/types.rs`):

| Tag | Type      | Payload                                 |
|-----|-----------|-----------------------------------------|
| 0   | `Null`    | —                                       |
| 1   | `Int32`   | `i32` LE (4 bytes)                      |
| 2   | `Int64`   | `i64` LE (8 bytes)                      |
| 3   | `Uint32`  | `u32` LE (4 bytes)                      |
| 4   | `Uint64`  | `u64` LE (8 bytes)                      |
| 5   | `Float64` | `f64` LE (8 bytes)                      |
| 6   | `Bool`    | 1 byte (`0` = false, otherwise true)    |
| 7   | `String`  | `u32` LE length + UTF-8 bytes           |

## Dumper JSON schema (v1)

`storemy-heap-dump` is the Rust binary that translates heap files into JSON
for the Python visualizer. Python parses the JSON, **not** the raw page
bytes — this is deliberate (see architecture notes in the repo root), so
that new `Value` tags or header changes do not require a Python update.

Python MUST refuse any dump whose `schema_version` does not match a version
it supports.

Top-level shape:

```json
{
  "schema_version": 1,
  "page_size": 4096,
  "page_count": <usize>,
  "field_count": <usize>,
  "field_names": ["id", "name", ...] | null,
  "pages": [<PageDump>, ...]
}
```

`PageDump`:

```json
{
  "page_no": <usize>,
  "format_version": <u8>,
  "header": { "num_slots": <u16>, "tuple_start": <u16>, "raw_hex": "..." },
  "slot_array_end": <usize>,
  "free_bytes": <i64>,
  "used_bytes": <usize>,
  "slots": [<SlotDump>, ...],
  "page_error": null | "unsupported page version: got X, expected Y"
}
```

`SlotDump` is a tagged union on `status`:

```json
{"status": "live",       "slot_id": N, "offset": N, "length": N, "raw_hex": "...", "decode": <DecodeResult>}
{"status": "tombstone",  "slot_id": N, "offset": 0, "length": 0}
{"status": "out_of_range","slot_id": N, "offset": N, "length": N, "error": "..."}
```

`DecodeResult` is tagged on `ok`:

```json
{"ok": "true",  "null_bitmap_hex": "...", "fields": [<DecodedField>, ...]}
{"ok": "false", "error": "...", "null_bitmap_hex": "..." | null, "fields_so_far": [...]}
```

`DecodedField`:

```json
{
  "index": <usize>,
  "name": <string> | null,
  "type": "Int32" | "Int64" | "String" | ...,
  "value": <json>,
  "byte_range": [<start>, <end>]
}
```

`byte_range` is relative to the tuple's first byte (not the page). A reader
that encounters an unknown `type` string should render it as-is — this is
the extension point for new `Value` variants.

## Hash index file

A hash index file is a concatenation of `PAGE_SIZE` (4096-byte) pages with
no file header. The index uses **static (linear) hashing with separate
chaining**: a fixed array of `num_buckets` *head* pages, each of which can
spill into a chain of *overflow* pages.

### Page layout

Every page (head or overflow) shares the same on-disk shape:

```text
[envelope: kind(1) + crc(4)]                  5 bytes
[bucket header: bucket_num(4) | n(4) | overflow(4)]  12 bytes
[n × IndexEntry]
```

`overflow` is the page number of the next page in the chain, or
`0xFFFF_FFFF` (NIL) at the chain tail. `crc` is CRC32 over the bytes
following the envelope.

The Rust types live in [`db/src/index/hash.rs`](db/src/index/hash.rs); see
`HashBucket::HEADER_SIZE`.

### Hash dumper JSON schema (v1)

Like the heap dumper, the Python visualizer parses JSON only — the
(future) `storemy-hash-dump` Rust binary is responsible for translating
raw pages into the shape below.

Top-level shape:

```json
{
  "schema_version": 1,
  "page_size": 4096,
  "page_count": <usize>,
  "num_buckets": <usize>,
  "key_types": ["Int32", "String", ...] | null,
  "pages":   [<PageDump>, ...],
  "buckets": [<BucketDump>, ...]
}
```

`PageDump`:

```json
{
  "page_no":     <usize>,
  "kind":        "head" | "overflow" | "unknown",
  "crc_ok":      <bool>,
  "bucket_num":  <u32>,
  "entry_count": <u32>,
  "overflow":    <u32> | null,
  "entries":    [<Entry>, ...],
  "page_error": null | "<reason>"
}
```

`Entry`:

```json
{
  "key": ["<stringified>", ...],
  "rid": [<page_no>, <slot_id>]
}
```

The dumper renders each `CompositeKey` component to a string (matching
the heap dumper's policy of keeping `Value` tag knowledge on the Rust
side). Python preserves them verbatim; new key types do not require a
Python update.

`BucketDump` is a *derived* view — the dumper walks each head page's
overflow chain and emits one entry per logical bucket so the Python
side does not have to re-walk:

```json
{
  "bucket_num":    <u32>,
  "chain":         [<page_no>, ...],
  "total_entries": <u32>,
  "capacity":      <u32>
}
```

`chain` is in walk order, head first. `capacity` is the maximum entry
count that fits in a single page, used to compute fill ratios.
