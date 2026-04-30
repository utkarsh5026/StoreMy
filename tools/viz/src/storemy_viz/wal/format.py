"""Byte-layout constants for the StoreMy WAL format.

Single source of truth on the Python side — every struct format string and
sentinel value lives here, not sprinkled through the parser.
"""

from __future__ import annotations

import struct

# Header: u8 type | u64 lsn | u64 prev_lsn | u64 tid | u64 ts | u32 body_len | u32 checksum
HEADER_STRUCT = struct.Struct("<BQQQQII")
HEADER_SIZE = HEADER_STRUCT.size
assert HEADER_SIZE == 41, "header size drift vs Rust LogRecordHeader::SIZE"

# PageId: u64 file_id | u32 page_no
PAGE_ID_STRUCT = struct.Struct("<QI")
PAGE_ID_SIZE = PAGE_ID_STRUCT.size

# Length prefix for images.
IMAGE_LEN_STRUCT = struct.Struct("<I")
IMAGE_LEN_SIZE = IMAGE_LEN_STRUCT.size

# u64 used for Lsn fields inside bodies (e.g. Clr.undo_next_lsn).
U64_STRUCT = struct.Struct("<Q")

LSN_INVALID = 0xFFFF_FFFF_FFFF_FFFF
TID_INVALID = 0xFFFF_FFFF_FFFF_FFFF
