"""Test helpers: a tiny WAL writer that produces bytes matching the Rust encoder.

We keep this in the test tree (not in the package) so production code stays
read-only — the writer is only here to generate fixtures.
"""

from __future__ import annotations

import binascii
import io
import struct

HEADER_FMT = "<BQQQQII"
PAGE_ID_FMT = "<QI"

BEGIN = 0
COMMIT = 1
ABORT = 2
UPDATE = 3
INSERT = 4
DELETE = 5
CHECKPOINT_BEGIN = 6
CHECKPOINT_END = 7
CLR = 8

LSN_INVALID = 0xFFFF_FFFF_FFFF_FFFF
TID_INVALID = 0xFFFF_FFFF_FFFF_FFFF


def _image(buf: io.BytesIO, data: bytes | None) -> None:
    if data is None:
        buf.write(struct.pack("<I", 0))
        return
    buf.write(struct.pack("<I", len(data)))
    buf.write(data)


def _page_id(file_id: int, page_no: int) -> bytes:
    return struct.pack(PAGE_ID_FMT, file_id, page_no)


def encode_record(
    *,
    rtype: int,
    lsn: int,
    prev_lsn: int,
    tid: int,
    timestamp: int = 0,
    body: bytes = b"",
) -> bytes:
    checksum = binascii.crc32(body) & 0xFFFF_FFFF
    header = struct.pack(
        HEADER_FMT, rtype, lsn, prev_lsn, tid, timestamp, len(body), checksum
    )
    return header + body


def begin(lsn: int, tid: int) -> bytes:
    return encode_record(rtype=BEGIN, lsn=lsn, prev_lsn=LSN_INVALID, tid=tid)


def commit(lsn: int, prev_lsn: int, tid: int) -> bytes:
    return encode_record(rtype=COMMIT, lsn=lsn, prev_lsn=prev_lsn, tid=tid)


def abort(lsn: int, prev_lsn: int, tid: int) -> bytes:
    return encode_record(rtype=ABORT, lsn=lsn, prev_lsn=prev_lsn, tid=tid)


def insert(
    lsn: int, prev_lsn: int, tid: int, file_id: int, page_no: int, after: bytes
) -> bytes:
    buf = io.BytesIO()
    buf.write(_page_id(file_id, page_no))
    _image(buf, after)
    return encode_record(
        rtype=INSERT, lsn=lsn, prev_lsn=prev_lsn, tid=tid, body=buf.getvalue()
    )


def update(
    lsn: int,
    prev_lsn: int,
    tid: int,
    file_id: int,
    page_no: int,
    before: bytes,
    after: bytes,
) -> bytes:
    buf = io.BytesIO()
    buf.write(_page_id(file_id, page_no))
    _image(buf, before)
    _image(buf, after)
    return encode_record(
        rtype=UPDATE, lsn=lsn, prev_lsn=prev_lsn, tid=tid, body=buf.getvalue()
    )


def clr(
    lsn: int,
    prev_lsn: int,
    tid: int,
    file_id: int,
    page_no: int,
    after: bytes,
    undo_next_lsn: int,
) -> bytes:
    buf = io.BytesIO()
    buf.write(_page_id(file_id, page_no))
    _image(buf, after)
    buf.write(struct.pack("<Q", undo_next_lsn))
    return encode_record(
        rtype=CLR, lsn=lsn, prev_lsn=prev_lsn, tid=tid, body=buf.getvalue()
    )


def build_wal(*record_bytes: bytes) -> bytes:
    """Concatenate records and assert their LSNs match file offsets."""
    out = b""
    offset = 0
    for rb in record_bytes:
        # sanity: parse header's lsn and check it matches offset
        (_type, lsn, *_rest) = struct.unpack(HEADER_FMT, rb[: struct.calcsize(HEADER_FMT)])
        assert lsn == offset, f"fixture bug: lsn={lsn} but offset={offset}"
        out += rb
        offset += len(rb)
    return out
