"""Binary WAL parser.

Reads a StoreMy WAL file into `LogRecord` objects. No dependency on the Rust
side at runtime — we re-read the format spec directly. See FORMAT.md.
"""

from __future__ import annotations

import binascii
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from .format import (
    HEADER_SIZE,
    HEADER_STRUCT,
    IMAGE_LEN_SIZE,
    IMAGE_LEN_STRUCT,
    PAGE_ID_SIZE,
    PAGE_ID_STRUCT,
    U64_STRUCT,
)
from .records import LogRecord, LogRecordType, PageId


class ParseError(Exception):
    """Raised when the WAL cannot be parsed further.

    The parser tries to be forgiving (truncated trailing records are reported
    as warnings, not errors) but bails on unrecoverable corruption like an
    unknown record-type discriminant.
    """


def read_file(path: str | Path) -> list[LogRecord]:
    """Read a full WAL file into memory. Convenient for small dev files."""
    with open(path, "rb") as f:
        return list(iter_records(f))


def iter_records(stream: BinaryIO) -> Iterator[LogRecord]:
    """Yield records one at a time from an open binary stream.

    `stream` should be positioned at the start of the WAL (offset 0). Records
    with bad checksums are still yielded — inspect `.checksum_ok` — so the
    visualizer can show partially-corrupted logs.
    """
    offset = 0
    while True:
        header_bytes = _read_exact(stream, HEADER_SIZE)
        if header_bytes is None:
            return  # clean EOF

        if len(header_bytes) < HEADER_SIZE:
            # Partial header at tail — report and stop.
            raise ParseError(
                f"truncated header at offset {offset}: "
                f"got {len(header_bytes)} bytes, need {HEADER_SIZE}"
            )

        (
            type_byte,
            lsn,
            prev_lsn,
            tid,
            timestamp,
            body_len,
            checksum,
        ) = HEADER_STRUCT.unpack(header_bytes)

        try:
            rtype = LogRecordType(type_byte)
        except ValueError as e:
            raise ParseError(
                f"unknown record_type byte {type_byte} at offset {offset}"
            ) from e

        body_bytes = _read_exact(stream, body_len)
        if body_bytes is None or len(body_bytes) < body_len:
            raise ParseError(
                f"truncated body for record at offset {offset}: "
                f"need {body_len} bytes"
            )

        actual_crc = binascii.crc32(body_bytes) & 0xFFFF_FFFF
        rec = LogRecord(
            offset=offset,
            lsn=lsn,
            prev_lsn=prev_lsn,
            tid=tid,
            record_type=rtype,
            timestamp=timestamp,
            body_len=body_len,
            checksum=checksum,
            checksum_ok=(actual_crc == checksum),
        )

        if lsn != offset:
            rec.warnings.append(f"lsn {lsn} != file offset {offset}")

        _fill_body(rec, body_bytes)
        yield rec
        offset += HEADER_SIZE + body_len


# ── helpers ──────────────────────────────────────────────────────────────────


def _read_exact(stream: BinaryIO, n: int) -> bytes | None:
    """Read exactly `n` bytes. Returns None on clean EOF before any byte is read."""
    if n == 0:
        return b""
    buf = stream.read(n)
    if not buf:
        return None
    return buf


def _fill_body(rec: LogRecord, body: bytes) -> None:
    """Decode the type-specific body payload into the record's fields."""
    if not rec.record_type.has_body:
        if body:
            rec.warnings.append(
                f"expected empty body for {rec.record_type.name}, got {len(body)} bytes"
            )
        return

    pos = 0
    if len(body) < PAGE_ID_SIZE:
        raise ParseError(
            f"body too short for PageId in {rec.record_type.name} at offset {rec.offset}"
        )
    file_id, page_no = PAGE_ID_STRUCT.unpack_from(body, pos)
    pos += PAGE_ID_SIZE
    rec.page_id = PageId(file_id=file_id, page_no=page_no)

    rtype = rec.record_type
    if rtype is LogRecordType.UPDATE:
        rec.before, pos = _read_image(body, pos)
        rec.after, pos = _read_image(body, pos)
    elif rtype is LogRecordType.INSERT:
        rec.after, pos = _read_image(body, pos)
    elif rtype is LogRecordType.DELETE:
        rec.before, pos = _read_image(body, pos)
    elif rtype is LogRecordType.CLR:
        rec.after, pos = _read_image(body, pos)
        if len(body) - pos < U64_STRUCT.size:
            raise ParseError(
                f"truncated undo_next_lsn in CLR at offset {rec.offset}"
            )
        (rec.undo_next_lsn,) = U64_STRUCT.unpack_from(body, pos)
        pos += U64_STRUCT.size

    if pos != len(body):
        rec.warnings.append(f"{len(body) - pos} trailing body bytes unconsumed")


def _read_image(body: bytes, pos: int) -> tuple[bytes | None, int]:
    """Decode one length-prefixed image. Zero length → None (matches Rust encoder)."""
    if len(body) - pos < IMAGE_LEN_SIZE:
        raise ParseError(f"truncated image length prefix at body offset {pos}")
    (img_len,) = IMAGE_LEN_STRUCT.unpack_from(body, pos)
    pos += IMAGE_LEN_SIZE
    if img_len == 0:
        return None, pos
    if len(body) - pos < img_len:
        raise ParseError(
            f"truncated image: need {img_len} bytes at body offset {pos}"
        )
    img = body[pos : pos + img_len]
    return img, pos + img_len
