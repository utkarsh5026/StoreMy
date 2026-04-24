"""Dataclasses for parsed WAL records.

Kept deliberately dumb — these are plain data containers. Parsing lives in
`parser.py`, analysis in `chains.py`, rendering in `views/`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum


class LogRecordType(IntEnum):
    BEGIN = 0
    COMMIT = 1
    ABORT = 2
    UPDATE = 3
    INSERT = 4
    DELETE = 5
    CHECKPOINT_BEGIN = 6
    CHECKPOINT_END = 7
    CLR = 8

    @property
    def has_body(self) -> bool:
        return self in {
            LogRecordType.UPDATE,
            LogRecordType.INSERT,
            LogRecordType.DELETE,
            LogRecordType.CLR,
        }


@dataclass(frozen=True, slots=True)
class PageId:
    file_id: int
    page_no: int

    def __str__(self) -> str:
        return f"PageId(file={self.file_id}, page={self.page_no})"


@dataclass(slots=True)
class LogRecord:
    """A single WAL record as read from disk.

    `offset` is the byte offset in the file where this record starts. For a
    valid WAL, `offset == lsn` — keeping both lets us sanity-check that.
    """

    offset: int
    lsn: int
    prev_lsn: int
    tid: int
    record_type: LogRecordType
    timestamp: int
    body_len: int
    checksum: int
    checksum_ok: bool

    page_id: PageId | None = None
    before: bytes | None = None
    after: bytes | None = None
    undo_next_lsn: int | None = None

    # Diagnostic flags set by the parser when it encounters non-fatal issues.
    warnings: list[str] = field(default_factory=list)

    @property
    def total_size(self) -> int:
        from .format import HEADER_SIZE

        return HEADER_SIZE + self.body_len
