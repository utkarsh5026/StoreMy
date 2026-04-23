"""Dataclasses that mirror the heap-dump JSON schema (v1).

These types are a thin, typed wrapper around the JSON produced by
``storemy-heap-dump``. They intentionally do **not** parse raw heap-file
bytes — that job belongs to the Rust dumper. See ``FORMAT.md`` for the
contract.

An unknown ``type`` string on a ``DecodedField`` is preserved verbatim so
that new ``Value`` variants can be displayed without a Python update.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SUPPORTED_DUMP_SCHEMA_VERSIONS = frozenset({1})


class DumpSchemaMismatch(Exception):
    """Raised when a dump JSON uses a ``schema_version`` we do not support."""


@dataclass(frozen=True)
class DecodedField:
    index: int
    name: str | None
    type_name: str
    value: Any
    byte_range: tuple[int, int]

    @property
    def byte_len(self) -> int:
        return self.byte_range[1] - self.byte_range[0]


@dataclass(frozen=True)
class DecodeOk:
    null_bitmap_hex: str
    fields: list[DecodedField]

    ok: bool = True


@dataclass(frozen=True)
class DecodeErr:
    error: str
    null_bitmap_hex: str | None
    fields_so_far: list[DecodedField]

    ok: bool = False


Decode = DecodeOk | DecodeErr


@dataclass(frozen=True)
class LiveSlot:
    slot_id: int
    offset: int
    length: int
    raw_hex: str
    decode: Decode

    status: str = "live"


@dataclass(frozen=True)
class TombstoneSlot:
    slot_id: int
    offset: int
    length: int

    status: str = "tombstone"


@dataclass(frozen=True)
class OutOfRangeSlot:
    slot_id: int
    offset: int
    length: int
    error: str

    status: str = "out_of_range"


Slot = LiveSlot | TombstoneSlot | OutOfRangeSlot


@dataclass(frozen=True)
class PageHeader:
    num_slots: int
    tuple_start: int
    raw_hex: str


@dataclass(frozen=True)
class PageDump:
    page_no: int
    format_version: int
    header: PageHeader
    slot_array_end: int
    free_bytes: int
    used_bytes: int
    slots: list[Slot]
    page_error: str | None

    @property
    def live_count(self) -> int:
        return sum(1 for s in self.slots if isinstance(s, LiveSlot))

    @property
    def tombstone_count(self) -> int:
        return sum(1 for s in self.slots if isinstance(s, TombstoneSlot))

    def fill_ratio(self, page_size: int) -> float:
        """Fraction of the page taken up by header + slot array + tuple data."""
        if page_size <= 0:
            return 0.0
        return (self.slot_array_end + self.used_bytes) / page_size


@dataclass(frozen=True)
class Dump:
    schema_version: int
    page_size: int
    page_count: int
    field_count: int
    field_names: list[str] | None
    pages: list[PageDump] = field(default_factory=list)


def from_json(obj: dict[str, Any]) -> Dump:
    """Builds a :class:`Dump` from the parsed JSON produced by the Rust dumper.

    Raises :class:`DumpSchemaMismatch` if ``schema_version`` is not supported.
    """

    version = obj.get("schema_version")
    if version not in SUPPORTED_DUMP_SCHEMA_VERSIONS:
        raise DumpSchemaMismatch(
            f"unsupported dump schema_version: got {version!r}, "
            f"supported {sorted(SUPPORTED_DUMP_SCHEMA_VERSIONS)}"
        )

    return Dump(
        schema_version=obj["schema_version"],
        page_size=obj["page_size"],
        page_count=obj["page_count"],
        field_count=obj["field_count"],
        field_names=obj.get("field_names"),
        pages=[_page_from_json(p) for p in obj.get("pages", [])],
    )


def _page_from_json(p: dict[str, Any]) -> PageDump:
    """Parse a single page entry from the Rust heap-dump JSON.

    Expects the keys produced by the Rust dumper (e.g. ``header``, ``slots``,
    ``free_bytes``). This is an internal helper; schema/version validation is
    handled by :func:`from_json`.
    """
    hdr = p["header"]
    return PageDump(
        page_no=p["page_no"],
        format_version=p["format_version"],
        header=PageHeader(
            num_slots=hdr["num_slots"],
            tuple_start=hdr["tuple_start"],
            raw_hex=hdr["raw_hex"],
        ),
        slot_array_end=p["slot_array_end"],
        free_bytes=p["free_bytes"],
        used_bytes=p["used_bytes"],
        slots=[_slot_from_json(s) for s in p.get("slots", [])],
        page_error=p.get("page_error"),
    )


def _slot_from_json(s: dict[str, Any]) -> Slot:
    """Parse a slot entry from the Rust heap-dump JSON.

    The Rust side tags slots with ``status``:
    - ``"live"``: includes raw bytes and a nested decode result
    - ``"tombstone"``: deleted slot metadata only
    - ``"out_of_range"``: slot metadata plus an error string

    Raises :class:`ValueError` for unknown ``status`` values.
    """
    status = s["status"]
    if status == "live":
        return LiveSlot(
            slot_id=s["slot_id"],
            offset=s["offset"],
            length=s["length"],
            raw_hex=s["raw_hex"],
            decode=_decode_from_json(s["decode"]),
        )
    if status == "tombstone":
        return TombstoneSlot(slot_id=s["slot_id"], offset=s["offset"], length=s["length"])
    if status == "out_of_range":
        return OutOfRangeSlot(
            slot_id=s["slot_id"],
            offset=s["offset"],
            length=s["length"],
            error=s["error"],
        )
    raise ValueError(f"unknown slot status: {status!r}")


def _decode_from_json(d: dict[str, Any]) -> Decode:
    """Parse the nested tuple decode result for a live slot.

    The Rust dumper emits a tagged union keyed by ``ok``; historically this has
    been serialized as a string (``"true"``/``"false"``) rather than a boolean.
    We accept either representation.
    """
    ok_raw = d.get("ok")
    ok = ok_raw is True or ok_raw == "true"

    fields = [_field_from_json(f) for f in d.get("fields", d.get("fields_so_far", []))]

    if ok:
        return DecodeOk(null_bitmap_hex=d["null_bitmap_hex"], fields=fields)
    return DecodeErr(
        error=d["error"],
        null_bitmap_hex=d.get("null_bitmap_hex"),
        fields_so_far=fields,
    )


def _field_from_json(f: dict[str, Any]) -> DecodedField:
    """Parse a decoded field entry from the Rust heap-dump JSON.

    ``byte_range`` is stored as a 2-element list/tuple and is normalized to a
    Python ``(start, end)`` tuple.
    """
    br = f["byte_range"]
    return DecodedField(
        index=f["index"],
        name=f.get("name"),
        type_name=f["type"],
        value=f.get("value"),
        byte_range=(br[0], br[1]),
    )
