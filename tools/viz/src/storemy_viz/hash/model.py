"""Dataclasses that mirror the hash-dump JSON schema (v1).

These types are a thin, typed wrapper around the JSON produced by a
``storemy-hash-dump`` Rust binary (not yet implemented — like the heap
visualizer, the Python side intentionally consumes JSON only). See
``FORMAT.md`` for the on-disk layout and the JSON schema.

The hash index uses **static (linear) hashing with separate chaining**:

* The directory is a fixed-size array of ``num_buckets`` *head* pages.
* When a head page fills up, the entries spill into a chain of
  *overflow* pages linked via the head's ``overflow`` pointer.
* A "bucket" is therefore a chain of one head page plus zero or more
  overflow pages, all sharing the same ``bucket_num``.

Unknown ``kind`` strings are preserved verbatim so that future page
kinds (e.g. directory pages, were the index ever extended) can be
displayed without a Python update.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SUPPORTED_DUMP_SCHEMA_VERSIONS = frozenset({1})


class DumpSchemaMismatch(Exception):
    """Raised when a dump JSON uses a ``schema_version`` we do not support."""


@dataclass(frozen=True)
class Entry:
    """One key → RID mapping inside a hash bucket page.

    ``key`` is a list of already-stringified composite-key components
    (the Rust dumper renders ``Value`` variants to strings so the Python
    side does not need to track type tags).
    """

    key: list[str]
    rid_page: int
    rid_slot: int


@dataclass(frozen=True)
class PageDump:
    """One physical page from the hash index file."""

    page_no: int
    kind: str  # "head" | "overflow" | "unknown"
    crc_ok: bool
    bucket_num: int
    entry_count: int
    overflow: int | None  # next page in the chain, or None if NIL
    entries: list[Entry]
    page_error: str | None


@dataclass(frozen=True)
class BucketDump:
    """A logical bucket: a head page plus its overflow chain.

    ``chain`` lists page numbers in walk order, head first.
    ``total_entries`` is the sum of ``entry_count`` across the chain.
    """

    bucket_num: int
    chain: list[int]
    total_entries: int
    capacity: int  # max entries that fit in one page (header + bucket header overhead)

    def fill_ratio(self) -> float:
        """Average per-page occupancy across the chain (0..1)."""
        if not self.chain or self.capacity <= 0:
            return 0.0
        return self.total_entries / (len(self.chain) * self.capacity)


@dataclass(frozen=True)
class Dump:
    schema_version: int
    page_size: int
    page_count: int
    num_buckets: int
    key_types: list[str] | None
    pages: list[PageDump] = field(default_factory=list)
    buckets: list[BucketDump] = field(default_factory=list)


def from_json(obj: dict[str, Any]) -> Dump:
    """Build a :class:`Dump` from the parsed JSON produced by the Rust dumper.

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
        num_buckets=obj["num_buckets"],
        key_types=obj.get("key_types"),
        pages=[_page_from_json(p) for p in obj.get("pages", [])],
        buckets=[_bucket_from_json(b) for b in obj.get("buckets", [])],
    )


def _page_from_json(p: dict[str, Any]) -> PageDump:
    overflow = p.get("overflow")
    return PageDump(
        page_no=p["page_no"],
        kind=p.get("kind", "unknown"),
        crc_ok=bool(p.get("crc_ok", True)),
        bucket_num=p["bucket_num"],
        entry_count=p["entry_count"],
        overflow=overflow if overflow is not None else None,
        entries=[_entry_from_json(e) for e in p.get("entries", [])],
        page_error=p.get("page_error"),
    )


def _entry_from_json(e: dict[str, Any]) -> Entry:
    rid = e.get("rid", [0, 0])
    return Entry(
        key=list(e.get("key", [])),
        rid_page=int(rid[0]),
        rid_slot=int(rid[1]),
    )


def _bucket_from_json(b: dict[str, Any]) -> BucketDump:
    return BucketDump(
        bucket_num=b["bucket_num"],
        chain=list(b.get("chain", [])),
        total_entries=b.get("total_entries", 0),
        capacity=b.get("capacity", 0),
    )
