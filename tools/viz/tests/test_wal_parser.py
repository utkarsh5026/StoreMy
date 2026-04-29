"""Round-trip tests: encoder (conftest) → parser → expected fields."""

from __future__ import annotations

import io

import pytest

from storemy_viz.wal import (
    LogRecordType,
    ParseError,
    build_chains,
    iter_records,
    read_file,
)
from storemy_viz.wal.format import HEADER_SIZE, LSN_INVALID

from . import conftest as w


def _parse(blob: bytes):
    return list(iter_records(io.BytesIO(blob)))


# ── header / primitives ──────────────────────────────────────────────────────


def test_empty_file_yields_no_records():
    assert _parse(b"") == []


def test_begin_record_roundtrip():
    blob = w.begin(lsn=0, tid=42)
    [r] = _parse(blob)
    assert r.offset == 0
    assert r.lsn == 0
    assert r.prev_lsn == LSN_INVALID
    assert r.tid == 42
    assert r.record_type is LogRecordType.BEGIN
    assert r.body_len == 0
    assert r.checksum_ok


def test_insert_record_carries_page_and_after_image():
    after = b"\x01\x02\x03\x04"
    blob = w.insert(lsn=0, prev_lsn=LSN_INVALID, tid=7, file_id=1, page_no=5, after=after)
    [r] = _parse(blob)
    assert r.record_type is LogRecordType.INSERT
    assert r.page_id is not None
    assert r.page_id.file_id == 1
    assert r.page_id.page_no == 5
    assert r.after == after
    assert r.before is None


def test_update_record_carries_both_images():
    before, after = b"AAAA", b"BBBBBB"
    blob = w.update(
        lsn=0, prev_lsn=LSN_INVALID, tid=1, file_id=2, page_no=3,
        before=before, after=after,
    )
    [r] = _parse(blob)
    assert r.record_type is LogRecordType.UPDATE
    assert r.before == before
    assert r.after == after


def test_clr_carries_undo_next_lsn():
    blob = w.clr(
        lsn=0, prev_lsn=LSN_INVALID, tid=1, file_id=1, page_no=1,
        after=b"x", undo_next_lsn=999,
    )
    [r] = _parse(blob)
    assert r.record_type is LogRecordType.CLR
    assert r.undo_next_lsn == 999


# ── corruption / edge cases ──────────────────────────────────────────────────


def test_unknown_record_type_raises():
    blob = bytearray(w.begin(lsn=0, tid=1))
    blob[0] = 99  # corrupt type byte
    with pytest.raises(ParseError, match="unknown record_type"):
        _parse(bytes(blob))


def test_truncated_body_raises():
    blob = w.insert(lsn=0, prev_lsn=LSN_INVALID, tid=1, file_id=1, page_no=1, after=b"abcd")
    with pytest.raises(ParseError, match="truncated body"):
        _parse(blob[: HEADER_SIZE + 3])


def test_bad_checksum_is_reported_but_record_still_yielded():
    blob = bytearray(
        w.insert(lsn=0, prev_lsn=LSN_INVALID, tid=1, file_id=1, page_no=1, after=b"xyz")
    )
    # Flip a body byte so the CRC no longer matches.
    blob[-1] ^= 0xFF
    [r] = _parse(bytes(blob))
    assert r.checksum_ok is False


# ── chain analysis ───────────────────────────────────────────────────────────


def test_chains_group_records_by_tid_in_order():
    # tid 1: begin → insert → commit ; tid 2: begin → abort (interleaved).
    r0 = w.begin(lsn=0, tid=1)
    off1 = len(r0)
    r1 = w.begin(lsn=off1, tid=2)
    off2 = off1 + len(r1)
    r2 = w.insert(
        lsn=off2, prev_lsn=0, tid=1, file_id=1, page_no=1, after=b"hello"
    )
    off3 = off2 + len(r2)
    r3 = w.abort(lsn=off3, prev_lsn=off1, tid=2)
    off4 = off3 + len(r3)
    r4 = w.commit(lsn=off4, prev_lsn=off2, tid=1)

    records = _parse(w.build_wal(r0, r1, r2, r3, r4))
    chains = build_chains(records)

    by_tid = {c.tid: c for c in chains}
    assert by_tid[1].outcome == "committed"
    assert by_tid[2].outcome == "aborted"
    assert [r.record_type for r in by_tid[1].records] == [
        LogRecordType.BEGIN, LogRecordType.INSERT, LogRecordType.COMMIT,
    ]
    assert by_tid[1].issues == []
    assert by_tid[2].issues == []


def test_chain_detects_in_flight_transaction():
    r0 = w.begin(lsn=0, tid=5)
    records = _parse(w.build_wal(r0))
    [chain] = build_chains(records)
    assert chain.outcome == "in-flight"
    assert any("no terminating" in i for i in chain.issues)


def test_chain_detects_bad_prev_lsn_link():
    r0 = w.begin(lsn=0, tid=1)
    off1 = len(r0)
    # Deliberately wrong prev_lsn (should be 0, we set it to 999).
    r1 = w.commit(lsn=off1, prev_lsn=999, tid=1)
    records = _parse(w.build_wal(r0, r1))
    [chain] = build_chains(records)
    assert any("prev_lsn" in i for i in chain.issues)


# ── file I/O ─────────────────────────────────────────────────────────────────


def test_read_file_reads_full_wal(tmp_path):
    blob = w.build_wal(
        w.begin(lsn=0, tid=1),
        w.commit(lsn=41, prev_lsn=0, tid=1),
    )
    p = tmp_path / "test.wal"
    p.write_bytes(blob)
    records = read_file(p)
    assert len(records) == 2
    assert records[0].record_type is LogRecordType.BEGIN
    assert records[1].record_type is LogRecordType.COMMIT
