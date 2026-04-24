"""Transaction chain analysis over parsed WAL records.

A "chain" is the ordered list of records written by one transaction, linked
via `prev_lsn`. We also detect structural anomalies that are common WAL bugs:

- records whose `prev_lsn` doesn't point at the actual prior record for the tid
- transactions with no terminating Commit/Abort
- orphan records whose `prev_lsn` references an LSN we never saw
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from .format import LSN_INVALID
from .records import LogRecord, LogRecordType


@dataclass
class Chain:
    tid: int
    records: list[LogRecord] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)

    @property
    def terminated(self) -> bool:
        if not self.records:
            return False
        last = self.records[-1].record_type
        return last in {LogRecordType.COMMIT, LogRecordType.ABORT}

    @property
    def outcome(self) -> str:
        if not self.records:
            return "empty"
        last = self.records[-1].record_type
        if last is LogRecordType.COMMIT:
            return "committed"
        if last is LogRecordType.ABORT:
            return "aborted"
        return "in-flight"


def build_chains(records: list[LogRecord]) -> list[Chain]:
    """Group records by tid, preserving on-disk order, and validate prev_lsn links.

    The input is expected to be in file order (the natural output of `iter_records`).
    Checkpoint records (which use `TransactionId::INVALID`) are skipped — they
    don't belong to any user transaction.
    """
    lsn_to_record: dict[int, LogRecord] = {r.lsn: r for r in records}
    by_tid: dict[int, list[LogRecord]] = defaultdict(list)

    for r in records:
        if r.record_type in {LogRecordType.CHECKPOINT_BEGIN, LogRecordType.CHECKPOINT_END}:
            continue
        by_tid[r.tid].append(r)

    chains: list[Chain] = []
    for tid, recs in by_tid.items():
        chain = Chain(tid=tid, records=recs)

        for i, r in enumerate(recs):
            expected_prev = LSN_INVALID if i == 0 else recs[i - 1].lsn
            if r.prev_lsn != expected_prev:
                chain.issues.append(
                    f"record lsn={r.lsn} has prev_lsn={_fmt_lsn(r.prev_lsn)}, "
                    f"expected {_fmt_lsn(expected_prev)}"
                )
            if r.prev_lsn != LSN_INVALID and r.prev_lsn not in lsn_to_record:
                chain.issues.append(
                    f"record lsn={r.lsn} references unknown prev_lsn={r.prev_lsn}"
                )

        if not chain.terminated:
            chain.issues.append("no terminating Commit/Abort — transaction left in-flight")

        chains.append(chain)

    chains.sort(key=lambda c: c.records[0].lsn if c.records else 0)
    return chains


def _fmt_lsn(lsn: int) -> str:
    return "INVALID" if lsn == LSN_INVALID else str(lsn)
