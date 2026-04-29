"""Rich-based terminal views for parsed WAL data."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..wal.chains import Chain
from ..wal.format import LSN_INVALID, TID_INVALID
from ..wal.records import LogRecord


def print_summary(records: list[LogRecord], console: Console | None = None) -> None:
    """One-screen overview: counts, LSN range, checksum health, issues."""
    console = console or Console()

    if not records:
        console.print("[yellow]WAL is empty.[/yellow]")
        return

    type_counts = Counter(r.record_type.name for r in records)
    bad_crc = sum(1 for r in records if not r.checksum_ok)
    warns = sum(len(r.warnings) for r in records)
    tids = {r.tid for r in records if r.tid != TID_INVALID}
    first, last = records[0], records[-1]
    total_bytes = last.offset + last.total_size

    stats = Table.grid(padding=(0, 2))
    stats.add_column(style="bold")
    stats.add_column()
    stats.add_row("records", str(len(records)))
    stats.add_row("total bytes", f"{total_bytes:,}")
    stats.add_row("lsn range", f"{first.lsn} → {last.lsn}")
    stats.add_row("distinct tids", str(len(tids)))
    stats.add_row("bad checksums", _warn_if(bad_crc))
    stats.add_row("parser warnings", _warn_if(warns))

    console.print(Panel(stats, title="WAL summary", expand=False))

    counts = Table(title="record types", show_header=True, header_style="bold cyan")
    counts.add_column("type")
    counts.add_column("count", justify="right")
    for name, n in sorted(type_counts.items(), key=lambda kv: -kv[1]):
        counts.add_row(name, str(n))
    console.print(counts)


def print_records(records: list[LogRecord], console: Console | None = None) -> None:
    """Full record-by-record table."""
    console = console or Console()
    tbl = Table(title="WAL records", show_header=True, header_style="bold cyan")
    tbl.add_column("offset", justify="right")
    tbl.add_column("lsn", justify="right")
    tbl.add_column("prev", justify="right")
    tbl.add_column("tid", justify="right")
    tbl.add_column("type")
    tbl.add_column("page")
    tbl.add_column("body")
    tbl.add_column("ts")
    tbl.add_column("crc")

    for r in records:
        tbl.add_row(
            str(r.offset),
            str(r.lsn),
            _fmt_lsn(r.prev_lsn),
            _fmt_tid(r.tid),
            r.record_type.name,
            _fmt_page(r),
            f"{r.body_len}B",
            _fmt_ts(r.timestamp),
            "[green]ok[/green]" if r.checksum_ok else "[red]BAD[/red]",
        )
    console.print(tbl)


def print_chains(chains: list[Chain], console: Console | None = None) -> None:
    """Per-transaction chain view."""
    console = console or Console()

    if not chains:
        console.print("[yellow]No transaction chains (only checkpoint records?).[/yellow]")
        return

    for chain in chains:
        outcome_color = {
            "committed": "green",
            "aborted": "yellow",
            "in-flight": "red",
            "empty": "white",
        }[chain.outcome]
        header = (
            f"tid={chain.tid}  "
            f"records={len(chain.records)}  "
            f"[{outcome_color}]{chain.outcome}[/{outcome_color}]"
        )

        tbl = Table(show_header=True, header_style="bold")
        tbl.add_column("lsn", justify="right")
        tbl.add_column("prev", justify="right")
        tbl.add_column("type")
        tbl.add_column("page")
        for r in chain.records:
            tbl.add_row(
                str(r.lsn),
                _fmt_lsn(r.prev_lsn),
                r.record_type.name,
                _fmt_page(r),
            )

        content = tbl
        if chain.issues:
            console.print(Panel(tbl, title=header, border_style=outcome_color))
            for issue in chain.issues:
                console.print(f"  [red]✗[/red] {issue}")
        else:
            console.print(Panel(content, title=header, border_style=outcome_color))


def _fmt_lsn(lsn: int) -> str:
    return "—" if lsn == LSN_INVALID else str(lsn)


def _fmt_tid(tid: int) -> str:
    return "—" if tid == TID_INVALID else str(tid)


def _fmt_page(r: LogRecord) -> str:
    if r.page_id is None:
        return ""
    return f"f{r.page_id.file_id}/p{r.page_id.page_no}"


def _fmt_ts(ts: int) -> str:
    if ts == 0:
        return "—"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _warn_if(n: int) -> str:
    return f"[red]{n}[/red]" if n else "[green]0[/green]"
