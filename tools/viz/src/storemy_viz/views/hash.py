"""Rich-based terminal views for hash-index dumps."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..hash.model import BucketDump, Dump, Entry, PageDump


def print_summary(dump: Dump, console: Console | None = None) -> None:
    """File-level overview: page count, chain-length distribution, fill, errors."""
    console = console or Console()

    head_pages = sum(1 for p in dump.pages if p.kind == "head")
    overflow_pages = sum(1 for p in dump.pages if p.kind == "overflow")
    total_entries = sum(p.entry_count for p in dump.pages)
    page_errors = sum(1 for p in dump.pages if p.page_error)
    bad_crc = sum(1 for p in dump.pages if not p.crc_ok)

    chain_lens = [len(b.chain) for b in dump.buckets]
    longest = max(chain_lens, default=0)
    avg = (sum(chain_lens) / len(chain_lens)) if chain_lens else 0.0
    overflowing = sum(1 for n in chain_lens if n > 1)

    stats = Table.grid(padding=(0, 2))
    stats.add_column(style="bold")
    stats.add_column()
    stats.add_row("pages", str(dump.page_count))
    stats.add_row("page size", f"{dump.page_size:,}")
    stats.add_row("buckets", str(dump.num_buckets))
    if dump.key_types:
        stats.add_row("key types", ", ".join(dump.key_types))
    stats.add_row("head pages", str(head_pages))
    stats.add_row("overflow pages", _warn_if(overflow_pages))
    stats.add_row("total entries", str(total_entries))
    stats.add_row("longest chain", _warn_if(longest, threshold=2))
    stats.add_row("avg chain", f"{avg:.2f}")
    stats.add_row("buckets with overflow", _warn_if(overflowing))
    stats.add_row("crc failures", _warn_if(bad_crc))
    stats.add_row("page-level errors", _warn_if(page_errors))
    console.print(Panel(stats, title="Hash index summary", expand=False))

    tbl = Table(title="buckets (sorted by chain length)", show_header=True, header_style="bold cyan")
    for col, justify in (
        ("bucket", "right"),
        ("chain", "right"),
        ("entries", "right"),
        ("fill", "right"),
        ("head", "right"),
        ("tail", "right"),
        ("status", "left"),
    ):
        tbl.add_column(col, justify=justify)

    for b in sorted(dump.buckets, key=lambda x: -len(x.chain)):
        head = str(b.chain[0]) if b.chain else "-"
        tail = str(b.chain[-1]) if b.chain else "-"
        tbl.add_row(
            str(b.bucket_num),
            str(len(b.chain)),
            str(b.total_entries),
            f"{b.fill_ratio() * 100:4.1f}%",
            head,
            tail,
            _bucket_status(b),
        )
    console.print(tbl)


def print_bucket(dump: Dump, bucket_num: int, console: Console | None = None) -> None:
    """Detailed view of one bucket: walk the chain, list entries per page."""
    console = console or Console()
    bucket = next((b for b in dump.buckets if b.bucket_num == bucket_num), None)
    if bucket is None:
        console.print(f"[red]bucket {bucket_num} not found in dump[/red]")
        return

    header = Table.grid(padding=(0, 2))
    header.add_column(style="bold")
    header.add_column()
    header.add_row("bucket", str(bucket.bucket_num))
    header.add_row("chain length", str(len(bucket.chain)))
    header.add_row("chain", " → ".join(str(pn) for pn in bucket.chain) or "[dim](empty)[/dim]")
    header.add_row("total entries", str(bucket.total_entries))
    header.add_row("avg fill", f"{bucket.fill_ratio() * 100:4.1f}%")
    console.print(Panel(header, title=f"bucket {bucket_num}", expand=False))

    for pn in bucket.chain:
        page = _find_page(dump, pn)
        if page is None:
            console.print(f"[red]page {pn} listed in chain but missing from dump[/red]")
            continue
        _print_page_entries(console, page, dump.key_types)


def print_page(dump: Dump, page_no: int, console: Console | None = None) -> None:
    """Detailed view of one page (regardless of head/overflow)."""
    console = console or Console()
    page = _find_page(dump, page_no)
    if page is None:
        console.print(f"[red]page {page_no} not found in dump[/red]")
        return
    _print_page_entries(console, page, dump.key_types)


# --- helpers ---------------------------------------------------------------


def _print_page_entries(
    console: Console, page: PageDump, key_types: list[str] | None
) -> None:
    """Render one page as a header panel plus an entries table."""
    header = Table.grid(padding=(0, 2))
    header.add_column(style="bold")
    header.add_column()
    header.add_row("page", str(page.page_no))
    header.add_row("kind", page.kind)
    header.add_row("bucket_num", str(page.bucket_num))
    header.add_row("entries", str(page.entry_count))
    header.add_row(
        "overflow",
        str(page.overflow) if page.overflow is not None else "[dim]NIL[/dim]",
    )
    header.add_row("crc", "[green]ok[/green]" if page.crc_ok else "[red]bad[/red]")
    if page.page_error:
        header.add_row("page_error", f"[red]{page.page_error}[/red]")

    border = "green" if page.crc_ok and not page.page_error else "red"
    console.print(
        Panel(
            header,
            title=f"page {page.page_no} ({page.kind})",
            border_style=border,
            expand=False,
        )
    )

    if not page.entries:
        return
    tbl = Table(show_header=True, header_style="bold cyan")
    tbl.add_column("idx", justify="right")
    tbl.add_column("key")
    tbl.add_column("rid")
    for i, e in enumerate(page.entries):
        tbl.add_row(str(i), _format_key(e, key_types), f"({e.rid_page}, {e.rid_slot})")
    console.print(tbl)


def _find_page(dump: Dump, page_no: int) -> PageDump | None:
    return next((p for p in dump.pages if p.page_no == page_no), None)


def _bucket_status(b: BucketDump) -> str:
    """Render a compact health/status string for a bucket row."""
    parts: list[str] = []
    if len(b.chain) > 4:
        parts.append(f"[red]long chain ({len(b.chain)})[/red]")
    elif len(b.chain) > 1:
        parts.append(f"[yellow]chained ({len(b.chain)})[/yellow]")
    if b.total_entries == 0:
        parts.append("[dim]empty[/dim]")
    return " ".join(parts) if parts else "[green]ok[/green]"


def _format_key(e: Entry, key_types: list[str] | None) -> str:
    """Render a composite key as ``(c0, c1, ...)``.

    ``key_types`` is currently informational only; the dumper hands us
    pre-stringified components, matching the heap visualizer's policy of
    keeping ``Value``-tag knowledge on the Rust side.
    """
    del key_types
    return "(" + ", ".join(e.key) + ")"


def _warn_if(n: int, threshold: int = 1) -> str:
    """Render a count, dimming zeros and escalating colour past ``threshold``."""
    if n == 0:
        return "0"
    return f"[red]{n}[/red]" if n >= threshold else f"[yellow]{n}[/yellow]"
