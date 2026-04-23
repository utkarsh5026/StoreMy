"""Rich-based terminal views for heap-page dumps."""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ..heap.model import (
    DecodeErr,
    DecodeOk,
    Dump,
    LiveSlot,
    OutOfRangeSlot,
    PageDump,
    TombstoneSlot,
)


def print_summary(dump: Dump, console: Console | None = None) -> None:
    """File-level overview: page count, live/tombstone ratio, fill distribution."""
    console = console or Console()

    total_live = sum(p.live_count for p in dump.pages)
    total_tomb = sum(p.tombstone_count for p in dump.pages)
    total_used = sum(p.used_bytes for p in dump.pages)
    total_free = sum(p.free_bytes for p in dump.pages)
    page_errors = sum(1 for p in dump.pages if p.page_error)

    stats = Table.grid(padding=(0, 2))
    stats.add_column(style="bold")
    stats.add_column()
    stats.add_row("pages", str(dump.page_count))
    stats.add_row("page size", f"{dump.page_size:,}")
    stats.add_row("fields", str(dump.field_count))
    if dump.field_names:
        stats.add_row("field names", ", ".join(dump.field_names))
    stats.add_row("live tuples", str(total_live))
    stats.add_row("tombstones", _warn_if(total_tomb))
    stats.add_row("tuple bytes used", f"{total_used:,}")
    stats.add_row("free bytes", f"{total_free:,}")
    stats.add_row("page-level errors", _warn_if(page_errors))

    console.print(Panel(stats, title="Heap file summary", expand=False))

    def add_columns(label: list[str]) -> None:
        for l in label:
            tbl.add_column(l, justify="right")

    tbl = Table(title="pages", show_header=True, header_style="bold cyan")
    add_columns(["page", "ver", "slots", "live", "tomb", "used", "free", "fill", "status"])

    for p in dump.pages:
        tbl.add_row(
            str(p.page_no),
            str(p.format_version),
            str(p.header.num_slots),
            str(p.live_count),
            _tomb_cell(p.tombstone_count),
            f"{p.used_bytes:,}",
            f"{p.free_bytes:,}",
            f"{p.fill_ratio(dump.page_size) * 100:4.1f}%",
            _page_status(p),
        )
    console.print(tbl)


def print_page(dump: Dump, page_no: int, console: Console | None = None) -> None:
    """Detailed view of one page: header + slot table + per-slot tuple fields."""
    console = console or Console()
    page = _find_page(dump, page_no)
    if page is None:
        console.print(f"[red]page {page_no} not found in dump[/red]")
        return

    def add_header(label: str, value: Any) -> None:
        header.add_row(label, str(value))

    header = Table.grid(padding=(0, 2))
    header.add_column(style="bold")
    header.add_column()
    add_header("page", page.page_no)
    add_header("format_version", page.format_version)
    add_header("num_slots", page.header.num_slots)
    add_header("tuple_start", page.header.tuple_start)
    add_header("slot_array_end", page.slot_array_end)
    add_header("free_bytes", page.free_bytes)
    add_header("used_bytes", page.used_bytes)
    add_header("header bytes", page.header.raw_hex)
    if page.page_error:
        add_header("page_error", page.page_error)

    console.print(Panel(header, title=f"page {page.page_no}", expand=False))

    slots = Table(title="slots", show_header=True, header_style="bold cyan")
    slots.add_column("slot", justify="right")
    slots.add_column("offset", justify="right")
    slots.add_column("len", justify="right")
    slots.add_column("status")
    slots.add_column("summary")

    for s in page.slots:
        slots.add_row(*_slot_row(s, dump.field_names))
    console.print(slots)

    for s in page.slots:
        if isinstance(s, LiveSlot):
            _print_live_slot(console, s, dump.field_names)
        elif isinstance(s, OutOfRangeSlot):
            console.print(
                Panel(
                    Text(s.error, style="red"),
                    title=f"slot {s.slot_id} — out of range",
                    border_style="red",
                )
            )



def _find_page(dump: Dump, page_no: int) -> PageDump | None:
    """Return the page dump for ``page_no`` if present, else ``None``."""
    for p in dump.pages:
        if p.page_no == page_no:
            return p
    return None


def _page_status(p: PageDump) -> str:
    """Render a compact health/status string for a page for the summary table."""
    if p.page_error:
        return f"[red]{p.page_error}[/red]"
    broken = sum(1 for s in p.slots if isinstance(s, OutOfRangeSlot))
    bad_decode = sum(
        1 for s in p.slots if isinstance(s, LiveSlot) and isinstance(s.decode, DecodeErr)
    )
    parts: list[str] = []
    if broken:
        parts.append(f"[red]{broken} out-of-range[/red]")
    if bad_decode:
        parts.append(f"[yellow]{bad_decode} decode-err[/yellow]")
    if p.header.num_slots == 0:
        parts.append("[dim]empty[/dim]")
    return " ".join(parts) if parts else "[green]ok[/green]"


def _slot_row(slot, field_names: list[str] | None) -> tuple[str, str, str, str, str]:
    """Format one slot as a `(slot, offset, len, status, summary)` row."""
    if isinstance(slot, TombstoneSlot):
        return (
            str(slot.slot_id),
            str(slot.offset),
            str(slot.length),
            "[dim]tombstone[/dim]",
            "[dim](empty)[/dim]",
        )
    if isinstance(slot, OutOfRangeSlot):
        return (
            str(slot.slot_id),
            str(slot.offset),
            str(slot.length),
            "[red]out-of-range[/red]",
            f"[red]{slot.error}[/red]",
        )
    # LiveSlot
    if isinstance(slot.decode, DecodeOk):
        summary = _format_inline_tuple(slot.decode.fields, field_names)
        status = "[green]live[/green]"
    else:
        summary = f"[yellow]decode error: {slot.decode.error}[/yellow]"
        status = "[yellow]live (decode err)[/yellow]"
    return (str(slot.slot_id), str(slot.offset), str(slot.length), status, summary)


def _print_live_slot(console: Console, slot: LiveSlot, field_names: list[str] | None) -> None:
    """Render a detailed panel for a live slot, including decoded fields when available."""
    body = Table.grid(padding=(0, 2))
    body.add_column(style="dim")
    body.add_column()

    body.add_row("offset", str(slot.offset))
    body.add_row("length", str(slot.length))
    body.add_row("raw", _wrap_hex(slot.raw_hex))

    fields_tbl = Table(show_header=True, header_style="bold cyan", box=None)
    fields_tbl.add_column("idx", justify="right")
    fields_tbl.add_column("name")
    fields_tbl.add_column("type")
    fields_tbl.add_column("bytes")
    fields_tbl.add_column("value")

    decoded = slot.decode
    fields = decoded.fields if isinstance(decoded, DecodeOk) else decoded.fields_so_far
    for f in fields:
        fields_tbl.add_row(
            str(f.index),
            f.name or (field_names[f.index] if field_names and f.index < len(field_names) else "-"),
            f.type_name,
            f"[{f.byte_range[0]}..{f.byte_range[1]})",
            _render_value(f.type_name, f.value),
        )

    body.add_row("fields", fields_tbl)

    title = f"slot {slot.slot_id}"
    border = "green"
    if isinstance(decoded, DecodeErr):
        body.add_row("[yellow]error[/yellow]", decoded.error)
        title += " — decode error"
        border = "yellow"

    console.print(Panel(body, title=title, border_style=border, expand=False))


def _format_inline_tuple(fields, field_names: list[str] | None) -> str:
    """Render decoded fields as a short inline `k=v` list for table summaries."""
    parts: list[str] = []
    for f in fields:
        label = f.name or (
            field_names[f.index] if field_names and f.index < len(field_names) else f"f{f.index}"
        )
        parts.append(f"{label}={_render_value(f.type_name, f.value)}")
    return ", ".join(parts) if parts else "[dim](no fields)[/dim]"


def _render_value(type_name: str, value) -> str:
    """Pretty-print a decoded value for display (with small type-aware tweaks)."""
    if value is None:
        return "[dim]NULL[/dim]"
    if type_name == "String":
        shown = value if len(value) <= 32 else value[:29] + "..."
        return f'"{shown}"'
    if type_name == "Bool":
        return "true" if value else "false"
    return str(value)


def _wrap_hex(h: str, width: int = 32) -> str:
    """Wrap a long hex string to a fixed column width for panel display."""
    out: list[str] = []
    for i in range(0, len(h), width):
        out.append(h[i : i + width])
    return "\n".join(out)


def _warn_if(n: int) -> str:
    """Render a count, highlighting non-zero values as warnings."""
    return f"[red]{n}[/red]" if n else "0"


def _tomb_cell(n: int) -> str:
    """Render a tombstone count with a softer warning style."""
    return f"[yellow]{n}[/yellow]" if n else "0"
