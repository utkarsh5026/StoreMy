"""CLI entrypoint — `storemy-viz …`."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from .hash import DumperFailed as HashDumperFailed
from .hash import DumperNotFound as HashDumperNotFound
from .hash import DumpSchemaMismatch as HashDumpSchemaMismatch
from .hash import load_dump as load_hash_dump
from .heap import DumperFailed, DumperNotFound, DumpSchemaMismatch, load_dump
from .views import (
    chains_to_dot,
    print_chains,
    print_hash_bucket,
    print_hash_page,
    print_hash_summary,
    print_heap_page,
    print_heap_summary,
    print_records,
    print_summary,
)
from .wal import build_chains, read_file

app = typer.Typer(help="Visualizers for StoreMy on-disk formats (dev-only).")
wal_app = typer.Typer(help="Inspect a WAL file.")
heap_app = typer.Typer(help="Inspect a heap file (via storemy-heap-dump).")
hash_app = typer.Typer(help="Inspect a hash index file (via storemy-hash-dump).")
app.add_typer(wal_app, name="wal")
app.add_typer(heap_app, name="heap")
app.add_typer(hash_app, name="hash")

console = Console()


@wal_app.command("summary")
def wal_summary(path: Path = typer.Argument(..., exists=True, readable=True)) -> None:
    """One-screen overview of a WAL file."""
    records = read_file(path)
    print_summary(records, console)


@wal_app.command("records")
def wal_records(path: Path = typer.Argument(..., exists=True, readable=True)) -> None:
    """Full record-by-record table."""
    records = read_file(path)
    print_records(records, console)


@wal_app.command("chains")
def wal_chains(path: Path = typer.Argument(..., exists=True, readable=True)) -> None:
    """Group records by transaction, validate prev_lsn chains."""
    records = read_file(path)
    chains = build_chains(records)
    print_chains(chains, console)


@wal_app.command("graph")
def wal_graph(
    path: Path = typer.Argument(..., exists=True, readable=True),
    output: Path = typer.Option(
        Path("chains.dot"), "--output", "-o", help="DOT file to write."
    ),
) -> None:
    """Export transaction chains as a Graphviz DOT file."""
    records = read_file(path)
    chains = build_chains(records)
    dot = chains_to_dot(chains)
    output.write_text(dot)
    console.print(
        f"[green]wrote[/green] {output}  "
        f"([dim]render with: dot -Tsvg {output} -o {output.with_suffix('.svg')}[/dim])"
    )


def _load_dump_or_exit(
    path: Path,
    fields: int | None,
    names: str | None,
    page: int | None,
):
    name_list = [n.strip() for n in names.split(",")] if names else None
    try:
        return load_dump(path, field_count=fields, field_names=name_list, page=page)
    except DumperNotFound as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=2) from e
    except DumperFailed as e:
        console.print(f"[red]dumper failed[/red]: {e}")
        raise typer.Exit(code=e.returncode) from e
    except DumpSchemaMismatch as e:
        console.print(f"[red]{e}[/red]")
        console.print(
            "[dim]upgrade storemy-viz or re-dump with a matching dumper "
            "version[/dim]"
        )
        raise typer.Exit(code=2) from e


@heap_app.command("summary")
def heap_summary(
    path: Path = typer.Argument(..., exists=True, readable=True),
    fields: int | None = typer.Option(
        None, "--fields", help="Number of tuple fields (required for .heap files)."
    ),
    names: str | None = typer.Option(
        None, "--names", help="Comma-separated field names (optional)."
    ),
) -> None:
    """File-level summary: per-page fill, live/tombstone counts."""
    dump = _load_dump_or_exit(path, fields, names, page=None)
    print_heap_summary(dump, console)


@heap_app.command("page")
def heap_page(
    path: Path = typer.Argument(..., exists=True, readable=True),
    page: int = typer.Option(..., "--page", help="Page number to inspect."),
    fields: int | None = typer.Option(
        None, "--fields", help="Number of tuple fields (required for .heap files)."
    ),
    names: str | None = typer.Option(
        None, "--names", help="Comma-separated field names (optional)."
    ),
) -> None:
    """Detailed view of one page: header, slot table, decoded tuples."""
    dump = _load_dump_or_exit(path, fields, names, page=page)
    print_heap_page(dump, page, console)


@heap_app.command("dump")
def heap_dump(
    path: Path = typer.Argument(..., exists=True, readable=True),
    output: Path = typer.Option(..., "--output", "-o", help="JSON file to write."),
    fields: int = typer.Option(..., "--fields", help="Number of tuple fields."),
    names: str | None = typer.Option(None, "--names", help="Comma-separated field names."),
) -> None:
    """Capture a heap file as JSON (for archiving or later inspection)."""
    import json

    dump = _load_dump_or_exit(path, fields, names, page=None)
    # Round-trip through a dict by re-serializing the model. For archival we
    # prefer the raw dumper output, but the model is frozen-dataclasses so we
    # rebuild it here explicitly.
    obj = {
        "schema_version": dump.schema_version,
        "page_size": dump.page_size,
        "page_count": dump.page_count,
        "field_count": dump.field_count,
        "field_names": dump.field_names,
        "pages": [_page_to_json(p) for p in dump.pages],
    }
    output.write_text(json.dumps(obj, indent=2))
    console.print(f"[green]wrote[/green] {output}")


def _page_to_json(p) -> dict:
    from .heap.model import DecodeErr, DecodeOk, LiveSlot, OutOfRangeSlot, TombstoneSlot

    def field_to_json(f) -> dict:
        return {
            "index": f.index,
            "name": f.name,
            "type": f.type_name,
            "value": f.value,
            "byte_range": [f.byte_range[0], f.byte_range[1]],
        }

    def slot_to_json(s) -> dict:
        if isinstance(s, TombstoneSlot):
            return {"status": "tombstone", "slot_id": s.slot_id, "offset": s.offset, "length": s.length}
        if isinstance(s, OutOfRangeSlot):
            return {
                "status": "out_of_range",
                "slot_id": s.slot_id,
                "offset": s.offset,
                "length": s.length,
                "error": s.error,
            }
        assert isinstance(s, LiveSlot)
        if isinstance(s.decode, DecodeOk):
            decode = {
                "ok": "true",
                "null_bitmap_hex": s.decode.null_bitmap_hex,
                "fields": [field_to_json(f) for f in s.decode.fields],
            }
        else:
            assert isinstance(s.decode, DecodeErr)
            decode = {
                "ok": "false",
                "error": s.decode.error,
                "null_bitmap_hex": s.decode.null_bitmap_hex,
                "fields_so_far": [field_to_json(f) for f in s.decode.fields_so_far],
            }
        return {
            "status": "live",
            "slot_id": s.slot_id,
            "offset": s.offset,
            "length": s.length,
            "raw_hex": s.raw_hex,
            "decode": decode,
        }

    return {
        "page_no": p.page_no,
        "format_version": p.format_version,
        "header": {
            "num_slots": p.header.num_slots,
            "tuple_start": p.header.tuple_start,
            "raw_hex": p.header.raw_hex,
        },
        "slot_array_end": p.slot_array_end,
        "free_bytes": p.free_bytes,
        "used_bytes": p.used_bytes,
        "slots": [slot_to_json(s) for s in p.slots],
        "page_error": p.page_error,
    }


def _load_hash_dump_or_exit(path: Path):
    """Load a hash dump JSON or exit with a friendly error."""
    try:
        return load_hash_dump(path)
    except HashDumperNotFound as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=2) from e
    except HashDumperFailed as e:
        console.print(f"[red]dumper failed[/red]: {e}")
        raise typer.Exit(code=e.returncode) from e
    except HashDumpSchemaMismatch as e:
        console.print(f"[red]{e}[/red]")
        console.print(
            "[dim]upgrade storemy-viz or re-dump with a matching dumper "
            "version[/dim]"
        )
        raise typer.Exit(code=2) from e


@hash_app.command("summary")
def hash_summary(path: Path = typer.Argument(..., exists=True, readable=True)) -> None:
    """File-level summary: chain-length distribution, fill, errors."""
    dump = _load_hash_dump_or_exit(path)
    print_hash_summary(dump, console)


@hash_app.command("bucket")
def hash_bucket(
    path: Path = typer.Argument(..., exists=True, readable=True),
    bucket: int = typer.Option(..., "--bucket", help="Bucket number to inspect."),
) -> None:
    """Detailed view of one bucket: walk its chain, list entries per page."""
    dump = _load_hash_dump_or_exit(path)
    print_hash_bucket(dump, bucket, console)


@hash_app.command("page")
def hash_page(
    path: Path = typer.Argument(..., exists=True, readable=True),
    page: int = typer.Option(..., "--page", help="Page number to inspect."),
) -> None:
    """Detailed view of one physical page (head or overflow)."""
    dump = _load_hash_dump_or_exit(path)
    print_hash_page(dump, page, console)


if __name__ == "__main__":
    app()
