# storemy-viz

Dev-only visualizers for StoreMy on-disk formats. Not shipped with the database —
this is a separate Python toolkit for humans debugging WAL, heap pages, and
(eventually) B-tree/hash index files.

## Why a separate Python project

The binary formats (WAL records, heap slot layout, index nodes) change as the
Rust rewrite progresses. Keeping the visualizer in Python with its own parser
means:

- Iteration is fast — change a view, re-run, done.
- No build coupling to the Rust crate.
- Graph/tabular libraries (rich, graphviz, textual) are available immediately.

The trade-off: the Python parser can drift from the Rust writer. Every format
we parse is documented in [`FORMAT.md`](FORMAT.md) — update both sides when
the format changes.

## Install

```bash
cd tools/viz
python -m venv .venv && source .venv/bin/activate
pip install -e ".[graph,dev]"
```

Or with uv:

```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[graph,dev]"
```

## Usage

```bash
# Summary stats over a WAL file
storemy-viz wal summary path/to/wal.log

# Pretty table of all records
storemy-viz wal records path/to/wal.log

# Transaction chains (which records belong to which txn, linked by prev_lsn)
storemy-viz wal chains path/to/wal.log

# Export txn chains as a Graphviz DOT file (requires [graph] extra)
storemy-viz wal graph path/to/wal.log -o chains.dot
dot -Tsvg chains.dot -o chains.svg
```

## Layout

```
tools/viz/
├── pyproject.toml
├── FORMAT.md                    # on-disk format spec (mirrors Rust side)
├── src/storemy_viz/
│   ├── cli.py                   # typer-based CLI entrypoint
│   ├── wal/
│   │   ├── format.py            # byte-layout constants
│   │   ├── records.py           # dataclasses
│   │   ├── parser.py            # binary → records
│   │   └── chains.py            # txn chain analysis
│   └── views/
│       ├── summary.py           # rich tables / stats
│       └── graph.py             # graphviz DOT export
└── tests/
    └── test_wal_parser.py       # round-trip against synthetic bytes
```

## Tests

```bash
pytest
```
