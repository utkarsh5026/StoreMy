"""Graphviz DOT export for WAL transaction chains.

We emit DOT directly — no dependency on the `graphviz` Python package is
needed to produce the `.dot` file; you only need it (and the `dot` binary)
to *render* it to SVG/PNG.
"""

from __future__ import annotations

from ..wal.chains import Chain
from ..wal.records import LogRecordType

_TYPE_COLOR = {
    LogRecordType.BEGIN: "#9ecae1",
    LogRecordType.COMMIT: "#74c476",
    LogRecordType.ABORT: "#fdae6b",
    LogRecordType.UPDATE: "#dadaeb",
    LogRecordType.INSERT: "#c7e9c0",
    LogRecordType.DELETE: "#fcbba1",
    LogRecordType.CLR: "#d4b9da",
    LogRecordType.CHECKPOINT_BEGIN: "#f0f0f0",
    LogRecordType.CHECKPOINT_END: "#f0f0f0",
}


def chains_to_dot(chains: list[Chain]) -> str:
    """Return DOT source for a transaction-chain diagram.

    One cluster per tid, nodes for each record, edges along `prev_lsn`.
    """
    lines: list[str] = [
        "digraph wal_chains {",
        '  rankdir="LR";',
        '  node [shape=box, style=filled, fontname="monospace", fontsize=10];',
        '  edge [fontname="monospace", fontsize=9];',
    ]

    for chain in chains:
        lines.append(f'  subgraph cluster_tid_{chain.tid} {{')
        lines.append(f'    label="tid {chain.tid} — {chain.outcome}";')
        lines.append('    style="rounded,dashed";')
        for r in chain.records:
            color = _TYPE_COLOR.get(r.record_type, "#ffffff")
            label = f"lsn {r.lsn}\\n{r.record_type.name}"
            if r.page_id is not None:
                label += f"\\nf{r.page_id.file_id}/p{r.page_id.page_no}"
            lines.append(
                f'    n{r.lsn} [label="{label}", fillcolor="{color}"];'
            )

        for i in range(1, len(chain.records)):
            prev, curr = chain.records[i - 1], chain.records[i]
            lines.append(f"    n{prev.lsn} -> n{curr.lsn};")
        lines.append("  }")

    lines.append("}")
    return "\n".join(lines) + "\n"
