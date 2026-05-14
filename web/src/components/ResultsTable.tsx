import type { Cell, Column } from "../types/api";

interface Props {
  columns: Column[];
  rows: Cell[][];
}

function renderCell(c: Cell): React.ReactNode {
  if (c === null) return <span className="text-dim italic">NULL</span>;
  if (typeof c === "boolean") return c ? "true" : "false";
  return String(c);
}

export function ResultsTable({ columns, rows }: Props) {
  if (rows.length === 0) {
    return <div className="font-mono text-[13px]">(0 rows)</div>;
  }
  return (
    <table className="border-collapse font-mono text-[13px]">
      <thead>
        <tr>
          {columns.map((c) => (
            <th
              key={c.name}
              title={`${c.type}${c.nullable ? "" : " NOT NULL"}`}
              className="border border-line px-2.5 py-1 text-left bg-panel2 font-semibold whitespace-nowrap"
            >
              {c.name}
              <span className="text-dim font-normal ml-1.5 text-[11px]">{c.type}</span>
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, i) => (
          <tr key={i}>
            {row.map((cell, j) => (
              <td key={j} className="border border-line px-2.5 py-1 whitespace-nowrap">
                {renderCell(cell)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
