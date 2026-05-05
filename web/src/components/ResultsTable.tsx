import type { Cell, Column } from "../types/api";

interface Props {
  columns: Column[];
  rows: Cell[][];
}

function renderCell(c: Cell): React.ReactNode {
  if (c === null) return <span className="null">NULL</span>;
  if (typeof c === "boolean") return c ? "true" : "false";
  return String(c);
}

export function ResultsTable({ columns, rows }: Props) {
  if (rows.length === 0) {
    return <div className="status-msg">(0 rows)</div>;
  }
  return (
    <table className="results">
      <thead>
        <tr>
          {columns.map((c) => (
            <th key={c.name} title={`${c.type}${c.nullable ? "" : " NOT NULL"}`}>
              {c.name}
              <span style={{ color: "var(--fg-dim)", fontWeight: 400, marginLeft: 6 }}>
                {c.type}
              </span>
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, i) => (
          <tr key={i}>
            {row.map((cell, j) => (
              <td key={j}>{renderCell(cell)}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
