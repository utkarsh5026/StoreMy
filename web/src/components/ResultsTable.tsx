import type { Cell, Column } from "../types/api";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table";

interface Props {
  columns: Column[];
  rows: Cell[][];
}

export function renderCell(c: Cell): React.ReactNode {
  if (c === null) return <span className="text-dim italic">NULL</span>;
  if (typeof c === "boolean") return c ? "true" : "false";
  return String(c);
}

export function ResultsTable({ columns, rows }: Props) {
  return (
    <Table className="font-mono text-[13px]">
      <TableHeader>
        <TableRow>
          {columns.map((c) => (
            <TableHead
              key={c.name}
              title={`${c.type}${c.nullable ? "" : " NOT NULL"}`}
              className="border border-line bg-panel2 px-2.5 py-1 font-semibold text-fg"
            >
              {c.name}
              <span className="text-dim font-normal ml-1.5 text-[11px]">{c.type}</span>
            </TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.length === 0 ? (
          <TableRow>
            <TableCell
              colSpan={Math.max(columns.length, 1)}
              className="border border-line px-2.5 py-2 text-dim italic"
            >
              0 rows
            </TableCell>
          </TableRow>
        ) : (
          rows.map((row, i) => (
            <TableRow key={i}>
              {row.map((cell, j) => (
                <TableCell key={j} className="border border-line px-2.5 py-1">
                  {renderCell(cell)}
                </TableCell>
              ))}
            </TableRow>
          ))
        )}
      </TableBody>
    </Table>
  );
}
