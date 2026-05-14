import { useEffect, useMemo, useState } from "react";
import type { HeapDump, HeapPage, HeapSlot } from "../types/api";
import { fetchHeap, StoremyError } from "../api/client";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table";

interface Props {
  db: string;
  table: string;
  refreshTick: number;
}

const PAGE_HDR_SIZE = 8;

type State =
  | { status: "loading" }
  | { status: "ok"; dump: HeapDump }
  | { status: "err"; message: string };

export function HeapInspector({ db, table, refreshTick }: Props) {
  const [state, setState] = useState<State>({ status: "loading" });
  const [pageIdx, setPageIdx] = useState(0);
  const [selectedSlot, setSelectedSlot] = useState<number | null>(null);

  useEffect(() => {
    let alive = true;
    setState({ status: "loading" });
    fetchHeap(db, table)
      .then((dump) => {
        if (!alive) return;
        setState({ status: "ok", dump });
      })
      .catch((e) => {
        if (!alive) return;
        const message = e instanceof StoremyError ? e.message : String(e);
        setState({ status: "err", message });
      });
    return () => {
      alive = false;
    };
  }, [db, table, refreshTick]);

  useEffect(() => {
    setSelectedSlot(null);
  }, [pageIdx, table]);

  if (state.status === "loading") {
    return <p className="font-mono text-[13px]">loading heap …</p>;
  }
  if (state.status === "err") {
    return (
      <div className="font-mono whitespace-pre-wrap bg-danger/8 border border-danger/30 rounded p-2.5 text-danger text-[13px]">
        {state.message}
      </div>
    );
  }

  const dump = state.dump;
  if (dump.pages.length === 0) {
    return (
      <p className="font-mono text-[13px]">
        no pages allocated yet for '{table}'
      </p>
    );
  }
  const page = dump.pages[Math.min(pageIdx, dump.pages.length - 1)];

  return (
    <div className="flex flex-col gap-3.5 font-mono text-[13px]">
      {/* Toolbar */}
      <div className="flex items-center gap-3 flex-wrap">
        <strong>{dump.table}</strong>
        <span className="text-dim text-xs">
          {dump.field_names.join(", ")} · {dump.page_count} page
          {dump.page_count === 1 ? "" : "s"} · page_size={dump.page_size}
        </span>
        {dump.pages.length > 1 && (
          <span className="flex items-center gap-1">
            page&nbsp;
            <select
              className="bg-panel2 text-fg border border-line rounded-[3px] px-1.5 py-0.5 font-mono text-[13px] outline-none focus:border-accent"
              value={pageIdx}
              onChange={(e) => setPageIdx(Number(e.target.value))}
            >
              {dump.pages.map((p, i) => (
                <option key={p.page_no} value={i}>
                  {p.page_no}
                </option>
              ))}
            </select>
          </span>
        )}
      </div>

      <PageView
        page={page}
        pageSize={dump.page_size}
        selectedSlot={selectedSlot}
        onPickSlot={setSelectedSlot}
      />
    </div>
  );
}

function PageView({
  page,
  pageSize,
  selectedSlot,
  onPickSlot,
}: {
  page: HeapPage;
  pageSize: number;
  selectedSlot: number | null;
  onPickSlot: (id: number | null) => void;
}) {
  if (page.blank) {
    return (
      <p className="font-mono text-[13px]">
        page {page.page_no}: empty (never written) — {page.free_bytes} free
        bytes
      </p>
    );
  }
  return (
    <div className="flex flex-col gap-3.5">
      {page.page_error && (
        <div className="font-mono whitespace-pre-wrap bg-danger/8 border border-danger/30 rounded p-2.5 text-danger text-[13px]">
          page error: {page.page_error}
        </div>
      )}
      <PageHeaderRow page={page} />
      <PageMap page={page} pageSize={pageSize} selectedSlot={selectedSlot} />
      <SlotTable page={page} selectedSlot={selectedSlot} onPick={onPickSlot} />
      <SlotDetail page={page} selectedSlot={selectedSlot} />
    </div>
  );
}

function PageHeaderRow({ page }: { page: HeapPage }) {
  return (
    <div className="flex flex-wrap gap-4 px-3 py-2 bg-panel2 border border-line rounded">
      <Stat label="num_slots" value={page.header.num_slots} />
      <Stat label="tuple_start" value={page.header.tuple_start} />
      <Stat label="slot_array_end" value={page.slot_array_end} />
      <Stat label="free" value={`${page.free_bytes} B`} />
      <Stat label="used" value={`${page.used_bytes} B`} />
      <Stat
        label="checksum"
        value={`0x${page.header.checksum.toString(16).padStart(8, "0")}`}
        mono
      />
    </div>
  );
}

function Stat({
  label,
  value,
  mono,
}: {
  label: string;
  value: string | number;
  mono?: boolean;
}) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[11px] uppercase text-dim tracking-[0.04em]">
        {label}
      </span>
      <span className={`font-semibold${mono ? " font-mono font-normal" : ""}`}>
        {value}
      </span>
    </div>
  );
}

// Visual memory map: header | slots → free ← tuples.
// Widths are data-driven percentages so they stay as style props.
function PageMap({
  page,
  pageSize,
  selectedSlot,
}: {
  page: HeapPage;
  pageSize: number;
  selectedSlot: number | null;
}) {
  const headerW = (PAGE_HDR_SIZE / pageSize) * 100;
  const slotsW = ((page.slot_array_end - PAGE_HDR_SIZE) / pageSize) * 100;
  const freeW = (page.free_bytes / pageSize) * 100;
  const tuplesW = (page.used_bytes / pageSize) * 100;

  const highlight = (() => {
    if (selectedSlot == null) return null;
    const s = page.slots.find(
      (sl) => sl.slot_id === selectedSlot && sl.status === "live",
    );
    if (!s || s.status !== "live") return null;
    return {
      left: (s.offset / pageSize) * 100,
      width: (s.length / pageSize) * 100,
    };
  })();

  const bandBase =
    "flex items-center justify-center text-[11px] whitespace-nowrap overflow-hidden border-r border-black/30 text-[#0d1117]";

  return (
    <div className="flex flex-col gap-1">
      {/* Bar */}
      <div className="relative flex h-9.5 border border-line rounded-[3px] overflow-hidden bg-panel2">
        <div
          className={bandBase}
          style={{ width: `${headerW}%`, background: "#e0a458" }}
        >
          hdr
        </div>
        <div
          className={bandBase}
          style={{ width: `${slotsW}%`, background: "#79c0ff" }}
        >
          slot dir
        </div>
        <div
          className="flex items-center justify-center text-[11px] whitespace-nowrap overflow-hidden border-r border-black/30 text-dim"
          style={{ width: `${freeW}%`, background: "#2d333b" }}
        >
          free
        </div>
        <div
          className={bandBase}
          style={{ width: `${tuplesW}%`, background: "#56d364" }}
        >
          tuples
        </div>
        {highlight && (
          <div
            className="absolute -top-0.5 -bottom-0.5 border-2 border-accent pointer-events-none"
            style={{ left: `${highlight.left}%`, width: `${highlight.width}%` }}
            title="selected slot bytes"
          />
        )}
      </div>

      {/* Axis */}
      <div className="relative h-4 text-[10px] text-dim">
        <span className="absolute left-0">0</span>
        <span
          className="absolute -translate-x-1/2"
          style={{ left: `${headerW}%` }}
        >
          {PAGE_HDR_SIZE}
        </span>
        <span
          className="absolute -translate-x-1/2"
          style={{ left: `${headerW + slotsW}%` }}
        >
          {page.slot_array_end}
        </span>
        <span
          className="absolute -translate-x-1/2"
          style={{ left: `${headerW + slotsW + freeW}%` }}
        >
          {page.header.tuple_start}
        </span>
        <span className="absolute right-0">{pageSize}</span>
      </div>
    </div>
  );
}

function SlotTable({
  page,
  selectedSlot,
  onPick,
}: {
  page: HeapPage;
  selectedSlot: number | null;
  onPick: (id: number | null) => void;
}) {
  if (page.slots.length === 0) {
    return <p className="font-mono text-[13px]">no slots on this page</p>;
  }
  return (
    <Table className="w-full max-w-180">
      <TableHeader>
        <TableRow>
          {["slot", "status", "offset", "length", "preview"].map((h) => (
            <TableHead
              key={h}
              className="border border-line bg-panel2 px-2.5 py-1 text-[13px] font-semibold text-fg"
            >
              {h}
            </TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {page.slots.map((s) => {
          const sel = s.slot_id === selectedSlot;
          return (
            <TableRow
              key={s.slot_id}
              onClick={() => onPick(sel ? null : s.slot_id)}
              className={[
                "cursor-pointer",
                s.status === "tombstone" ? "text-dim" : "",
                s.status === "out_of_range" ? "text-danger" : "",
                sel ? "bg-accent/20" : "hover:bg-accent/8",
              ]
                .filter(Boolean)
                .join(" ")}
            >
              <TableCell className="border border-line px-2.5 py-1 text-[13px]">
                {s.slot_id}
              </TableCell>
              <TableCell className="border border-line px-2.5 py-1 text-[13px]">
                {s.status}
              </TableCell>
              <TableCell className="border border-line px-2.5 py-1 text-[13px]">
                {s.offset}
              </TableCell>
              <TableCell className="border border-line px-2.5 py-1 text-[13px]">
                {s.length}
              </TableCell>
              <TableCell className="border border-line px-2.5 py-1 text-[13px]">
                {slotPreview(s)}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

function slotPreview(s: HeapSlot): React.ReactNode {
  if (s.status === "tombstone") {
    return <span className="text-dim italic">— deleted —</span>;
  }
  if (s.status === "out_of_range") {
    return <span className="text-danger">{s.error}</span>;
  }
  if (s.decode.ok === "false") {
    return <span className="text-danger">decode failed</span>;
  }
  return s.decode.fields
    .map((f) => (f.value === null ? "NULL" : String(f.value)))
    .join(", ");
}

function SlotDetail({
  page,
  selectedSlot,
}: {
  page: HeapPage;
  selectedSlot: number | null;
}) {
  if (selectedSlot == null) {
    return (
      <div className="p-2.5 border border-dashed border-line rounded text-dim italic bg-transparent">
        click a slot to see its raw bytes and decoded fields
      </div>
    );
  }
  const s = page.slots.find((sl) => sl.slot_id === selectedSlot);
  if (!s) return null;

  const detailClass =
    "flex flex-col gap-2 p-2.5 border border-line rounded bg-panel2";

  if (s.status === "tombstone") {
    return (
      <div className={detailClass}>
        <h4 className="m-0 text-[13px] font-semibold">
          slot {s.slot_id} — tombstone
        </h4>
        <p className="text-dim text-xs m-0">
          (offset, length) = (0, 0) means this slot was deleted or never used,
          and its slot pointer is reusable by a future insert.
        </p>
      </div>
    );
  }
  if (s.status === "out_of_range") {
    return (
      <div className={detailClass}>
        <h4 className="m-0 text-[13px] font-semibold">
          slot {s.slot_id} — out of range
        </h4>
        <div className="font-mono text-danger text-[13px] bg-danger/8 border border-danger/30 rounded p-2">
          {s.error}
        </div>
      </div>
    );
  }
  return (
    <div className={detailClass}>
      <h4 className="m-0 text-[13px] font-semibold">
        slot {s.slot_id} — bytes [{s.offset}..{s.offset + s.length})
      </h4>
      {s.decode.ok === "false" ? (
        <>
          <div className="font-mono text-danger text-[13px] bg-danger/8 border border-danger/30 rounded p-2">
            decode failed: {s.decode.error}
          </div>
          <FieldsTable
            title="fields decoded so far"
            fields={s.decode.fields_so_far}
          />
        </>
      ) : (
        <>
          <div className="flex gap-2 items-baseline">
            <span className="text-dim text-xs">null bitmap:</span>
            <code className="font-mono text-[13px]">
              {s.decode.null_bitmap_hex}
            </code>
          </div>
          <FieldsTable title="fields" fields={s.decode.fields} />
        </>
      )}
      <RawBytesView hex={s.raw_hex} />
    </div>
  );
}

function FieldsTable({
  title,
  fields,
}: {
  title: string;
  fields: { index: number; name: string; type: string; value: unknown }[];
}) {
  if (fields.length === 0) return null;
  return (
    <>
      <span className="text-dim text-[11px]">{title}</span>
      <Table>
        <TableHeader>
          <TableRow>
            {["#", "name", "type", "value"].map((h) => (
              <TableHead
                key={h}
                className="border border-line bg-panel px-2 py-0.75 text-[13px] font-semibold text-fg"
              >
                {h}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {fields.map((f) => (
            <TableRow key={f.index}>
              <TableCell className="border border-line px-2 py-0.75 text-[13px]">
                {f.index}
              </TableCell>
              <TableCell className="border border-line px-2 py-0.75 text-[13px]">
                {f.name}
              </TableCell>
              <TableCell className="border border-line px-2 py-0.75 text-[13px]">
                {f.type}
              </TableCell>
              <TableCell className="border border-line px-2 py-0.75 text-[13px]">
                {f.value === null ? (
                  <span className="text-dim italic">NULL</span>
                ) : (
                  String(f.value)
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </>
  );
}

function RawBytesView({ hex }: { hex: string }) {
  const fullBytes = useMemo(() => hexToBytes(hex), [hex]);
  const trimmed = useMemo(() => trimTrailingZeros(fullBytes), [fullBytes]);
  const showing = trimmed.length;
  const skipped = fullBytes.length - showing;
  const rows: string[][] = [];
  for (let i = 0; i < showing; i += 32) {
    rows.push(
      Array.from(trimmed.slice(i, i + 32)).map((b) =>
        b.toString(16).padStart(2, "0"),
      ),
    );
  }
  return (
    <div className="flex flex-col gap-1">
      <span className="text-dim text-[11px]">
        raw bytes ({fullBytes.length} total
        {skipped > 0 ? `, last ${skipped} omitted (zero padding)` : ""})
      </span>
      <pre className="m-0 p-2 bg-panel border border-line rounded-[3px] text-xs leading-relaxed overflow-x-auto max-h-60 overflow-y-auto">
        {rows.map((row, i) => (
          <div key={i}>
            <span className="text-dim">
              {(i * 32).toString(16).padStart(4, "0")}
            </span>
            <span> </span>
            {row.map((b, j) => (
              <span key={j} className={b === "00" ? "text-[#444]" : ""}>
                {b}
                {j === 15 ? "  " : " "}
              </span>
            ))}
          </div>
        ))}
      </pre>
    </div>
  );
}

function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function trimTrailingZeros(bytes: Uint8Array): Uint8Array {
  let end = bytes.length;
  while (end > 16 && bytes[end - 1] === 0) end -= 1;
  end = Math.min(bytes.length, end + 16);
  return bytes.subarray(0, end);
}
