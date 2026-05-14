// Visualises a single heap page: header, slot directory, free region, tuple
// region, and per-slot decoded fields. The point is to make the slotted-page
// layout LITERAL — slots grow forward from byte PAGE_HDR_SIZE, tuples grow
// backward from PAGE_SIZE. Pick a slot to see its raw bytes and decode.
import { useEffect, useMemo, useState } from "react";
import type { HeapDump, HeapPage, HeapSlot } from "../types/api";
import { fetchHeap, StoremyError } from "../api/client";

interface Props {
  db: string;
  table: string;
  /** Bumped when a query succeeds, so we re-fetch and reflect new tuples. */
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

  // Reset selection when the visible page changes.
  useEffect(() => {
    setSelectedSlot(null);
  }, [pageIdx, table]);

  if (state.status === "loading") {
    return <div className="status-msg">loading heap …</div>;
  }
  if (state.status === "err") {
    return <div className="error-box">{state.message}</div>;
  }

  const dump = state.dump;
  if (dump.pages.length === 0) {
    return <div className="status-msg">no pages allocated yet for '{table}'</div>;
  }
  const page = dump.pages[Math.min(pageIdx, dump.pages.length - 1)];

  return (
    <div className="heap-inspector">
      <div className="heap-toolbar">
        <strong>{dump.table}</strong>
        <span className="dim">
          {dump.field_names.join(", ")} · {dump.page_count} page
          {dump.page_count === 1 ? "" : "s"} · page_size={dump.page_size}
        </span>
        {dump.pages.length > 1 && (
          <span className="page-picker">
            page&nbsp;
            <select
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
      <div className="page-view">
        <div className="status-msg">
          page {page.page_no}: empty (never written) — {page.free_bytes} free bytes
        </div>
      </div>
    );
  }
  return (
    <div className="page-view">
      {page.page_error && (
        <div className="error-box">page error: {page.page_error}</div>
      )}
      <PageHeaderRow page={page} />
      <PageMap page={page} pageSize={pageSize} selectedSlot={selectedSlot} />
      <SlotTable
        page={page}
        selectedSlot={selectedSlot}
        onPick={onPickSlot}
      />
      <SlotDetail page={page} selectedSlot={selectedSlot} />
    </div>
  );
}

function PageHeaderRow({ page }: { page: HeapPage }) {
  return (
    <div className="page-header">
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
    <div className="stat">
      <span className="stat-label">{label}</span>
      <span className={mono ? "stat-value mono" : "stat-value"}>{value}</span>
    </div>
  );
}

// Visual map of the page: header | slot pointers → free region ← tuple region.
// Width is proportional to byte ranges so the eye can see how full the page is.
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

  // Highlight the selected slot's tuple region inside the tuple band.
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

  return (
    <div className="page-map">
      <div className="page-map-bar">
        <div className="band header" style={{ width: `${headerW}%` }}>
          hdr
        </div>
        <div className="band slots" style={{ width: `${slotsW}%` }}>
          slot dir
        </div>
        <div className="band free" style={{ width: `${freeW}%` }}>
          free
        </div>
        <div className="band tuples" style={{ width: `${tuplesW}%` }}>
          tuples
        </div>
        {highlight && (
          <div
            className="band-highlight"
            style={{ left: `${highlight.left}%`, width: `${highlight.width}%` }}
            title={`selected slot bytes`}
          />
        )}
      </div>
      <div className="page-map-axis">
        <span>0</span>
        <span style={{ left: `${headerW}%` }}>{PAGE_HDR_SIZE}</span>
        <span style={{ left: `${headerW + slotsW}%` }}>
          {page.slot_array_end}
        </span>
        <span style={{ left: `${headerW + slotsW + freeW}%` }}>
          {page.header.tuple_start}
        </span>
        <span style={{ right: 0 }}>{pageSize}</span>
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
    return <div className="status-msg">no slots on this page</div>;
  }
  return (
    <table className="slots">
      <thead>
        <tr>
          <th>slot</th>
          <th>status</th>
          <th>offset</th>
          <th>length</th>
          <th>preview</th>
        </tr>
      </thead>
      <tbody>
        {page.slots.map((s) => {
          const sel = s.slot_id === selectedSlot;
          return (
            <tr
              key={s.slot_id}
              className={`slot-row ${s.status} ${sel ? "selected" : ""}`}
              onClick={() => onPick(sel ? null : s.slot_id)}
            >
              <td>{s.slot_id}</td>
              <td>{s.status}</td>
              <td>{s.offset}</td>
              <td>{s.length}</td>
              <td>{slotPreview(s)}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function slotPreview(s: HeapSlot): React.ReactNode {
  if (s.status === "tombstone") {
    return <span className="dim">— deleted —</span>;
  }
  if (s.status === "out_of_range") {
    return <span className="err">{s.error}</span>;
  }
  if (s.decode.ok === "false") {
    return <span className="err">decode failed</span>;
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
      <div className="slot-detail empty">
        click a slot to see its raw bytes and decoded fields
      </div>
    );
  }
  const s = page.slots.find((sl) => sl.slot_id === selectedSlot);
  if (!s) return null;

  if (s.status === "tombstone") {
    return (
      <div className="slot-detail">
        <h4>slot {s.slot_id} — tombstone</h4>
        <p className="dim">
          (offset, length) = (0, 0) means this slot was deleted or never used,
          and its slot pointer is reusable by a future insert.
        </p>
      </div>
    );
  }
  if (s.status === "out_of_range") {
    return (
      <div className="slot-detail">
        <h4>slot {s.slot_id} — out of range</h4>
        <div className="error-box">{s.error}</div>
      </div>
    );
  }
  return (
    <div className="slot-detail">
      <h4>
        slot {s.slot_id} — bytes [{s.offset}..{s.offset + s.length})
      </h4>
      {s.decode.ok === "false" ? (
        <>
          <div className="error-box">decode failed: {s.decode.error}</div>
          <FieldsTable
            title="fields decoded so far"
            fields={s.decode.fields_so_far}
          />
        </>
      ) : (
        <>
          <div className="kv">
            <span className="dim">null bitmap:</span>
            <code>{s.decode.null_bitmap_hex}</code>
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
      <div className="dim small">{title}</div>
      <table className="fields">
        <thead>
          <tr>
            <th>#</th>
            <th>name</th>
            <th>type</th>
            <th>value</th>
          </tr>
        </thead>
        <tbody>
          {fields.map((f) => (
            <tr key={f.index}>
              <td>{f.index}</td>
              <td>{f.name}</td>
              <td>{f.type}</td>
              <td>
                {f.value === null ? <span className="dim">NULL</span> : String(f.value)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

// Hex bytes display, 32 bytes per row with row offsets. We collapse very long
// runs of trailing zeros (which happen because string fields are padded to
// STRING_MAX_SIZE on disk) so the panel doesn't dominate the screen.
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
    <div className="raw-bytes">
      <div className="dim small">
        raw bytes ({fullBytes.length} total
        {skipped > 0 ? `, last ${skipped} omitted (zero padding)` : ""})
      </div>
      <pre className="hex">
        {rows.map((row, i) => (
          <div key={i}>
            <span className="hex-offset">
              {(i * 32).toString(16).padStart(4, "0")}
            </span>
            <span> </span>
            {row.map((b, j) => (
              <span key={j} className={b === "00" ? "zero" : ""}>
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
  // Keep up to 16 trailing zeros visible so the run is obvious without
  // scrolling forever.
  end = Math.min(bytes.length, end + 16);
  return bytes.subarray(0, end);
}
