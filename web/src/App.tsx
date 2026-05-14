import { useCallback, useEffect, useState } from "react";
import { SqlEditor } from "./components/SqlEditor";
import { ResultsTable } from "./components/ResultsTable";
import { TableList } from "./components/TableList";
import { HeapInspector } from "./components/HeapInspector";
import { TableContentPreview } from "./components/TableContentPreview";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./components/ui/table";
import {
  DatabasePicker,
  type DatabaseSelection,
} from "./components/DatabasePicker";
import { CreateTableWizard } from "./components/CreateTableWizard";
import { listTables, runQuery, StoremyError } from "./api/client";
import type { ApiError, QueryResult, TableSummary } from "./types/api";

const SAMPLE_SQL = `-- Welcome to StoreMy.
-- Cmd/Ctrl + Enter to run the highlighted block (or all of it).

CREATE TABLE users (id INT, name VARCHAR);
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
SELECT * FROM users;`;

type RunState =
  | { status: "idle" }
  | { status: "running" }
  | { status: "ok"; result: QueryResult; ms: number }
  | { status: "err"; error: ApiError; ms: number };

type Tab = "results" | "heap";

export default function App() {
  const [selectedDb, setSelectedDb] = useState<DatabaseSelection | null>(null);

  if (!selectedDb) {
    return <DatabasePicker onSelect={setSelectedDb} />;
  }

  return (
    <Workspace
      db={selectedDb.name}
      initialSql={selectedDb.initialSql}
      onExit={() => setSelectedDb(null)}
    />
  );
}

function Workspace({
  db,
  initialSql,
  onExit,
}: {
  db: string;
  initialSql?: string;
  onExit: () => void;
}) {
  const [sql, setSql] = useState(initialSql ?? SAMPLE_SQL);
  const [tables, setTables] = useState<TableSummary[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [runState, setRunState] = useState<RunState>({ status: "idle" });
  const [tab, setTab] = useState<Tab>("results");
  const [heapTick, setHeapTick] = useState(0);
  const [wizardOpen, setWizardOpen] = useState(false);

  const refreshTables = useCallback(async () => {
    try {
      setTables(await listTables(db));
    } catch {
      // Ignore — sidebar will just show empty if the server is down.
    }
  }, [db]);

  useEffect(() => {
    refreshTables();
  }, [refreshTables]);

  const runSql = useCallback(
    async (input: string) => {
      if (!input.trim()) return;
      setRunState({ status: "running" });
      const t0 = performance.now();
      try {
        const result = await runQuery(db, input);
        const ms = Math.round(performance.now() - t0);
        setRunState({ status: "ok", result, ms });
        setTab("results");
        setHeapTick((t) => t + 1);
        await refreshTables();
      } catch (e) {
        const ms = Math.round(performance.now() - t0);
        const error: ApiError =
          e instanceof StoremyError
            ? { kind: e.kind, message: e.message }
            : { kind: "internal", message: String(e) };
        setRunState({ status: "err", error, ms });
      }
    },
    [db, refreshTables],
  );

  const onRun = useCallback(() => runSql(sql), [sql, runSql]);

  const onWizardCreate = useCallback(
    (generatedSql: string) => {
      setWizardOpen(false);
      setSql(generatedSql);
      runSql(generatedSql);
    },
    [runSql],
  );

  return (
    <div
      className="grid h-full grid-cols-[240px_1fr] grid-rows-[40px_1fr]"
      style={{ gridTemplateAreas: '"header header" "sidebar main"' }}
    >
      {/* Header */}
      <div className="[grid-area:header] flex items-center gap-2.5 px-4 border-b border-line bg-panel font-semibold tracking-[0.04em]">
        <button
          className="btn-ghost text-xs px-2.5 py-1"
          onClick={onExit}
          title="Switch database"
        >
          ← Databases
        </button>
        <span className="font-mono text-accent text-[13px]">{db}</span>
        <span className="text-dim font-normal text-xs ml-0.5">/ StoreMy</span>
      </div>

      {/* Sidebar */}
      <TableList
        tables={tables}
        selected={selected}
        onSelect={(name) => {
          setSelected(name);
          setTab("heap");
        }}
        onNewTable={() => setWizardOpen(true)}
      />

      {/* Main area */}
      <div
        className="[grid-area:main] grid min-h-0"
        style={{ gridTemplateRows: "40% 60%" }}
      >
        {/* Editor pane */}
        <div className="flex flex-col min-h-0">
          <div className="flex items-center gap-3 px-3 py-2 border-b border-line bg-panel shrink-0">
            <button
              className="btn"
              onClick={onRun}
              disabled={runState.status === "running"}
            >
              {runState.status === "running" ? "Running…" : "Run"}
            </button>
            <span className="text-dim text-xs">Cmd/Ctrl + Enter</span>
          </div>
          <SqlEditor value={sql} onChange={setSql} onRun={onRun} />
        </div>

        {/* Results pane */}
        <div className="flex flex-col min-h-0 border-t border-line">
          {/* Tabs + status bar */}
          <div className="flex flex-col shrink-0 border-b border-line bg-panel text-xs text-dim">
            <div className="flex px-2">
              <TabButton
                active={tab === "results"}
                onClick={() => setTab("results")}
              >
                Results
              </TabButton>
              <TabButton
                active={tab === "heap"}
                onClick={() => setTab("heap")}
                disabled={!selected}
                title={selected ? "" : "select a table from the sidebar"}
              >
                Heap{selected ? ` · ${selected}` : ""}
              </TabButton>
            </div>
            <div className="flex gap-4 px-3 py-1.5">
              {tab === "results" ? (
                <ResultsHeader state={runState} />
              ) : (
                <span className="text-dim">
                  live page bytes — refreshes after each run
                </span>
              )}
            </div>
          </div>

          {/* Results body */}
          <div className="flex-1 overflow-auto p-3">
            {tab === "results" ? (
              <ResultsBody state={runState} />
            ) : selected ? (
              <HeapTabBody db={db} table={selected} refreshTick={heapTick} />
            ) : (
              <span className="font-mono text-[13px]">
                select a table from the sidebar
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Create Table Wizard */}
      {wizardOpen && (
        <CreateTableWizard
          onClose={() => setWizardOpen(false)}
          onCreate={onWizardCreate}
        />
      )}
    </div>
  );
}

function HeapTabBody({
  db,
  table,
  refreshTick,
}: {
  db: string;
  table: string;
  refreshTick: number;
}) {
  return (
    <div className="grid min-h-full gap-3 xl:grid-cols-[minmax(320px,0.85fr)_minmax(480px,1.15fr)]">
      <TableContentPreview db={db} table={table} refreshTick={refreshTick} />
      <section className="min-h-0 rounded border border-line bg-panel/50 p-3">
        <HeapInspector db={db} table={table} refreshTick={refreshTick} />
      </section>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  disabled,
  title,
  children,
}: {
  active: boolean;
  onClick: () => void;
  disabled?: boolean;
  title?: string;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      title={title}
      className={[
        "bg-transparent border-0 border-b-2 px-3.5 py-1.5 cursor-pointer font-medium text-xs rounded-none",
        active ? "text-fg border-accent" : "text-dim border-transparent",
        disabled ? "cursor-not-allowed text-[#555]" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      {children}
    </button>
  );
}

function ResultsHeader({ state }: { state: RunState }) {
  switch (state.status) {
    case "idle":
      return <span>Ready.</span>;
    case "running":
      return <span>Running…</span>;
    case "ok":
      return (
        <>
          <span className="text-good">OK</span>
          <span>{summariseResult(state.result)}</span>
          <span>{state.ms} ms</span>
        </>
      );
    case "err":
      return (
        <>
          <span className="text-danger">ERROR ({state.error.kind})</span>
          <span>{state.ms} ms</span>
        </>
      );
  }
}

function ResultsBody({ state }: { state: RunState }) {
  if (state.status === "idle" || state.status === "running") return null;
  if (state.status === "err") {
    return (
      <div className="font-mono whitespace-pre-wrap bg-danger/8 border border-danger/30 rounded p-2.5 text-danger text-[13px]">
        <span className="inline-block px-1.5 py-0.5 rounded-sm bg-danger/20 mr-2 text-[11px] uppercase tracking-wider">
          {state.error.kind}
        </span>
        {state.error.message}
      </div>
    );
  }
  const r = state.result;
  if (r.kind === "selected") {
    return <ResultsTable columns={r.columns} rows={r.rows} />;
  }
  if (r.kind === "indexes_shown") {
    return (
      <Table className="font-mono text-[13px]">
        <TableHeader>
          <TableRow>
            {["name", "table", "columns", "kind"].map((h) => (
              <TableHead
                key={h}
                className="border border-line bg-panel2 px-2.5 py-1 font-semibold text-fg"
              >
                {h}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {r.rows.map((row) => (
            <TableRow key={row.name}>
              <TableCell className="border border-line px-2.5 py-1">
                {row.name}
              </TableCell>
              <TableCell className="border border-line px-2.5 py-1">
                {row.table}
              </TableCell>
              <TableCell className="border border-line px-2.5 py-1">
                {row.columns.join(", ")}
              </TableCell>
              <TableCell className="border border-line px-2.5 py-1">
                {row.kind}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    );
  }
  return <div className="font-mono text-[13px]">{summariseResult(r)}</div>;
}

function summariseResult(r: QueryResult): string {
  switch (r.kind) {
    case "table_created":
      return r.already_exists
        ? `table '${r.name}' already exists`
        : `created table '${r.name}' (file ${r.file_id})`;
    case "table_dropped":
      return `dropped table '${r.name}'`;
    case "index_created":
      return r.already_exists
        ? `index '${r.name}' already exists`
        : `created index '${r.name}' on '${r.table}'`;
    case "index_dropped":
      return `dropped index '${r.name}'`;
    case "indexes_shown":
      return r.scope
        ? `${r.rows.length} index(es) on '${r.scope}'`
        : `${r.rows.length} index(es)`;
    case "inserted":
      return `inserted ${r.rows} row(s) into '${r.table}'`;
    case "deleted":
      return `deleted ${r.rows} row(s) from '${r.table}'`;
    case "updated":
      return `updated ${r.rows} row(s) in '${r.table}'`;
    case "selected":
      return `${r.rows.length} row(s) from '${r.table}'`;
    default:
      return "";
  }
}
