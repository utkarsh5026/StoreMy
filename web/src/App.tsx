import { useCallback, useEffect, useState } from "react";
import { SqlEditor } from "./components/SqlEditor";
import { ResultsTable } from "./components/ResultsTable";
import { TableList } from "./components/TableList";
import { HeapInspector } from "./components/HeapInspector";
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
  const [sql, setSql] = useState(SAMPLE_SQL);
  const [tables, setTables] = useState<TableSummary[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [runState, setRunState] = useState<RunState>({ status: "idle" });
  const [tab, setTab] = useState<Tab>("results");
  // Bumped on every successful run so HeapInspector knows to re-fetch and
  // pick up newly-inserted/deleted rows.
  const [heapTick, setHeapTick] = useState(0);

  const refreshTables = useCallback(async () => {
    try {
      setTables(await listTables());
    } catch {
      // Ignore — sidebar will just show empty if the server is down.
    }
  }, []);

  useEffect(() => {
    refreshTables();
  }, [refreshTables]);

  const onRun = useCallback(async () => {
    // The engine accepts one statement at a time. If the editor holds
    // a selection, run that; otherwise treat the whole editor as one
    // statement (the parser will reject multi-statement input — surfaced
    // as a parse error, which is fine for v1).
    const input = sql.trim();
    if (input === "") return;
    setRunState({ status: "running" });
    const t0 = performance.now();
    try {
      const result = await runQuery(input);
      const ms = Math.round(performance.now() - t0);
      setRunState({ status: "ok", result, ms });
      setTab("results");
      setHeapTick((t) => t + 1);
      // Any DDL/DML may have changed the catalog — refresh.
      await refreshTables();
    } catch (e) {
      const ms = Math.round(performance.now() - t0);
      const error: ApiError =
        e instanceof StoremyError
          ? { kind: e.kind, message: e.message }
          : { kind: "internal", message: String(e) };
      setRunState({ status: "err", error, ms });
    }
  }, [sql, refreshTables]);

  return (
    <div className="app">
      <div className="header">
        StoreMy <span className="dim">/ web v0.1</span>
      </div>
      <TableList
        tables={tables}
        selected={selected}
        onSelect={(name) => {
          setSelected(name);
          setTab("heap");
        }}
      />
      <div className="main">
        <div className="editor-pane">
          <div className="editor-toolbar">
            <button onClick={onRun} disabled={runState.status === "running"}>
              {runState.status === "running" ? "Running…" : "Run"}
            </button>
            <span className="hint">Cmd/Ctrl + Enter</span>
          </div>
          <SqlEditor value={sql} onChange={setSql} onRun={onRun} />
        </div>
        <div className="results-pane">
          <div className="results-toolbar">
            <div className="tabs">
              <button
                className={`tab ${tab === "results" ? "active" : ""}`}
                onClick={() => setTab("results")}
              >
                Results
              </button>
              <button
                className={`tab ${tab === "heap" ? "active" : ""}`}
                onClick={() => setTab("heap")}
                disabled={!selected}
                title={selected ? "" : "select a table from the sidebar"}
              >
                Heap{selected ? ` · ${selected}` : ""}
              </button>
            </div>
            <div className="toolbar-status">
              {tab === "results" ? (
                <ResultsHeader state={runState} />
              ) : (
                <span className="dim">live page bytes — refreshes after each run</span>
              )}
            </div>
          </div>
          <div className="results-body">
            {tab === "results" ? (
              <ResultsBody state={runState} />
            ) : selected ? (
              <HeapInspector table={selected} refreshTick={heapTick} />
            ) : (
              <div className="status-msg">select a table from the sidebar</div>
            )}
          </div>
        </div>
      </div>
    </div>
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
          <span className="ok">OK</span>
          <span>{summariseResult(state.result)}</span>
          <span>{state.ms} ms</span>
        </>
      );
    case "err":
      return (
        <>
          <span className="err">ERROR ({state.error.kind})</span>
          <span>{state.ms} ms</span>
        </>
      );
  }
}

function ResultsBody({ state }: { state: RunState }) {
  if (state.status === "idle" || state.status === "running") return null;
  if (state.status === "err") {
    return (
      <div className="error-box">
        <span className="kind">{state.error.kind}</span>
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
      <table className="results">
        <thead>
          <tr>
            <th>name</th>
            <th>table</th>
            <th>columns</th>
            <th>kind</th>
          </tr>
        </thead>
        <tbody>
          {r.rows.map((row) => (
            <tr key={row.name}>
              <td>{row.name}</td>
              <td>{row.table}</td>
              <td>{row.columns.join(", ")}</td>
              <td>{row.kind}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  }
  // Non-row results (created/inserted/updated/etc.) — render the summary.
  return <div className="status-msg">{summariseResult(r)}</div>;
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
      return r.scope ? `${r.rows.length} index(es) on '${r.scope}'` : `${r.rows.length} index(es)`;
    case "inserted":
      return `inserted ${r.rows} row(s) into '${r.table}'`;
    case "deleted":
      return `deleted ${r.rows} row(s) from '${r.table}'`;
    case "updated":
      return `updated ${r.rows} row(s) in '${r.table}'`;
    case "selected":
      return `${r.rows.length} row(s) from '${r.table}'`;
  }
}
