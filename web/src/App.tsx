import { useEffect } from "react";
import { Routes, Route, useNavigate, useParams } from "react-router-dom";
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
  Tabs,
  TabsList,
  TabsTrigger,
  TabsContent,
} from "./components/ui/tabs";
import {
  DatabasePicker,
  type DatabaseSelection,
} from "./components/DatabasePicker";
import { CreateTableWizard } from "./components/CreateTableWizard";
import { useDatabaseStore, useQueryStore, useUiStore, type RunState } from "./store";
import type { QueryResult } from "./types/api";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<DatabasePickerPage />} />
      <Route path="/:dbName" element={<WorkspacePage />} />
    </Routes>
  );
}

function DatabasePickerPage() {
  const navigate = useNavigate();
  const setDb = useDatabaseStore((s) => s.setDb);

  const handleSelect = (sel: DatabaseSelection) => {
    setDb(sel);
    navigate(`/${sel.name}`);
  };

  return <DatabasePicker onSelect={handleSelect} />;
}

function WorkspacePage() {
  const { dbName } = useParams<{ dbName: string }>();
  const db = useDatabaseStore((s) => s.db);
  const setDb = useDatabaseStore((s) => s.setDb);

  useEffect(() => {
    if (dbName && (!db || db.name !== dbName)) {
      setDb({ name: dbName });
    }
  }, [dbName]);

  if (!db) return null;
  return <Workspace />;
}

function Workspace() {
  const db = useDatabaseStore((s) => s.db)!;
  const refreshTables = useDatabaseStore((s) => s.refreshTables);
  const tables = useDatabaseStore((s) => s.tables);

  const sql = useQueryStore((s) => s.sql);
  const setSql = useQueryStore((s) => s.setSql);
  const runState = useQueryStore((s) => s.runState);
  const heapTick = useQueryStore((s) => s.heapTick);
  const runSql = useQueryStore((s) => s.runSql);
  const resetSql = useQueryStore((s) => s.resetSql);

  const tab = useUiStore((s) => s.tab);
  const setTab = useUiStore((s) => s.setTab);
  const selectedTable = useUiStore((s) => s.selectedTable);
  const setSelectedTable = useUiStore((s) => s.setSelectedTable);
  const wizardOpen = useUiStore((s) => s.wizardOpen);
  const openWizard = useUiStore((s) => s.openWizard);
  const closeWizard = useUiStore((s) => s.closeWizard);
  const resetUi = useUiStore((s) => s.reset);

  const setDb = useDatabaseStore((s) => s.setDb);
  const navigate = useNavigate();

  useEffect(() => {
    resetSql(db.initialSql);
    resetUi();
    refreshTables();
  }, [db.name]);

  const onExit = () => {
    resetUi();
    setDb(null);
    navigate("/");
  };

  const onWizardCreate = (generatedSql: string) => {
    closeWizard();
    setSql(generatedSql);
    runSql(generatedSql);
  };

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
        <span className="font-mono text-accent text-[13px]">{db.name}</span>
        <span className="text-dim font-normal text-xs ml-0.5">/ StoreMy</span>
      </div>

      {/* Sidebar */}
      <TableList
        tables={tables}
        selected={selectedTable}
        onSelect={(name) => {
          setSelectedTable(name);
          setTab("heap");
        }}
        onNewTable={openWizard}
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
              onClick={() => runSql()}
              disabled={runState.status === "running"}
            >
              {runState.status === "running" ? "Running…" : "Run"}
            </button>
            <span className="text-dim text-xs">Cmd/Ctrl + Enter</span>
          </div>
          <SqlEditor value={sql} onChange={setSql} onRun={() => runSql()} />
        </div>

        {/* Results pane */}
        <div className="flex flex-col min-h-0 border-t border-line">
          <div className="flex flex-col shrink-0 border-b border-line bg-panel text-xs text-dim">
            <div className="flex px-2">
              <TabButton active={tab === "results"} onClick={() => setTab("results")}>
                Results
              </TabButton>
              <TabButton
                active={tab === "heap"}
                onClick={() => setTab("heap")}
                disabled={!selectedTable}
                title={selectedTable ? "" : "select a table from the sidebar"}
              >
                Heap{selectedTable ? ` · ${selectedTable}` : ""}
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

          <div className="flex-1 overflow-auto p-3">
            {tab === "results" ? (
              <ResultsBody state={runState} />
            ) : selectedTable ? (
              <HeapTabBody db={db.name} table={selectedTable} refreshTick={heapTick} />
            ) : (
              <span className="font-mono text-[13px]">
                select a table from the sidebar
              </span>
            )}
          </div>
        </div>
      </div>

      {wizardOpen && (
        <CreateTableWizard onClose={closeWizard} onCreate={onWizardCreate} />
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
    <Tabs defaultValue="table" className="flex h-full flex-col">
      <TabsList variant="line" className="shrink-0 self-start">
        <TabsTrigger value="table">Table</TabsTrigger>
        <TabsTrigger value="heap">Heap Inspector</TabsTrigger>
      </TabsList>
      <TabsContent value="table" className="min-h-0 flex-1 overflow-auto pt-2">
        <TableContentPreview db={db} table={table} refreshTick={refreshTick} />
      </TabsContent>
      <TabsContent
        value="heap"
        className="min-h-0 flex-1 overflow-auto rounded border border-line bg-panel/50 p-3 pt-2"
      >
        <HeapInspector db={db} table={table} refreshTick={refreshTick} />
      </TabsContent>
    </Tabs>
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
              <TableCell className="border border-line px-2.5 py-1">{row.name}</TableCell>
              <TableCell className="border border-line px-2.5 py-1">{row.table}</TableCell>
              <TableCell className="border border-line px-2.5 py-1">{row.columns.join(", ")}</TableCell>
              <TableCell className="border border-line px-2.5 py-1">{row.kind}</TableCell>
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
