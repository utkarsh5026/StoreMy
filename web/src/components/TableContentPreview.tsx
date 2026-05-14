import { useEffect, useState } from "react";
import { runQuery, StoremyError } from "../api/client";
import type { ApiError, QueryRows } from "../types/api";
import { ResultsTable } from "./ResultsTable";

interface Props {
  db: string;
  table: string;
  refreshTick: number;
}

type PreviewState =
  | { status: "loading" }
  | { status: "ok"; result: QueryRows; ms: number }
  | { status: "err"; error: ApiError; ms: number };

const SIMPLE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function TableContentPreview({ db, table, refreshTick }: Props) {
  const [state, setState] = useState<PreviewState>({ status: "loading" });

  useEffect(() => {
    let alive = true;
    const t0 = performance.now();

    if (!SIMPLE_IDENTIFIER.test(table)) {
      setState({
        status: "err",
        error: {
          kind: "unsupported",
          message: `cannot preview table '${table}' because it is not a simple identifier`,
        },
        ms: 0,
      });
      return () => {
        alive = false;
      };
    }

    setState({ status: "loading" });
    runQuery(db, `SELECT * FROM ${table}`)
      .then((result) => {
        if (!alive) return;
        const ms = Math.round(performance.now() - t0);
        if (result.kind !== "selected") {
          setState({
            status: "err",
            error: {
              kind: "internal",
              message: `preview query returned '${result.kind}' instead of rows`,
            },
            ms,
          });
          return;
        }
        setState({ status: "ok", result, ms });
      })
      .catch((e) => {
        if (!alive) return;
        const ms = Math.round(performance.now() - t0);
        const error: ApiError =
          e instanceof StoremyError
            ? { kind: e.kind, message: e.message }
            : { kind: "internal", message: String(e) };
        setState({ status: "err", error, ms });
      });

    return () => {
      alive = false;
    };
  }, [db, table, refreshTick]);

  return (
    <section className="flex min-h-0 flex-col gap-2 rounded border border-line bg-panel/50 p-3">
      <div className="flex items-baseline justify-between gap-3">
        <div>
          <h3 className="m-0 text-sm font-semibold">Table contents</h3>
          <p className="m-0 font-mono text-[11px] text-dim">{table}</p>
        </div>
        <PreviewMeta state={state} />
      </div>
      <div className="min-h-0 overflow-auto">
        <PreviewBody state={state} />
      </div>
    </section>
  );
}

function PreviewMeta({ state }: { state: PreviewState }) {
  switch (state.status) {
    case "loading":
      return <span className="text-xs text-dim">loading rows...</span>;
    case "ok":
      return (
        <span className="text-xs text-dim">
          {state.result.rows.length} row(s) · {state.ms} ms
        </span>
      );
    case "err":
      return (
        <span className="text-xs text-danger">
          error ({state.error.kind}) · {state.ms} ms
        </span>
      );
  }
}

function PreviewBody({ state }: { state: PreviewState }) {
  switch (state.status) {
    case "loading":
      return <p className="font-mono text-[13px] text-dim">loading rows...</p>;
    case "ok":
      return <ResultsTable columns={state.result.columns} rows={state.result.rows} />;
    case "err":
      return (
        <div className="font-mono whitespace-pre-wrap bg-danger/[0.08] border border-danger/30 rounded p-2.5 text-danger text-[13px]">
          {state.error.message}
        </div>
      );
  }
}
