import { create } from "zustand";
import { runQuery, StoremyError } from "../api/client";
import type { ApiError, QueryResult } from "../types/api";
import { useDatabaseStore } from "./database";
import { useUiStore } from "./ui";

const SAMPLE_SQL = `-- Welcome to StoreMy.
-- Cmd/Ctrl + Enter to run the highlighted block (or all of it).

CREATE TABLE users (id INT, name VARCHAR);
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
SELECT * FROM users;`;

export type RunState =
  | { status: "idle" }
  | { status: "running" }
  | { status: "ok"; result: QueryResult; ms: number }
  | { status: "err"; error: ApiError; ms: number };

interface QueryState {
  sql: string;
  runState: RunState;
  heapTick: number;
  setSql: (sql: string) => void;
  runSql: (input?: string) => Promise<void>;
  resetSql: (initial?: string) => void;
}

export const useQueryStore = create<QueryState>((set, get) => ({
  sql: SAMPLE_SQL,
  runState: { status: "idle" },
  heapTick: 0,

  setSql: (sql) => set({ sql }),

  resetSql: (initial) => set({ sql: initial ?? SAMPLE_SQL, runState: { status: "idle" } }),

  runSql: async (input) => {
    const sql = input ?? get().sql;
    if (!sql.trim()) return;

    const db = useDatabaseStore.getState().db;
    if (!db) return;

    set({ runState: { status: "running" } });
    const t0 = performance.now();

    try {
      const result = await runQuery(db.name, sql);
      const ms = Math.round(performance.now() - t0);
      set((s) => ({ runState: { status: "ok", result, ms }, heapTick: s.heapTick + 1 }));
      useUiStore.getState().setTab("results");
      await useDatabaseStore.getState().refreshTables();
    } catch (e) {
      const ms = Math.round(performance.now() - t0);
      const error: ApiError =
        e instanceof StoremyError
          ? { kind: e.kind, message: e.message }
          : { kind: "internal", message: String(e) };
      set({ runState: { status: "err", error, ms } });
    }
  },
}));
