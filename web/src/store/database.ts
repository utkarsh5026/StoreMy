import { create } from "zustand";
import { listTables } from "../api/client";
import type { TableSummary } from "../types/api";
import type { DatabaseSelection } from "../components/DatabasePicker";

interface DatabaseState {
  db: DatabaseSelection | null;
  tables: TableSummary[];
  setDb: (db: DatabaseSelection | null) => void;
  refreshTables: () => Promise<void>;
}

export const useDatabaseStore = create<DatabaseState>((set, get) => ({
  db: null,
  tables: [],

  setDb: (db) => set({ db, tables: [] }),

  refreshTables: async () => {
    const { db } = get();
    if (!db) return;
    try {
      set({ tables: await listTables(db.name) });
    } catch {
      // Sidebar shows empty if server is down — that's fine.
    }
  },
}));
