import { useCallback, useEffect, useRef, useState } from "react";
import { createDatabase, listDatabases, StoremyError } from "../api/client";
import type { DatabaseSummary } from "../types/api";

interface Props {
  onSelect: (name: string) => void;
}

type LoadState =
  | { status: "loading" }
  | { status: "ok"; databases: DatabaseSummary[] }
  | { status: "err"; message: string };

type CreateState =
  | { status: "idle" }
  | { status: "creating" }
  | { status: "err"; message: string };

export function DatabasePicker({ onSelect }: Props) {
  const [load, setLoad] = useState<LoadState>({ status: "loading" });
  const [create, setCreate] = useState<CreateState>({ status: "idle" });
  const [newName, setNewName] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  const refresh = useCallback(async () => {
    try {
      const dbs = await listDatabases();
      setLoad({ status: "ok", databases: dbs });
    } catch (e) {
      const msg = e instanceof StoremyError ? e.message : String(e);
      setLoad({ status: "err", message: msg });
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const handleCreate = useCallback(async () => {
    const name = newName.trim();
    if (!name) return;
    setCreate({ status: "creating" });
    try {
      await createDatabase(name);
      setNewName("");
      setCreate({ status: "idle" });
      await refresh();
    } catch (e) {
      const msg = e instanceof StoremyError ? e.message : String(e);
      setCreate({ status: "err", message: msg });
    }
  }, [newName, refresh]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") handleCreate();
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-full bg-bg px-4 py-10">
      {/* Logo / heading */}
      <div className="text-center mb-9">
        <span className="block text-[28px] font-bold tracking-[0.06em] text-fg mb-1.5">
          StoreMy
        </span>
        <span className="text-dim text-sm">Select a database to continue</span>
      </div>

      {/* Card */}
      <div className="w-full max-w-105 flex flex-col gap-6">
        {/* Database list */}
        <div className="bg-panel border border-line rounded-[6px] overflow-hidden min-h-15 flex flex-col justify-center">
          {load.status === "loading" && (
            <p className="text-dim text-[13px] italic px-4 py-4 text-center">
              Loading…
            </p>
          )}
          {load.status === "err" && (
            <div className="font-mono whitespace-pre-wrap bg-danger/8 border-b border-danger/30 p-3 text-danger text-[13px]">
              {load.message}
            </div>
          )}
          {load.status === "ok" && load.databases.length === 0 && (
            <p className="text-dim text-[13px] italic px-4 py-4 text-center">
              No databases yet. Create one below.
            </p>
          )}
          {load.status === "ok" && load.databases.length > 0 && (
            <ul className="list-none m-0 p-0">
              {load.databases.map((db) => (
                <li
                  key={db.name}
                  onClick={() => onSelect(db.name)}
                  className="flex items-center gap-2.5 px-4 py-3 cursor-pointer border-b border-line last:border-b-0 hover:bg-panel2 transition-colors"
                >
                  <span className="text-accent text-sm">⬡</span>
                  <span className="font-mono text-sm font-semibold flex-1">
                    {db.name}
                  </span>
                  <span className="text-dim text-[13px]">→</span>
                </li>
              ))}
            </ul>
          )}
        </div>

        {/* Create new database */}
        <div className="flex flex-col gap-2">
          <p className="text-[11px] uppercase tracking-[0.08em] text-dim">
            New database
          </p>
          <div className="flex gap-2">
            <input
              ref={inputRef}
              className="input-field flex-1"
              type="text"
              placeholder="my_database"
              value={newName}
              onChange={(e) => {
                setNewName(e.target.value);
                setCreate({ status: "idle" });
              }}
              onKeyDown={handleKeyDown}
              disabled={create.status === "creating"}
            />
            <button
              className="btn"
              onClick={handleCreate}
              disabled={!newName.trim() || create.status === "creating"}
            >
              {create.status === "creating" ? "Creating…" : "Create"}
            </button>
          </div>
          {create.status === "err" && (
            <p className="text-danger text-xs font-mono">{create.message}</p>
          )}
          <p className="text-dim text-[11px]">
            Letters, digits, and underscores only. Must start with a letter.
          </p>
        </div>
      </div>
    </div>
  );
}
