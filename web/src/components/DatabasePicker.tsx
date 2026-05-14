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
    <div className="db-picker">
      <div className="db-picker-header">
        <span className="db-picker-logo">StoreMy</span>
        <span className="db-picker-sub">Select a database to continue</span>
      </div>

      <div className="db-picker-body">
        <div className="db-list-section">
          {load.status === "loading" && (
            <div className="db-empty">Loading…</div>
          )}
          {load.status === "err" && (
            <div className="error-box">{load.message}</div>
          )}
          {load.status === "ok" && load.databases.length === 0 && (
            <div className="db-empty">No databases yet. Create one below.</div>
          )}
          {load.status === "ok" && load.databases.length > 0 && (
            <ul className="db-list">
              {load.databases.map((db) => (
                <li key={db.name} onClick={() => onSelect(db.name)}>
                  <span className="db-icon">⬡</span>
                  <span className="db-name">{db.name}</span>
                  <span className="db-arrow">→</span>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="db-create-section">
          <div className="db-create-label">New database</div>
          <div className="db-create-row">
            <input
              ref={inputRef}
              className="db-name-input"
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
              onClick={handleCreate}
              disabled={!newName.trim() || create.status === "creating"}
            >
              {create.status === "creating" ? "Creating…" : "Create"}
            </button>
          </div>
          {create.status === "err" && (
            <div className="db-create-error">{create.message}</div>
          )}
          <div className="db-name-hint">
            Letters, digits, and underscores only. Must start with a letter.
          </div>
        </div>
      </div>
    </div>
  );
}
