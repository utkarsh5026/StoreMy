import { useCallback, useEffect, useRef, useState } from "react";
import {
  applyTemplate,
  createDatabase,
  listDatabases,
  StoremyError,
} from "../api/client";
import {
  DATABASE_TEMPLATES,
  type DatabaseTemplate,
} from "../templates/databases";
import type { DatabaseSummary } from "../types/api";

export interface DatabaseSelection {
  name: string;
  initialSql?: string;
}

interface Props {
  onSelect: (selection: DatabaseSelection) => void;
}

type LoadState =
  | { status: "loading" }
  | { status: "ok"; databases: DatabaseSummary[] }
  | { status: "err"; message: string };

type CreateState =
  | { status: "idle" }
  | { status: "creating" }
  | { status: "err"; message: string };

type TemplateState =
  | { status: "idle" }
  | {
      status: "applying";
      templateId: string;
      databaseName: string;
      message: string;
      current?: number;
      total?: number;
    }
  | { status: "err"; templateId: string; databaseName: string; message: string };

export function DatabasePicker({ onSelect }: Props) {
  const [load, setLoad] = useState<LoadState>({ status: "loading" });
  const [create, setCreate] = useState<CreateState>({ status: "idle" });
  const [templateState, setTemplateState] = useState<TemplateState>({
    status: "idle",
  });
  const [newName, setNewName] = useState("");
  const [showTemplates, setShowTemplates] = useState(false);
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

  const handleTemplateCreate = useCallback(
    async (template: DatabaseTemplate) => {
      const databaseName = nextAvailableDatabaseName(
        template.defaultDatabaseName,
        load.status === "ok" ? load.databases : [],
      );

      setTemplateState({
        status: "applying",
        templateId: template.id,
        databaseName,
        message: "Creating database...",
      });

      try {
        await createDatabase(databaseName);
        await applyTemplate(databaseName, template.statements, (progress) => {
          setTemplateState({
            status: "applying",
            templateId: template.id,
            databaseName,
            message: "Creating tables and adding starter rows...",
            current: progress.current,
            total: progress.total,
          });
        });
        await refresh();
        setTemplateState({ status: "idle" });
        onSelect({ name: databaseName, initialSql: template.starterQuery });
      } catch (e) {
        const msg = e instanceof StoremyError ? e.message : String(e);
        setTemplateState({
          status: "err",
          templateId: template.id,
          databaseName,
          message: msg,
        });
        await refresh();
      }
    },
    [load, onSelect, refresh],
  );

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") handleCreate();
  };

  const isApplyingTemplate = templateState.status === "applying";

  return (
    <div className="flex flex-col items-center justify-center min-h-full bg-bg px-4 py-10">
      {/* Logo / heading */}
      <div className="text-center mb-9">
        <span className="block text-[28px] font-bold tracking-[0.06em] text-fg mb-1.5">
          StoreMy
        </span>
        <span className="text-dim text-sm">Select a database to continue</span>
      </div>

      <div className="w-full max-w-2xl flex flex-col gap-6">
        {/* Database list — primary focus */}
        <div className="flex flex-col gap-2">
          <p className="text-[11px] uppercase tracking-[0.08em] text-dim">
            Your databases
          </p>
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
                No databases yet — create one below or start from a template.
              </p>
            )}
            {load.status === "ok" && load.databases.length > 0 && (
              <ul className="list-none m-0 p-0">
                {load.databases.map((db) => (
                  <li
                    key={db.name}
                    onClick={() => onSelect({ name: db.name })}
                    className="flex items-center gap-2.5 px-4 py-3 cursor-pointer border-b border-line last:border-b-0 hover:bg-panel2 transition-colors"
                  >
                    <span className="text-accent text-sm">⬡</span>
                    <div className="flex-1 min-w-0">
                      <span className="font-mono text-sm font-semibold">{db.name}</span>
                      {(db.tables ?? []).length > 0 && (
                        <p className="font-mono text-[11px] text-dim mt-0.5 truncate">
                          {(db.tables ?? []).join(" · ")}
                        </p>
                      )}
                    </div>
                    <span className="text-dim text-[13px]">→</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
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
              disabled={create.status === "creating" || isApplyingTemplate}
            />
            <button
              className="btn"
              onClick={handleCreate}
              disabled={
                !newName.trim() ||
                create.status === "creating" ||
                isApplyingTemplate
              }
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

        {/* Templates — collapsed by default */}
        <div className="flex flex-col gap-3">
          <button
            className="flex items-center gap-2 text-[11px] uppercase tracking-[0.08em] text-dim hover:text-fg transition-colors w-fit"
            onClick={() => setShowTemplates((v) => !v)}
          >
            <span
              className="inline-block transition-transform duration-150"
              style={{ transform: showTemplates ? "rotate(90deg)" : "rotate(0deg)" }}
            >
              ▶
            </span>
            Start from a template
          </button>

          {showTemplates && (
            <div className="flex flex-col gap-3">
              <p className="text-dim text-[13px]">
                Pick a ready-made schema, then add rows or query it right away.
              </p>
              <div className="grid gap-3 md:grid-cols-2">
                {DATABASE_TEMPLATES.map((template) => {
                  const isActive =
                    templateState.status === "applying" &&
                    templateState.templateId === template.id;
                  const failed =
                    templateState.status === "err" &&
                    templateState.templateId === template.id;

                  return (
                    <div
                      key={template.id}
                      className="flex min-h-45 flex-col gap-3 rounded-md border border-line bg-panel p-4"
                    >
                      <div className="flex-1">
                        <div className="mb-1 flex items-center justify-between gap-3">
                          <h2 className="text-base font-semibold text-fg">
                            {template.name}
                          </h2>
                          <span className="font-mono text-[11px] text-accent">
                            {template.defaultDatabaseName}
                          </span>
                        </div>
                        <p className="text-[13px] leading-5 text-dim">
                          {template.description}
                        </p>
                        <p className="mt-3 font-mono text-[11px] leading-5 text-dim">
                          {template.tables.join(" · ")}
                        </p>
                      </div>
                      <button
                        className="btn"
                        onClick={() => handleTemplateCreate(template)}
                        disabled={isApplyingTemplate || create.status === "creating"}
                      >
                        {isActive ? "Setting up..." : "Use template"}
                      </button>
                      {isActive && (
                        <p className="text-xs text-dim">
                          {templateState.message}
                          {templateState.current && templateState.total
                            ? ` (${templateState.current}/${templateState.total})`
                            : ""}
                        </p>
                      )}
                      {failed && (
                        <p className="font-mono text-xs text-danger">
                          {templateState.databaseName}: {templateState.message}
                        </p>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function nextAvailableDatabaseName(
  baseName: string,
  databases: DatabaseSummary[],
): string {
  const names = new Set(databases.map((db) => db.name));
  if (!names.has(baseName)) return baseName;

  for (let suffix = 2; ; suffix += 1) {
    const candidate = `${baseName}_${suffix}`;
    if (!names.has(candidate)) return candidate;
  }
}
