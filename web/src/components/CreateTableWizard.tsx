import { useCallback, useState } from "react";

type SqlType = "INT" | "FLOAT" | "BOOLEAN" | "VARCHAR" | "TEXT";

interface ColumnDef {
  id: string;
  name: string;
  type: SqlType;
  varcharLen: string;
  notNull: boolean;
  unique: boolean;
  primaryKey: boolean;
  defaultValue: string;
}

interface Props {
  onClose: () => void;
  onCreate: (sql: string) => void;
}

let _nextId = 0;
function newId() {
  return String(++_nextId);
}

function makeColumn(): ColumnDef {
  return {
    id: newId(),
    name: "",
    type: "INT",
    varcharLen: "255",
    notNull: false,
    unique: false,
    primaryKey: false,
    defaultValue: "",
  };
}

function renderType(col: ColumnDef): string {
  if (col.type === "VARCHAR") {
    return `VARCHAR(${col.varcharLen.trim() || "255"})`;
  }
  return col.type;
}


function renderDefault(col: ColumnDef): string {
  const v = col.defaultValue.trim();
  if (!v) return "";
  if (col.type === "VARCHAR" && !v.startsWith("'")) return ` DEFAULT '${v}'`;
  return ` DEFAULT ${v}`;
}

function buildSql(tableName: string, columns: ColumnDef[]): string {
  if (!tableName.trim() || columns.length === 0) return "";
  const cols = columns
    .filter((c) => c.name.trim())
    .map((c) => {
      const parts: string[] = [`  ${c.name.trim()} ${renderType(c)}`];
      if (c.primaryKey) parts.push("PRIMARY KEY");
      if (c.notNull && !c.primaryKey) parts.push("NOT NULL");
      if (c.unique) parts.push("UNIQUE");
      const def = renderDefault(c);
      if (def) parts.push(def.trim());
      return parts.join(" ");
    });
  if (cols.length === 0) return "";
  return `CREATE TABLE ${tableName.trim()} (\n${cols.join(",\n")}\n);`;
}

// ── Component ────────────────────────────────────────────────────────────────

export function CreateTableWizard({ onClose, onCreate }: Props) {
  const [tableName, setTableName] = useState("");
  const [columns, setColumns] = useState<ColumnDef[]>([makeColumn()]);

  const sql = buildSql(tableName, columns);
  const canCreate = sql.length > 0;

  const updateCol = useCallback(
    <K extends keyof ColumnDef>(id: string, key: K, value: ColumnDef[K]) => {
      setColumns((prev) =>
        prev.map((c) => {
          if (c.id !== id) {
            if (key === "primaryKey" && value === true) {
              return { ...c, primaryKey: false };
            }
            return c;
          }
          return { ...c, [key]: value };
        }),
      );
    },
    [],
  );

  const addColumn = () => setColumns((prev) => [...prev, makeColumn()]);
  const removeColumn = (id: string) =>
    setColumns((prev) => prev.filter((c) => c.id !== id));

  const onBackdrop = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  return (
    <div
      className="fixed inset-0 bg-black/60 flex items-center justify-center z-50"
      onClick={onBackdrop}
    >
      <div className="bg-panel border border-line rounded-lg w-[min(900px,96vw)] max-h-[88vh] flex flex-col overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-4.5 py-3.5 border-b border-line font-semibold text-[15px]">
          <span>Create Table</span>
          <button
            className="btn-ghost border-transparent text-dim hover:text-fg hover:border-line px-2 py-0.5 text-[13px]"
            onClick={onClose}
          >
            ✕
          </button>
        </div>

        {/* Table name row */}
        <div className="flex items-center gap-3 px-4.5 py-3 border-b border-line bg-panel2">
          <label className="text-[11px] uppercase tracking-[0.08em] text-dim whitespace-nowrap">
            Table name
          </label>
          <input
            className="input-field max-w-[320px] text-[14px]"
            placeholder="e.g. users"
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            autoFocus
            spellCheck={false}
          />
        </div>

        {/* Body: column builder + SQL preview */}
        <div
          className="flex-1 min-h-0 overflow-hidden grid"
          style={{ gridTemplateColumns: "1fr 300px" }}
        >
          {/* Left: column list */}
          <div className="flex flex-col border-r border-line overflow-hidden">
            <div className="text-[11px] uppercase tracking-[0.08em] text-dim px-3.5 py-2 border-b border-line bg-panel2">
              Columns
            </div>
            <div className="flex-1 overflow-y-auto flex flex-col gap-0.5 p-2">
              {columns.map((col, idx) => (
                <ColumnRow
                  key={col.id}
                  col={col}
                  index={idx}
                  onChange={updateCol}
                  onRemove={columns.length > 1 ? removeColumn : undefined}
                />
              ))}
            </div>
            <div className="px-2.5 py-2 border-t border-line">
              <button
                className="btn-ghost text-xs py-1.5 px-3"
                onClick={addColumn}
              >
                + Add column
              </button>
            </div>
          </div>

          {/* Right: SQL preview */}
          <div className="flex flex-col bg-bg overflow-hidden">
            <div className="text-[11px] uppercase tracking-[0.08em] text-dim px-3.5 py-2 border-b border-line bg-panel2">
              SQL preview
            </div>
            <pre className="flex-1 m-0 p-3 font-mono text-xs leading-relaxed text-fg overflow-auto whitespace-pre-wrap">
              {sql || (
                <span className="text-dim italic">
                  fill in a table name and columns…
                </span>
              )}
            </pre>
          </div>
        </div>

        {/* Footer */}
        <div className="flex justify-end gap-2.5 px-4.5 py-3 border-t border-line bg-panel2">
          <button className="btn-ghost" onClick={onClose}>
            Cancel
          </button>
          <button
            className="btn"
            disabled={!canCreate}
            onClick={() => canCreate && onCreate(sql)}
          >
            Create Table
          </button>
        </div>
      </div>
    </div>
  );
}

interface ColumnRowProps {
  col: ColumnDef;
  index: number;
  onChange: <K extends keyof ColumnDef>(
    id: string,
    key: K,
    value: ColumnDef[K],
  ) => void;
  onRemove?: (id: string) => void;
}

const SQL_TYPES: SqlType[] = ["INT", "FLOAT", "BOOLEAN", "VARCHAR", "TEXT"];

const selectCls =
  "bg-bg border border-line rounded-[3px] text-fg font-mono text-[13px] px-1.5 py-1 outline-none cursor-pointer focus:border-accent";

function ColumnRow({ col, index, onChange, onRemove }: ColumnRowProps) {
  return (
    <div className="flex items-center gap-1.5 px-1.5 py-1.5 rounded bg-panel2 hover:bg-[#1f2936]">
      {/* Index */}
      <span className="text-[11px] text-dim font-mono min-w-4.5 text-right shrink-0">
        {index + 1}
      </span>

      {/* Name */}
      <input
        className="input-field w-32.5 shrink-0"
        placeholder="column name"
        value={col.name}
        onChange={(e) => onChange(col.id, "name", e.target.value)}
        spellCheck={false}
      />

      {/* Type */}
      <select
        className={selectCls}
        value={col.type}
        onChange={(e) => onChange(col.id, "type", e.target.value as SqlType)}
      >
        {SQL_TYPES.map((t) => (
          <option key={t} value={t}>
            {t}
          </option>
        ))}
      </select>

      {/* VARCHAR length (only shown for VARCHAR) */}
      {col.type === "VARCHAR" && (
        <input
          className="input-field w-13 shrink-0"
          placeholder="len"
          value={col.varcharLen}
          onChange={(e) => onChange(col.id, "varcharLen", e.target.value)}
          title="max length"
        />
      )}

      {/* Constraints */}
      <CheckLabel title="NOT NULL" label="NN">
        <input
          type="checkbox"
          className="accent-primary w-3.5 h-3.5 cursor-pointer"
          checked={col.notNull}
          onChange={(e) => onChange(col.id, "notNull", e.target.checked)}
        />
      </CheckLabel>
      <CheckLabel title="UNIQUE" label="UQ">
        <input
          type="checkbox"
          className="accent-primary w-3.5 h-3.5 cursor-pointer"
          checked={col.unique}
          onChange={(e) => onChange(col.id, "unique", e.target.checked)}
        />
      </CheckLabel>
      <CheckLabel title="PRIMARY KEY" label="PK">
        <input
          type="checkbox"
          className="accent-primary w-3.5 h-3.5 cursor-pointer"
          checked={col.primaryKey}
          onChange={(e) => onChange(col.id, "primaryKey", e.target.checked)}
        />
      </CheckLabel>

      {/* Default value */}
      {col.type === "BOOLEAN" ? (
        <select
          className={`${selectCls} w-25 shrink-0`}
          value={col.defaultValue}
          onChange={(e) => onChange(col.id, "defaultValue", e.target.value)}
          title="default value"
        >
          <option value="">— default —</option>
          <option value="TRUE">TRUE</option>
          <option value="FALSE">FALSE</option>
        </select>
      ) : (
        <input
          className="input-field w-22.5 shrink-0"
          placeholder="default"
          value={col.defaultValue}
          onChange={(e) => onChange(col.id, "defaultValue", e.target.value)}
          title="default value"
        />
      )}

      {/* Remove */}
      {onRemove && (
        <button
          className="btn-ghost border-transparent text-dim hover:text-danger hover:border-danger ml-auto px-1.5 py-0.5 text-[11px] shrink-0"
          onClick={() => onRemove(col.id)}
          title="remove column"
        >
          ✕
        </button>
      )}
    </div>
  );
}

function CheckLabel({
  title,
  label,
  children,
}: {
  title: string;
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label
      className="flex flex-col items-center gap-0.5 cursor-pointer select-none shrink-0"
      title={title}
    >
      {children}
      <span className="text-[10px] text-dim tracking-[0.04em]">{label}</span>
    </label>
  );
}
