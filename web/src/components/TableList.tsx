import type { TableSummary } from "../types/api";

interface Props {
  tables: TableSummary[];
  selected: string | null;
  onSelect: (name: string) => void;
  onNewTable: () => void;
}

export function TableList({ tables, selected, onSelect, onNewTable }: Props) {
  return (
    <div className="[grid-area:sidebar] border-r border-line bg-panel overflow-y-auto py-3 flex flex-col">
      <p className="text-[11px] uppercase text-dim tracking-[0.08em] mx-3 mb-1.5">
        Tables
      </p>

      <button
        className="btn-ghost mx-3 mb-2 text-xs py-1.5 justify-start"
        onClick={onNewTable}
      >
        + New Table
      </button>

      {tables.length === 0 ? (
        <p className="px-3 py-2 text-dim italic text-xs">no tables yet</p>
      ) : (
        <ul className="list-none m-0 p-0">
          {tables.map((t) => (
            <li
              key={t.name}
              onClick={() => onSelect(t.name)}
              className={[
                "flex justify-between items-center px-3 py-1.5 cursor-pointer font-mono text-[13px]",
                selected === t.name
                  ? "bg-panel2 border-l-2 border-accent pl-2.5"
                  : "hover:bg-panel2",
              ].join(" ")}
            >
              <span>{t.name}</span>
              <span className="text-dim text-[11px]">
                {t.column_count} cols
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
