import type { TableSummary } from "../types/api";

interface Props {
  tables: TableSummary[];
  selected: string | null;
  onSelect: (name: string) => void;
}

export function TableList({ tables, selected, onSelect }: Props) {
  return (
    <div className="sidebar">
      <h3>Tables</h3>
      {tables.length === 0 ? (
        <div className="empty">no tables yet</div>
      ) : (
        <ul>
          {tables.map((t) => (
            <li
              key={t.name}
              className={selected === t.name ? "selected" : ""}
              onClick={() => onSelect(t.name)}
            >
              <span>{t.name}</span>
              <span className="col-count">{t.column_count} cols</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
