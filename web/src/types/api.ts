// Hand-written mirrors of the DTOs in db/src/web/dto.rs.
// Keep in sync: when an enum gains a variant on the Rust side, add it here too.

export type ApiErrorKind =
  | "parse"
  | "unsupported"
  | "table_not_found"
  | "table_exists"
  | "column_not_found"
  | "duplicate_column"
  | "wrong_column_count"
  | "null_violation"
  | "type_error"
  | "bind"
  | "engine"
  | "worker_gone"
  | "database_not_found"
  | "database_exists"
  | "invalid_database_name"
  | "internal";

export interface ApiError {
  kind: ApiErrorKind;
  message: string;
}

export interface Column {
  name: string;
  /** SQL type name as rendered server-side, e.g. "INT", "VARCHAR". */
  type: string;
  nullable: boolean;
}

/** Cell type — large i64/u64 values arrive as strings to avoid JS number truncation. */
export type Cell = string | number | boolean | null;

export interface DatabaseSummary {
  name: string;
  tables: string[];
}

export interface TableSummary {
  name: string;
  column_count: number;
  file_id: number;
}

export interface TableInfo {
  name: string;
  file_id: number;
  file_path: string;
  primary_key: string[] | null;
  columns: Column[];
}

export interface QueryRows {
  table: string;
  columns: Column[];
  rows: Cell[][];
}

export interface ShownIndex {
  name: string;
  table: string;
  columns: string[];
  kind: string;
}

export type QueryResult =
  | { kind: "table_created"; name: string; file_id: number; already_exists: boolean }
  | { kind: "table_dropped"; name: string }
  | { kind: "index_created"; name: string; table: string; already_exists: boolean }
  | { kind: "index_dropped"; name: string }
  | { kind: "indexes_shown"; scope: string | null; rows: ShownIndex[] }
  | { kind: "inserted"; table: string; rows: number }
  | { kind: "deleted"; table: string; rows: number }
  | { kind: "updated"; table: string; rows: number }
  | ({ kind: "selected" } & QueryRows);

// ---------- heap dump (GET /api/heap/{name}) ----------

export interface PageHeader {
  num_slots: number;
  tuple_start: number;
  checksum: number;
  /** First 8 bytes of the page in hex. */
  raw_hex: string;
}

export interface DecodedField {
  index: number;
  name: string;
  /** SQL type name (e.g. "INT", "VARCHAR") or "NULL". */
  type: string;
  value: Cell;
}

export type SlotDecode =
  | { ok: "true"; null_bitmap_hex: string; fields: DecodedField[] }
  | {
      ok: "false";
      error: string;
      null_bitmap_hex: string | null;
      fields_so_far: DecodedField[];
    };

export type HeapSlot =
  | {
      status: "live";
      slot_id: number;
      offset: number;
      length: number;
      raw_hex: string;
      decode: SlotDecode;
    }
  | { status: "tombstone"; slot_id: number; offset: number; length: number }
  | {
      status: "out_of_range";
      slot_id: number;
      offset: number;
      length: number;
      error: string;
    };

export interface HeapPage {
  page_no: number;
  header: PageHeader;
  slot_array_end: number;
  free_bytes: number;
  used_bytes: number;
  slots: HeapSlot[];
  page_error: string | null;
  blank: boolean;
}

export interface HeapDump {
  schema_version: number;
  page_size: number;
  page_count: number;
  field_count: number;
  field_names: string[];
  table: string;
  pages: HeapPage[];
}
