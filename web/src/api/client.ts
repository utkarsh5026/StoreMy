// Thin fetch wrappers. Errors are normalized into ApiError so callers
// can distinguish "your SQL was bad" (parse/bind) from "server is down".
import type {
  ApiError,
  DatabaseSummary,
  HeapDump,
  QueryResult,
  TableInfo,
  TableSummary,
} from "../types/api";

export class StoremyError extends Error {
  readonly kind: ApiError["kind"];
  constructor(err: ApiError) {
    super(err.message);
    this.kind = err.kind;
    this.name = "StoremyError";
  }
}

async function jsonOrThrow<T>(res: Response): Promise<T> {
  const text = await res.text();
  let body: unknown = null;
  if (text.length > 0) {
    try {
      body = JSON.parse(text);
    } catch {
      throw new StoremyError({
        kind: "internal",
        message: `non-JSON response (${res.status}): ${text.slice(0, 200)}`,
      });
    }
  }
  if (!res.ok) {
    if (
      body &&
      typeof body === "object" &&
      "kind" in body &&
      "message" in body
    ) {
      throw new StoremyError(body as ApiError);
    }
    throw new StoremyError({
      kind: "internal",
      message: `request failed (${res.status})`,
    });
  }
  return body as T;
}

export async function listDatabases(): Promise<DatabaseSummary[]> {
  const res = await fetch("/api/databases");
  return jsonOrThrow<DatabaseSummary[]>(res);
}

export async function createDatabase(name: string): Promise<DatabaseSummary> {
  const res = await fetch("/api/databases", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name }),
  });
  return jsonOrThrow<DatabaseSummary>(res);
}

export async function runQuery(db: string, sql: string): Promise<QueryResult> {
  const res = await fetch(`/api/${encodeURIComponent(db)}/query`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  return jsonOrThrow<QueryResult>(res);
}

export interface ApplyTemplateProgress {
  current: number;
  total: number;
  statement: string;
}

export async function applyTemplate(
  db: string,
  statements: string[],
  onProgress?: (progress: ApplyTemplateProgress) => void,
): Promise<void> {
  for (const [index, statement] of statements.entries()) {
    onProgress?.({
      current: index + 1,
      total: statements.length,
      statement,
    });
    await runQuery(db, statement);
  }
}

export async function listTables(db: string): Promise<TableSummary[]> {
  const res = await fetch(`/api/${encodeURIComponent(db)}/tables`);
  return jsonOrThrow<TableSummary[]>(res);
}

export async function describeTable(
  db: string,
  name: string,
): Promise<TableInfo> {
  const res = await fetch(
    `/api/${encodeURIComponent(db)}/tables/${encodeURIComponent(name)}`,
  );
  return jsonOrThrow<TableInfo>(res);
}

export async function fetchHeap(db: string, name: string): Promise<HeapDump> {
  const res = await fetch(
    `/api/${encodeURIComponent(db)}/heap/${encodeURIComponent(name)}`,
  );
  return jsonOrThrow<HeapDump>(res);
}
