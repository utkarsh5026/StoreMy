// Thin fetch wrappers. Errors are normalised into ApiError so callers
// can distinguish "your SQL was bad" (parse/bind) from "server is down".
import type {
  ApiError,
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
      // Non-JSON response — surface as transport error.
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

export async function runQuery(sql: string): Promise<QueryResult> {
  const res = await fetch("/api/query", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  return jsonOrThrow<QueryResult>(res);
}

export async function listTables(): Promise<TableSummary[]> {
  const res = await fetch("/api/tables");
  return jsonOrThrow<TableSummary[]>(res);
}

export async function describeTable(name: string): Promise<TableInfo> {
  const res = await fetch(`/api/tables/${encodeURIComponent(name)}`);
  return jsonOrThrow<TableInfo>(res);
}

export async function fetchHeap(name: string): Promise<HeapDump> {
  const res = await fetch(`/api/heap/${encodeURIComponent(name)}`);
  return jsonOrThrow<HeapDump>(res);
}
