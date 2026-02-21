// db/helpers.ts - Query helpers and transaction management

import { type SQLQueryBindings } from "bun:sqlite";
import { SkyREPLError, calculateBackoff } from "@skyrepl/contracts";
import { getDatabase } from "./init";

export { getDatabase };

export function queryOne<T>(sql: string, params: unknown[] = []): T | null {
  const db = getDatabase();
  const row = db.prepare(sql).get(...(params as SQLQueryBindings[])) as T | undefined;
  return row ?? null;
}

export function queryMany<T>(sql: string, params: unknown[] = []): T[] {
  const db = getDatabase();
  return db.prepare(sql).all(...(params as SQLQueryBindings[])) as T[];
}

export function execute(sql: string, params: unknown[] = []): void {
  const db = getDatabase();
  db.prepare(sql).run(...(params as SQLQueryBindings[]));
}

export function transaction<T>(fn: () => T, retries = 3): T {
  const db = getDatabase();

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return db.transaction(fn)();
    } catch (e) {
      const error = e as { code?: string; message: string };

      if (error.code === "SQLITE_BUSY" && attempt < retries) {
        const delay = calculateBackoff(attempt);
        Bun.sleepSync(delay);
        continue;
      }

      throw new SkyREPLError("DATABASE_ERROR", error.message, "internal", { cause: e });
    }
  }

  // Unreachable, but TypeScript needs this
  throw new SkyREPLError("DATABASE_ERROR", "Transaction retry exhausted", "internal");
}
