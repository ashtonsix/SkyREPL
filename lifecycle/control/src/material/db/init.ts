// db/init.ts - Connection management, migrations, WAL

import { Database } from "bun:sqlite";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { SkyREPLError } from "@skyrepl/contracts";

interface Migration {
  version: number;
  sql: string;
}

let connection: Database | null = null;

export function initDatabase(path: string): Database {
  connection = new Database(path);
  connection.exec("PRAGMA journal_mode = WAL");
  connection.exec("PRAGMA foreign_keys = ON");
  connection.exec("PRAGMA busy_timeout = 5000");
  return connection;
}

export function getDatabase(): Database {
  if (!connection) {
    throw new SkyREPLError("DATABASE_ERROR", "Database not initialized", "internal");
  }
  return connection;
}

export function closeDatabase(): void {
  if (connection) {
    connection.close();
    connection = null;
  }
}

function discoverMigrations(): Migration[] {
  const dir = join(import.meta.dir, "../../migrations");
  const files = readdirSync(dir)
    .filter(f => /^\d{3}_.*\.sql$/.test(f))
    .sort();
  return files.map(f => ({
    version: parseInt(f.slice(0, 3), 10),
    sql: readFileSync(join(dir, f), "utf-8"),
  }));
}

export function getMigrationVersion(): number {
  const db = getDatabase();
  try {
    const result = db.prepare("SELECT version FROM schema_version").get() as { version: number } | undefined;
    return result?.version ?? 0;
  } catch {
    return 0; // Table doesn't exist yet
  }
}

export function runMigrations(): void {
  const db = getDatabase();
  const current = getMigrationVersion();
  const migrations = discoverMigrations();

  const maxMigration = migrations.length > 0
    ? Math.max(...migrations.map(m => m.version))
    : 0;

  if (current > maxMigration) {
    throw new SkyREPLError(
      "MIGRATION_ERROR",
      `DB schema v${current} is newer than code v${maxMigration}`,
      "internal"
    );
  }

  for (const migration of migrations) {
    if (migration.version <= current) continue;
    db.exec(migration.sql);
  }
}

export function walCheckpoint(): void {
  const db = getDatabase();
  db.run("PRAGMA wal_checkpoint(TRUNCATE)");
}
