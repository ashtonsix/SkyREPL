import { existsSync, readFileSync } from 'fs';
import { join, dirname } from 'path';

/**
 * A single profile from repl.toml.
 * Fields map 1:1 to TOML keys (snake_case).
 */
export interface Profile {
  command?: string;
  provider?: string;
  region?: string;
  spec?: string;
  instance_type?: string;
  workdir?: string;
  max_duration?: string;
  hold_duration?: string;
  env?: Record<string, string>;
}

export interface ProjectConfig {
  /** Absolute path to the repl.toml file */
  path: string;
  /** All profiles keyed by name */
  profiles: Record<string, Profile>;
}

/**
 * Walk up from `startDir` looking for repl.toml.
 * Returns null if none found (stops at filesystem root).
 */
export function findProjectConfig(startDir: string = process.cwd()): ProjectConfig | null {
  let dir = startDir;
  while (true) {
    const candidate = join(dir, 'repl.toml');
    if (existsSync(candidate)) {
      return loadProjectConfig(candidate);
    }
    const parent = dirname(dir);
    if (parent === dir) break; // reached root
    dir = parent;
  }
  return null;
}

/**
 * Parse a repl.toml file into a ProjectConfig.
 *
 * Each top-level table is a profile. Nested `.env` tables become the
 * profile's env record. Unknown keys are passed through (future-proofing).
 */
function loadProjectConfig(filePath: string): ProjectConfig {
  const raw = readFileSync(filePath, 'utf-8');
  const parsed = Bun.TOML.parse(raw) as Record<string, unknown>;

  const profiles: Record<string, Profile> = {};
  for (const [name, value] of Object.entries(parsed)) {
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      profiles[name] = value as Profile;
    }
  }

  return { path: filePath, profiles };
}

/**
 * Parse a human-friendly duration string into milliseconds.
 * Supports: "10m", "1h", "30s", "1h30m", "6h", "500ms"
 * Falls back to parseInt for bare numbers (treated as ms).
 */
export function parseDuration(s: string): number {
  let total = 0;
  let remaining = s.trim();

  const pattern = /(\d+)\s*(ms|s|m|h)/g;
  let match;
  let matched = false;
  while ((match = pattern.exec(remaining)) !== null) {
    matched = true;
    const value = parseInt(match[1], 10);
    switch (match[2]) {
      case 'ms': total += value; break;
      case 's': total += value * 1000; break;
      case 'm': total += value * 60_000; break;
      case 'h': total += value * 3_600_000; break;
    }
  }

  if (!matched) {
    const n = parseInt(remaining, 10);
    if (!isNaN(n)) return n;
    throw new Error(`Invalid duration: "${s}"`);
  }

  return total;
}
