import { homedir } from 'os';
import { join } from 'path';

/**
 * Get control plane URL from environment or config file.
 *
 * Resolution order:
 * 1. REPL_CONTROL_PLANE_URL environment variable
 * 2. ~/.repl/config file (if exists)
 * 3. Default: http://localhost:3000
 */
export function getControlPlaneUrl(): string {
  // Check environment variable first
  if (process.env.REPL_CONTROL_PLANE_URL) {
    return process.env.REPL_CONTROL_PLANE_URL;
  }

  // TODO: Read from ~/.repl/config if it exists
  // For now, just return default

  return 'http://localhost:3000';
}
