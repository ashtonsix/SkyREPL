import { readFileSync, writeFileSync, existsSync, mkdirSync, chmodSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';
import { idToSlug } from '@skyrepl/contracts';
import type { ApiClient } from './client';
import { findProjectConfig } from './project';

// =============================================================================
// Paths
// =============================================================================

const HOME = homedir();
const REPL_DIR = join(HOME, '.repl');
const SSH_CONFIG_PATH = join(REPL_DIR, 'ssh_config');
const USER_SSH_CONFIG = join(HOME, '.ssh', 'config');
const INCLUDE_LINE = 'Include ~/.repl/ssh_config';

// =============================================================================
// SSH Accessibility Check
// =============================================================================

/**
 * Returns true if an allocation is SSH-accessible:
 * - ACTIVE allocations are always accessible.
 * - COMPLETE allocations are accessible only while debug_hold_until is in the future.
 */
export function isSSHAccessible(alloc: {
  status: string;
  debug_hold_until: number | null;
}): boolean {
  if (alloc.status === 'ACTIVE') return true;
  if (alloc.status === 'COMPLETE' && alloc.debug_hold_until !== null && alloc.debug_hold_until > Date.now()) {
    return true;
  }
  return false;
}

// =============================================================================
// Host Entry Generation
// =============================================================================

/**
 * Resolve the SSH Host slug for an allocation.
 * Priority: alias (from config) > display_name > base36 slug
 *
 * Warns on stderr if multiple alias keys map to the same allocation target.
 */
export function resolveSSHSlug(alloc: {
  id: number;
  display_name?: string | null;
}, aliases?: Record<string, string>): string {
  // Check alias map: keys are alias names, values are display_names or slugs
  if (aliases) {
    const slug = idToSlug(alloc.id);
    const displayName = alloc.display_name;
    const matchingAliases = Object.entries(aliases).filter(([, target]) =>
      target === slug || (displayName && target === displayName)
    );
    if (matchingAliases.length > 1) {
      const names = matchingAliases.map(([alias]) => alias).join(', ');
      process.stderr.write(
        `warning: multiple aliases map to the same allocation (id=${alloc.id}): ${names} — using first match\n`
      );
    }
    if (matchingAliases.length > 0) {
      return matchingAliases[0]![0];
    }
  }
  // Prefer display_name, fall back to base36 slug
  if (alloc.display_name) {
    return alloc.display_name;
  }
  return idToSlug(alloc.id);
}

/**
 * Generate a single SSH Host block for one allocation.
 * Host slug: display_name if available, otherwise base36 allocation ID slug.
 */
function generateHostEntry(alloc: {
  id: number;
  instance_ip: string | null;
  user: string;
  workdir: string;
  display_name?: string | null;
}, aliases?: Record<string, string>): string {
  const slug = resolveSSHSlug(alloc, aliases);
  const ip = alloc.instance_ip ?? '0.0.0.0';
  const lines = [
    `Host repl-${slug}`,
    `  HostName ${ip}`,
    `  User ${alloc.user || 'root'}`,
    `  Port 22`,
    `  StrictHostKeyChecking no`,
    `  UserKnownHostsFile /dev/null`,
    `  LogLevel ERROR`,
    `  RequestTTY yes`,
    `  ProxyCommand ~/.repl/bin/ssh_proxy.sh %h`,
  ];
  if (alloc.workdir) {
    lines.push(`  RemoteCommand cd '${(alloc.workdir || '~').replace(/'/g, "'\\''")}' && exec $SHELL -l`);
  }
  return lines.join('\n');
}

/** The catch-all entry that must appear LAST in the file. */
const CATCH_ALL_ENTRY = `Host repl-*
  ProxyCommand ~/.repl/bin/ssh_fallback.sh %n`;

// =============================================================================
// Public API
// =============================================================================

/**
 * Query the control plane for all SSH-accessible allocations and write
 * ~/.repl/ssh_config with per-allocation Host entries.
 *
 * Returns the count of per-allocation entries written (excludes catch-all).
 */
export async function regenerateSSHConfig(client: ApiClient): Promise<number> {
  const { data: allocations } = await client.listAllocations();

  // Load SSH aliases from repl.toml if available
  const project = findProjectConfig();
  const aliases: Record<string, string> = {};
  if (project) {
    for (const profile of Object.values(project.profiles)) {
      if (profile.ssh_aliases) {
        Object.assign(aliases, profile.ssh_aliases);
      }
    }
  }
  const hasAliases = Object.keys(aliases).length > 0;

  // Filter to SSH-accessible allocations that have an IP
  const sshAllocations = allocations.filter((a: any) =>
    isSSHAccessible(a) && a.instance_ip
  );

  // Build config content: per-allocation entries first, catch-all last
  // Deduplicate slugs: two allocations resolving to the same slug → skip with warning
  const emittedSlugs = new Set<string>();
  const hostEntries: string[] = [];
  for (const a of sshAllocations) {
    const allocWithName = { ...a, display_name: a.instance_display_name ?? a.display_name };
    const slug = resolveSSHSlug(allocWithName, hasAliases ? aliases : undefined);
    if (emittedSlugs.has(slug)) {
      process.stderr.write(
        `warning: duplicate SSH slug 'repl-${slug}' for allocation id=${a.id} — skipping (first match wins)\n`
      );
      continue;
    }
    emittedSlugs.add(slug);
    hostEntries.push(generateHostEntry(allocWithName, hasAliases ? aliases : undefined));
  }

  const header = [
    '# SkyREPL SSH Config',
    `# Generated: ${new Date().toISOString()}`,
    '# Do not edit manually — regenerated by `repl ssh-config`',
    '',
  ].join('\n');

  const sections = [header, ...hostEntries, CATCH_ALL_ENTRY];
  const configContent = sections.join('\n\n') + '\n';

  // Ensure ~/.repl exists
  mkdirSync(REPL_DIR, { recursive: true });

  writeFileSync(SSH_CONFIG_PATH, configContent, { encoding: 'utf-8' });
  // chmod 600: contains IPs; no keys but still sensitive
  chmodSync(SSH_CONFIG_PATH, 0o600);

  return hostEntries.length;
}

/**
 * Ensure ~/.ssh/config contains `Include ~/.repl/ssh_config` at the top.
 * SSH reads config top-down; Include must precede any Host entries it affects.
 *
 * Returns true if the file was modified, false if Include was already present.
 */
export async function ensureSSHConfigInclude(): Promise<boolean> {
  // Ensure ~/.ssh directory exists
  const sshDir = join(HOME, '.ssh');
  mkdirSync(sshDir, { recursive: true });

  let existing = '';
  if (existsSync(USER_SSH_CONFIG)) {
    existing = readFileSync(USER_SSH_CONFIG, 'utf-8');
  }

  // Check for Include line (any whitespace variant)
  const alreadyIncluded = existing.split('\n').some(
    (line) => line.trim() === INCLUDE_LINE
  );

  if (alreadyIncluded) {
    return false;
  }

  // Prepend Include line. SSH requires Include at the top to affect
  // subsequent Host blocks in the same file.
  const newContent = INCLUDE_LINE + '\n' + (existing ? existing : '');
  writeFileSync(USER_SSH_CONFIG, newContent, { encoding: 'utf-8' });

  return true;
}
