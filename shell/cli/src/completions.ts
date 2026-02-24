// completions.ts — Core completion infrastructure for shell autocomplete
//
// Contains:
//   - Completion types and central command registry (source of truth)
//   - --list-completions handler (reads daemon-pushed cache)
//   - Shell script generators (zsh, bash)

import { readFileSync, existsSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';
import { findProjectConfig } from './project';

// =============================================================================
// Types
// =============================================================================

export type DynamicSource = 'allocations' | 'runs' | 'instances' | 'profiles' | 'providers' | 'workflows';

// Flag value types:
//   null          = boolean flag (no value expected)
//   false         = valued flag with no specific completions
//   string[]      = static choices
//   DynamicSource = dynamic completions from cache
type FlagValue = DynamicSource | string[] | null | false;

export interface FlagDef {
  names: string[];       // e.g. ['-p', '--profile'] or ['--dry-run']
  value: FlagValue;
  desc: string;
}

export interface SubcommandSpec {
  name: string;
  desc?: string;
  flags?: FlagDef[];
  positional?: Array<{ source: DynamicSource | string[] }>;
}

export interface CompletionSpec {
  subcommands?: SubcommandSpec[];
  flags?: FlagDef[];
  positional?: Array<{ source: DynamicSource | string[] }>;
}

// =============================================================================
// Static values
// =============================================================================

const DURATION_VALUES = ['5m', '15m', '30m', '1h', '2h', '4h', '8h', '24h'];
const ROLE_VALUES = ['admin', 'member', 'viewer'];

const RUN_FLAGS: FlagDef[] = [
  { names: ['-p', '--profile'], value: 'profiles', desc: 'Select profile' },
  { names: ['-c', '--command'], value: false, desc: 'Command to run' },
  { names: ['--timeout'], value: DURATION_VALUES, desc: 'Max run duration' },
  { names: ['--hold'], value: DURATION_VALUES, desc: 'Debug hold duration' },
  { names: ['--workdir'], value: false, desc: 'Working directory' },
  { names: ['--provider'], value: 'providers', desc: 'Cloud provider' },
  { names: ['--spec'], value: false, desc: 'Instance spec (instance type or distro:version:arch)' },
  { names: ['--region'], value: false, desc: 'Cloud region' },
  { names: ['--dry-run'], value: null, desc: 'Preview without executing' },
  { names: ['--no-preflight'], value: null, desc: 'Skip preflight checks' },
];

// =============================================================================
// Command registry (source of truth for completions — no circular imports)
// =============================================================================

const commandDescriptions: Record<string, string> = {
  run: 'Launch a run',
  ssh: 'SSH into an allocation',
  extend: 'Extend debug hold',
  release: 'Release debug hold',
  control: 'Manage control plane',
  instance: 'Manage instances',
  allocation: 'Manage allocations',
  artifact: 'Manage artifacts',
  team: 'Manage team',
  auth: 'Manage API keys',
  setup: 'First-run setup',
  'ssh-config': 'Regenerate SSH config',
  daemon: 'Manage local daemon',
  completion: 'Shell completion scripts',
  help: 'Show help',
};

const commandSpecs: Record<string, CompletionSpec> = {
  run: {
    subcommands: [
      { name: 'launch', desc: 'Launch a run', flags: RUN_FLAGS },
      { name: 'list', desc: 'List runs' },
      { name: 'cancel', desc: 'Cancel a workflow', positional: [{ source: 'workflows' }] },
      { name: 'attach', desc: 'Attach to run output', positional: [{ source: 'runs' }] },
    ],
    // bare `repl run` acts like `repl run launch`
    flags: RUN_FLAGS,
  },
  ssh: {
    positional: [{ source: 'allocations' }],
  },
  extend: {
    positional: [{ source: 'allocations' }, { source: DURATION_VALUES }],
  },
  release: {
    positional: [{ source: 'allocations' }],
  },
  control: {
    subcommands: [
      { name: 'start', desc: 'Start control plane' },
      { name: 'stop', desc: 'Stop control plane' },
      { name: 'restart', desc: 'Restart control plane' },
      { name: 'status', desc: 'Show status' },
      { name: 'reset', desc: 'Reset control plane' },
    ],
  },
  instance: {
    subcommands: [
      { name: 'list', desc: 'List instances' },
      { name: 'terminate', desc: 'Terminate an instance', positional: [{ source: 'instances' }] },
    ],
  },
  allocation: {
    subcommands: [{ name: 'list', desc: 'List allocations' }],
  },
  artifact: {
    subcommands: [
      { name: 'list', desc: 'List artifacts for a run', positional: [{ source: 'runs' }] },
      {
        name: 'download', desc: 'Download an artifact',
        flags: [
          { names: ['--output'], value: false, desc: 'Output directory' },
        ],
      },
    ],
  },
  team: {
    subcommands: [
      { name: 'create', desc: 'Create a team' },
      {
        name: 'invite', desc: 'Invite a user',
        flags: [
          { names: ['--role'], value: ROLE_VALUES, desc: 'User role' },
          { names: ['--budget'], value: false, desc: 'Budget cap' },
          { names: ['--name'], value: false, desc: 'Display name' },
        ],
      },
      { name: 'remove', desc: 'Remove a user' },
      { name: 'list', desc: 'List members' },
      { name: 'info', desc: 'Show team info' },
      {
        name: 'budget', desc: 'Show/set budget caps',
        flags: [
          { names: ['--team'], value: false, desc: 'Team budget cap' },
          { names: ['--user'], value: false, desc: 'User budget cap' },
        ],
      },
    ],
  },
  auth: {
    subcommands: [
      {
        name: 'create-key', desc: 'Create an API key',
        flags: [
          { names: ['--name'], value: false, desc: 'Key name' },
          { names: ['--role'], value: ROLE_VALUES, desc: 'User role' },
        ],
      },
      { name: 'revoke-key', desc: 'Revoke an API key' },
      { name: 'list-keys', desc: 'List API keys' },
    ],
  },
  setup: {
    subcommands: [{ name: 'tailscale', desc: 'Configure Tailscale' }],
  },
  daemon: {
    subcommands: [
      { name: 'start', desc: 'Start daemon' },
      { name: 'stop', desc: 'Stop daemon' },
      { name: 'status', desc: 'Show daemon status' },
    ],
  },
  completion: {
    subcommands: [
      { name: 'bash', desc: 'Bash completion script' },
      { name: 'zsh', desc: 'Zsh completion script' },
      { name: 'fish', desc: 'Fish completion script' },
      { name: 'install', desc: 'Install completions' },
    ],
  },
  'ssh-config': {},
  help: {},
};

// =============================================================================
// Cache types (mirrors daemon/completion-cache.ts CompletionCache)
// =============================================================================

interface CacheAllocation {
  slug: string;
  status: string;
  run_slug: string | null;
  command: string | null;
  provider: string | null;
  created_at: number;
  debug_hold_until: number | null;
}

interface CacheRun {
  slug: string;
  state: string;
  command: string;
  exit_code: number | null;
  created_at: number;
}

interface CacheInstance {
  slug: string;
  provider: string;
  spec: string;
  region: string;
  state: string;
  ip: string | null;
  created_at: number;
}

interface CompletionCache {
  version: number;
  updated_at: string;
  allocations: CacheAllocation[];
  runs: CacheRun[];
  instances: CacheInstance[];
  profiles: string[];
  providers: string[];
}

// =============================================================================
// Time formatting helper
// =============================================================================

export function timeAgo(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return 'just now';
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  return `${months}mo ago`;
}

// =============================================================================
// --list-completions handler
// =============================================================================

const CACHE_FILE = join(homedir(), '.repl', 'completion-cache.json');

export function listCompletions(source: string): void {
  let cache: CompletionCache = { version: 0, updated_at: '', allocations: [], runs: [], instances: [], profiles: [], providers: [] };
  try {
    if (existsSync(CACHE_FILE)) {
      cache = JSON.parse(readFileSync(CACHE_FILE, 'utf-8')) as CompletionCache;
    }
  } catch {
    // Corrupt cache — use empty defaults
  }

  const now = Date.now();

  switch (source) {
    case 'allocations': {
      const runBySlug = new Map<string, CacheRun>();
      for (const run of cache.runs ?? []) {
        runBySlug.set(run.slug, run);
      }
      for (const alloc of cache.allocations ?? []) {
        const run = alloc.run_slug ? runBySlug.get(alloc.run_slug) : null;
        const command = run?.command ?? '-';
        const cmd = command.length > 30 ? command.slice(0, 27) + '...' : command;
        const ago = timeAgo(now - (alloc.created_at ?? 0));
        const provider = alloc.provider ?? '-';
        const desc = `${alloc.status} | ${cmd} | ${provider} | ${ago}`;
        console.log(`${alloc.slug}\t${desc}`);
      }
      break;
    }

    case 'runs':
    case 'workflows': {
      for (const run of cache.runs ?? []) {
        const cmd = (run.command ?? '-').length > 30
          ? run.command.slice(0, 27) + '...'
          : (run.command ?? '-');
        const ago = timeAgo(now - (run.created_at ?? 0));
        const exit = run.exit_code !== null && run.exit_code !== undefined
          ? `exit:${run.exit_code}`
          : '-';
        const desc = `${run.state} | ${cmd} | ${exit} | ${ago}`;
        console.log(`${run.slug}\t${desc}`);
      }
      break;
    }

    case 'instances': {
      for (const inst of cache.instances ?? []) {
        const ago = timeAgo(now - (inst.created_at ?? 0));
        const ip = inst.ip ?? '-';
        const provSpec = `${inst.provider}/${inst.spec}`;
        const desc = `${inst.state} | ${provSpec} | ${ip} | ${ago}`;
        console.log(`${inst.slug}\t${desc}`);
      }
      break;
    }

    case 'profiles': {
      // Read directly from repl.toml (CWD-relative) — don't rely on daemon cache
      // because the daemon's CWD won't match the user's project directory
      try {
        const project = findProjectConfig();
        if (project) {
          for (const name of Object.keys(project.profiles)) {
            console.log(name);
          }
        }
      } catch {
        // repl.toml missing or unparseable — no completions
      }
      break;
    }

    case 'providers': {
      for (const provider of cache.providers ?? []) {
        console.log(provider);
      }
      break;
    }

    default:
      break;
  }

  process.exit(0);
}

// =============================================================================
// Shared generator helpers
// =============================================================================

/** Get all flag names from a FlagDef array. */
function allFlagNames(flags: FlagDef[]): string[] {
  return flags.flatMap(f => f.names);
}

/**
 * Build zsh _arguments specs for a set of flags.
 * Returns specs (for _arguments) and dynamics (for case $state).
 * Used by subcommand-level _arguments calls (not for mixed subcommand+flag commands).
 */
function buildZshFlagSpecs(flags: FlagDef[]): { specs: string[]; dynamics: Array<[string, DynamicSource]> } {
  const specs: string[] = [];
  const dynamics: Array<[string, DynamicSource]> = [];

  for (const flag of flags) {
    const primary = flag.names.find(n => n.startsWith('--')) || flag.names[0];
    const stateId = primary.replace(/^-+/, '');

    let valueSuffix = '';
    if (flag.value === null) {
      // boolean — no value part
    } else if (flag.value === false) {
      valueSuffix = `:${stateId}:`;
    } else if (Array.isArray(flag.value)) {
      valueSuffix = `:${stateId}:(${flag.value.join(' ')})`;
    } else {
      valueSuffix = `:${stateId}:->_${stateId}`;
      dynamics.push([stateId, flag.value as DynamicSource]);
    }

    if (flag.names.length > 1) {
      const short = flag.names.find(n => !n.startsWith('--'))!;
      const long = flag.names.find(n => n.startsWith('--'))!;
      const excl = `(${short} ${long})`;
      specs.push(`'${excl}${short}[${flag.desc}]${valueSuffix}'`);
      specs.push(`'${excl}${long}[${flag.desc}]${valueSuffix}'`);
    } else {
      specs.push(`'${flag.names[0]}[${flag.desc}]${valueSuffix}'`);
    }
  }

  return { specs, dynamics };
}

/**
 * Build zsh _arguments call + state handler for flags and/or positionals.
 * Returns lines of zsh code (without leading indent).
 */
function buildZshArgs(flags: FlagDef[] | undefined, positional: Array<{ source: DynamicSource | string[] }> | undefined): string[] {
  const lines: string[] = [];
  const argSpecs: string[] = [];
  const allDynamics: Array<[string, DynamicSource]> = [];

  if (flags && flags.length > 0) {
    const { specs, dynamics } = buildZshFlagSpecs(flags);
    argSpecs.push(...specs);
    allDynamics.push(...dynamics);
  }

  const posDynamics: Array<[number, DynamicSource]> = [];
  if (positional && positional.length > 0) {
    for (let i = 0; i < positional.length; i++) {
      const pos = positional[i];
      if (Array.isArray(pos.source)) {
        argSpecs.push(`':arg${i + 1}:(${pos.source.join(' ')})'`);
      } else {
        argSpecs.push(`':arg${i + 1}:->_pos${i}'`);
        posDynamics.push([i, pos.source as DynamicSource]);
      }
    }
  }

  if (argSpecs.length === 0) return lines;

  lines.push(`_arguments \\`);
  for (let i = 0; i < argSpecs.length; i++) {
    lines.push(`  ${argSpecs[i]}${i < argSpecs.length - 1 ? ' \\' : ''}`);
  }

  if (allDynamics.length > 0 || posDynamics.length > 0) {
    lines.push(`case $state in`);
    for (const [stateId, source] of allDynamics) {
      lines.push(`  _${stateId})`);
      lines.push(`    _repl_dynamic '${source}'`);
      lines.push(`    ;;`);
    }
    for (const [idx, source] of posDynamics) {
      lines.push(`  _pos${idx})`);
      lines.push(`    _repl_dynamic '${source}'`);
      lines.push(`    ;;`);
    }
    lines.push(`esac`);
  }

  return lines;
}

/**
 * Emit zsh case handler for completing flag values.
 * Checks $words[$((CURRENT-1))] and returns early if a match is found.
 * Used for mixed subcommand+flag commands (e.g., `run`).
 */
function emitZshFlagValueHandler(lines: string[], flags: FlagDef[]): void {
  const valued = flags.filter(f => f.value !== null);
  if (valued.length === 0) return;

  lines.push(`          case \$words[\$((CURRENT-1))] in`);

  for (const flag of valued.filter(f => typeof f.value === 'string')) {
    lines.push(`            ${flag.names.join('|')})`);
    lines.push(`              _repl_dynamic '${flag.value}'`);
    lines.push(`              return`);
    lines.push(`              ;;`);
  }

  const staticFlags = valued.filter(f => Array.isArray(f.value));
  const byValues = new Map<string, string[]>();
  for (const flag of staticFlags) {
    const key = (flag.value as string[]).join(' ');
    const names = byValues.get(key) || [];
    names.push(...flag.names);
    byValues.set(key, names);
  }
  for (const [values, names] of byValues) {
    lines.push(`            ${names.join('|')})`);
    lines.push(`              compadd -- ${values}`);
    lines.push(`              return`);
    lines.push(`              ;;`);
  }

  const noComplete = valued.filter(f => f.value === false);
  if (noComplete.length > 0) {
    lines.push(`            ${noComplete.flatMap(f => f.names).join('|')})`);
    lines.push(`              return`);
    lines.push(`              ;;`);
  }

  lines.push(`          esac`);
}

/**
 * Emit zsh loop that finds the first non-flag word (the subcommand)
 * by walking $words[2..CURRENT-1], skipping valued flags and their arguments.
 * Sets local variable $sub.
 */
function emitZshSubcommandFinder(lines: string[], flags: FlagDef[]): void {
  const valued = flags.filter(f => f.value !== null);
  const booleans = flags.filter(f => f.value === null);

  lines.push(`          local sub=""`);
  lines.push(`          local i`);
  lines.push(`          for (( i=2; i <= CURRENT; i++ )); do`);
  lines.push(`            [[ \$i -eq \$CURRENT ]] && break`);
  lines.push(`            case \$words[\$i] in`);

  if (valued.length > 0) {
    lines.push(`              ${valued.flatMap(f => f.names).join('|')})`);
    lines.push(`                (( i++ ))`);
    lines.push(`                ;;`);
  }

  if (booleans.length > 0) {
    lines.push(`              ${booleans.flatMap(f => f.names).join('|')})`);
    lines.push(`                ;;`);
  }

  lines.push(`              -*)`);
  lines.push(`                ;;`);
  lines.push(`              *)`);
  lines.push(`                sub=\$words[\$i]`);
  lines.push(`                break`);
  lines.push(`                ;;`);
  lines.push(`            esac`);
  lines.push(`          done`);
}

// =============================================================================
// Zsh generator
// =============================================================================

export function generateZshScript(cliPath: string): string {
  const escapedCliPath = cliPath.replace(/'/g, "'\\''");

  const commandsArray = Object.entries(commandDescriptions)
    .map(([name, desc]) => `    '${name}:${desc}'`)
    .join('\n');

  const argsCases = buildZshArgsCases(escapedCliPath);

  return `#compdef repl

_repl() {
  local -a commands
  commands=(
${commandsArray}
  )

  local -a global_flags
  global_flags=('--json[Output as JSON]' '--quiet[Minimal output]')

  _arguments -C \\
    '1:command:->cmd' \\
    '*::arg:->args' \\
    \$global_flags

  case \$state in
    cmd)
      _describe 'command' commands
      ;;
    args)
      local cmd=\$words[1]
      case \$cmd in
${argsCases}
      esac
      ;;
  esac
}

_repl_dynamic() {
  local source=\$1
  local items
  items=$('${escapedCliPath}' --list-completions "\$source" 2>/dev/null)
  [[ -z "\$items" ]] && return
  local -a ids descs
  while IFS=\$'\\t' read -r id desc; do
    ids+=("\$id")
    if [[ -n "\$desc" ]]; then
      descs+=("\$id:\$desc")
    else
      descs+=("\$id")
    fi
  done <<< "\$items"
  _describe '' descs
}

compdef _repl repl
`;
}

function buildZshArgsCases(escapedCliPath: string): string {
  const cases: string[] = [];

  for (const [cmdName, spec] of Object.entries(commandSpecs)) {
    const lines: string[] = [];
    lines.push(`        ${cmdName})`);

    if (spec.subcommands && spec.subcommands.length > 0) {
      const hasTopFlags = spec.flags && spec.flags.length > 0;

      if (hasTopFlags) {
        // Mixed subcommands + flags (e.g., `run`).
        // Use _describe + manual value/subcommand handling.
        // (_arguments -C garbles output when mixing flags with subcommands.)
        const subcmdDescs = spec.subcommands
          .map(s => s.desc ? `'${s.name}:${s.desc}'` : `'${s.name}'`)
          .join(' ');
        const optDescs = spec.flags!.flatMap(f =>
          f.names.map(n => `'${n}:${f.desc}'`)
        ).join(' ');

        lines.push(`          local -a subcmds opts`);
        lines.push(`          subcmds=(${subcmdDescs})`);
        lines.push(`          opts=(${optDescs})`);

        emitZshFlagValueHandler(lines, spec.flags!);
        emitZshSubcommandFinder(lines, spec.flags!);

        lines.push(`          if [[ -z "\$sub" ]]; then`);
        lines.push(`            if [[ \$words[\$CURRENT] == -* ]]; then`);
        lines.push(`              _describe 'option' opts`);
        lines.push(`            else`);
        lines.push(`              _describe 'subcommand' subcmds`);
        lines.push(`            fi`);
        lines.push(`            return`);
        lines.push(`          fi`);

        lines.push(`          case \$sub in`);
        for (const sub of spec.subcommands) {
          lines.push(`            ${sub.name})`);
          const hasSubFlags = sub.flags && sub.flags.length > 0;
          const hasSubPos = sub.positional && sub.positional.length > 0;

          if (hasSubFlags) {
            lines.push(`              if [[ \$words[\$CURRENT] == -* ]]; then`);
            lines.push(`                _describe 'option' opts`);
            lines.push(`              fi`);
          }

          if (hasSubPos) {
            for (const pos of sub.positional!) {
              if (typeof pos.source === 'string') {
                lines.push(`              _repl_dynamic '${pos.source}'`);
              } else if (Array.isArray(pos.source)) {
                lines.push(`              compadd -- ${pos.source.join(' ')}`);
              }
            }
          }

          if (!hasSubFlags && !hasSubPos) {
            lines.push(`              # no completions`);
          }
          lines.push(`              ;;`);
        }
        lines.push(`          esac`);
      } else {
        // Subcommands only — simpler pattern
        const subcmdDescs = spec.subcommands
          .map(s => s.desc ? `'${s.name}:${s.desc}'` : `'${s.name}'`)
          .join(' ');
        lines.push(`          local -a subcmds`);
        lines.push(`          subcmds=(${subcmdDescs})`);
        lines.push(`          if [[ \$CURRENT -eq 2 ]]; then`);
        lines.push(`            _describe 'subcommand' subcmds`);
        lines.push(`          else`);
        lines.push(`            local sub=\$words[2]`);
        lines.push(`            case \$sub in`);

        for (const sub of spec.subcommands) {
          lines.push(`              ${sub.name})`);
          const subArgs = buildZshArgs(sub.flags, sub.positional);
          if (subArgs.length > 0) {
            lines.push(...subArgs.map(l => `                ${l}`));
          } else {
            lines.push(`                # no completions`);
          }
          lines.push(`                ;;`);
        }

        lines.push(`            esac`);
        lines.push(`          fi`);
      }
    } else {
      // No subcommands — handle flags and positionals directly
      const directArgs = buildZshArgs(spec.flags, spec.positional);
      if (directArgs.length > 0) {
        lines.push(...directArgs.map(l => `          ${l}`));
      } else {
        lines.push(`          # no completions`);
      }
    }

    lines.push(`          ;;`);
    cases.push(lines.join('\n'));
  }

  return cases.join('\n');
}

// =============================================================================
// Bash generator
// =============================================================================

export function generateBashScript(cliPath: string): string {
  const escapedCliPath = cliPath.replace(/'/g, "'\\''");
  const topLevelCommands = Object.keys(commandSpecs).join(' ');
  const cmdCases = buildBashCmdCases(escapedCliPath);

  return `_repl_completions() {
  local cur prev words cword
  _init_completion || return

  local commands="${topLevelCommands}"

  if [[ \$cword -eq 1 ]]; then
    COMPREPLY=(\$(compgen -W "\$commands" -- "\$cur"))
    return
  fi

  local cmd="\${words[1]}"
  case "\$cmd" in
${cmdCases}
  esac
}

_repl_dynamic() {
  local source="\$1"
  local items
  items=$('${escapedCliPath}' --list-completions "\$source" 2>/dev/null)
  [[ -z "\$items" ]] && return
  local ids
  ids=\$(echo "\$items" | cut -f1)
  COMPREPLY=(\$(compgen -W "\$ids" -- "\$cur"))
}

complete -F _repl_completions repl
`;
}

function buildBashCmdCases(escapedCliPath: string): string {
  const cases: string[] = [];

  for (const [cmdName, spec] of Object.entries(commandSpecs)) {
    const lines: string[] = [];
    lines.push(`    ${cmdName})`);

    if (spec.subcommands && spec.subcommands.length > 0) {
      const subNames = spec.subcommands.map(s => s.name).join(' ');
      const hasTopFlags = spec.flags && spec.flags.length > 0;

      lines.push(`      if [[ \$cword -eq 2 ]]; then`);
      if (hasTopFlags) {
        const flagNames = allFlagNames(spec.flags!).join(' ');
        lines.push(`        if [[ "\$cur" == -* ]]; then`);
        lines.push(`          COMPREPLY=(\$(compgen -W "${flagNames}" -- "\$cur"))`);
        lines.push(`        else`);
        lines.push(`          COMPREPLY=(\$(compgen -W "${subNames}" -- "\$cur"))`);
        lines.push(`        fi`);
      } else {
        lines.push(`        COMPREPLY=(\$(compgen -W "${subNames}" -- "\$cur"))`);
      }
      lines.push(`        return`);
      lines.push(`      fi`);
      lines.push(`      local sub="\${words[2]}"`);
      lines.push(`      case "\$sub" in`);

      for (const sub of spec.subcommands) {
        lines.push(`        ${sub.name})`);
        buildBashSubArgs(lines, sub);
        lines.push(`          ;;`);
      }

      if (hasTopFlags) {
        // Fallthrough: first arg is a flag, not a subcommand
        lines.push(`        *)`);
        buildBashFlagArgs(lines, spec.flags!, '          ');
        lines.push(`          ;;`);
      }

      lines.push(`      esac`);
    } else {
      // No subcommands
      if (spec.flags && spec.flags.length > 0) {
        buildBashFlagArgs(lines, spec.flags, '      ');
      }

      if (spec.positional && spec.positional.length > 0) {
        for (let i = 0; i < spec.positional.length; i++) {
          const cwordIdx = 2 + i;
          const pos = spec.positional[i];
          lines.push(`      if [[ \$cword -eq ${cwordIdx} ]]; then`);
          if (Array.isArray(pos.source)) {
            lines.push(`        COMPREPLY=(\$(compgen -W "${pos.source.join(' ')}" -- "\$cur"))`);
          } else {
            lines.push(`        _repl_dynamic '${pos.source}'`);
          }
          lines.push(`        return`);
          lines.push(`      fi`);
        }
      }
    }

    lines.push(`      ;;`);
    cases.push(lines.join('\n'));
  }

  return cases.join('\n');
}

function buildBashSubArgs(lines: string[], sub: SubcommandSpec): void {
  if (sub.flags && sub.flags.length > 0) {
    const flagNames = allFlagNames(sub.flags).join(' ');
    lines.push(`          if [[ "\$cur" == -* ]]; then`);
    lines.push(`            COMPREPLY=(\$(compgen -W "${flagNames}" -- "\$cur"))`);
    lines.push(`            return`);
    lines.push(`          fi`);
    const valued = sub.flags.filter(f => f.value !== null && f.value !== false);
    if (valued.length > 0) {
      lines.push(`          case "\$prev" in`);
      for (const flag of valued) {
        const pattern = flag.names.join('|');
        if (Array.isArray(flag.value)) {
          lines.push(`            ${pattern})`);
          lines.push(`              COMPREPLY=(\$(compgen -W "${flag.value.join(' ')}" -- "\$cur"))`);
          lines.push(`              return ;;`);
        } else {
          lines.push(`            ${pattern})`);
          lines.push(`              _repl_dynamic '${flag.value}'`);
          lines.push(`              return ;;`);
        }
      }
      lines.push(`          esac`);
    }
  }

  if (sub.positional && sub.positional.length > 0) {
    for (let i = 0; i < sub.positional.length; i++) {
      const cwordIdx = 3 + i;
      const pos = sub.positional[i];
      lines.push(`          if [[ \$cword -eq ${cwordIdx} ]]; then`);
      if (Array.isArray(pos.source)) {
        lines.push(`            COMPREPLY=(\$(compgen -W "${pos.source.join(' ')}" -- "\$cur"))`);
      } else {
        lines.push(`            _repl_dynamic '${pos.source}'`);
      }
      lines.push(`            return`);
      lines.push(`          fi`);
    }
  }
}

function buildBashFlagArgs(lines: string[], flags: FlagDef[], indent: string): void {
  const flagNames = allFlagNames(flags).join(' ');
  lines.push(`${indent}if [[ "\$cur" == -* ]]; then`);
  lines.push(`${indent}  COMPREPLY=(\$(compgen -W "${flagNames}" -- "\$cur"))`);
  lines.push(`${indent}  return`);
  lines.push(`${indent}fi`);

  const valued = flags.filter(f => f.value !== null && f.value !== false);
  if (valued.length > 0) {
    lines.push(`${indent}case "\$prev" in`);
    for (const flag of valued) {
      const pattern = flag.names.join('|');
      if (Array.isArray(flag.value)) {
        lines.push(`${indent}  ${pattern})`);
        lines.push(`${indent}    COMPREPLY=(\$(compgen -W "${(flag.value as string[]).join(' ')}" -- "\$cur"))`);
        lines.push(`${indent}    return ;;`);
      } else {
        lines.push(`${indent}  ${pattern})`);
        lines.push(`${indent}    _repl_dynamic '${flag.value}'`);
        lines.push(`${indent}    return ;;`);
      }
    }
    lines.push(`${indent}esac`);
  }
}
