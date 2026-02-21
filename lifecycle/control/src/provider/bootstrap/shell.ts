// provider/bootstrap/shell.ts - Shell script bootstrap assembler
//
// Assembles a complete #!/bin/bash bootstrap script from BootstrapConfig by
// consuming the same snippet library as cloud-init.ts, but formatting the
// output as a standalone shell script rather than a #cloud-config YAML document.
//
// Each CloudInitSnippet contributes:
//   writeFiles -> rendered as mkdir + heredoc cat commands
//   runcmd     -> rendered as shell command invocations
//   packages   -> rendered as apt-get install commands

import { createHash } from "crypto";
import type { BootstrapConfig, BootstrapScript } from "../types";
import type { CloudInitSnippet, WriteFileEntry } from "./snippets";
import {
  disableAptDaily,
  downloadAgent,
  installFeature,
  startAgent,
  writeAgentConfig,
} from "./snippets";

// =============================================================================
// Shell rendering helpers
// =============================================================================

/**
 * Render a WriteFileEntry as shell commands:
 *   mkdir -p <dir>
 *   cat > <path> << 'HEREDOC'
 *   <content>
 *   HEREDOC
 *
 * Uses a unique heredoc delimiter to avoid collisions with content.
 */
function renderWriteFile(entry: WriteFileEntry): string {
  const dir = entry.path.replace(/\/[^/]+$/, "");
  const lines: string[] = [];

  if (dir) {
    lines.push(`mkdir -p ${dir}`);
  }

  lines.push(`cat > ${entry.path} << 'SKYREPL_CONFIG_EOF'`);
  // Ensure content ends with a newline before the delimiter
  const content = entry.content.endsWith("\n")
    ? entry.content
    : entry.content + "\n";
  lines.push(content + "SKYREPL_CONFIG_EOF");

  return lines.join("\n");
}

/**
 * Render a runcmd entry (string[]) as a shell command line.
 *
 * For simple commands (no shell metacharacters in args) the arguments are
 * joined with spaces. For entries that already embed a shell expression
 * (e.g. ["bash", "-c", "..."]) the args are quoted where necessary.
 */
function renderCmd(cmd: string[]): string {
  if (cmd.length === 0) return "";

  // Special case: ["bash", "-c", <script>] — emit as-is so the inner
  // script string is not double-escaped.
  if (cmd[0] === "bash" && cmd[1] === "-c" && cmd.length === 3) {
    return `bash -c ${shellQuote(cmd[2])}`;
  }

  return cmd.map(shellQuote).join(" ");
}

/**
 * Quote a single shell argument if it contains characters that need quoting.
 * Uses single-quotes for safety; embeds literal single-quotes via '\''.
 */
function shellQuote(arg: string): string {
  // If the argument is safe (alphanumeric, hyphen, slash, dot, colon, @, =),
  // return it unquoted for readability.
  if (/^[a-zA-Z0-9_./:@=+-]+$/.test(arg)) return arg;
  // Otherwise single-quote with embedded single-quote escaping.
  return `'${arg.replace(/'/g, "'\\''")}'`;
}

/**
 * Render apt-get install for a list of packages.
 */
function renderPackages(packages: string[]): string {
  const pkgList = packages.join(" ");
  return `apt-get install -y ${pkgList}`;
}

// =============================================================================
// Snippet collection
// =============================================================================

/**
 * Collect all snippets in canonical order (same order as cloud-init assembler).
 * The initScript is handled separately in the assembler (not via userInitScript
 * snippet) so that we can emit the raw script with a comment rather than
 * wrapping it in `bash -c`.
 */
function collectSnippets(config: BootstrapConfig): CloudInitSnippet[] {
  return [
    disableAptDaily(),
    writeAgentConfig(config.controlPlaneUrl, config.registrationToken),
    downloadAgent(config.controlPlaneUrl),
    ...(config.features ?? []).map((f) =>
      installFeature(f.name, f.config as Record<string, string> | undefined)
    ),
    startAgent(),
  ];
}

// =============================================================================
// Assembler
// =============================================================================

/**
 * Assemble a shell bootstrap script from BootstrapConfig.
 *
 * Returns { content, format: "shell", checksum } where:
 *   - content  is a valid #!/bin/bash script string
 *   - checksum is the SHA-256 hex digest of content
 *
 * The script:
 *   1. Sets strict mode (set -eu)
 *   2. Redirects output to /tmp/bootstrap.log
 *   3. Processes snippets in canonical order:
 *      - writeFiles rendered as heredoc cat commands
 *      - packages rendered as apt-get install
 *      - runcmd rendered as shell invocations
 *   4. Appends environment variable exports (SKYREPL_* + custom env)
 *
 * Note: environment variables are appended after snippet commands because the
 * agent reads the config file rather than env vars for registration, and the
 * export statements must come before the agent exec call. The startAgent()
 * snippet is last, so env exports are inserted immediately before it by
 * collecting all snippet lines first, then inserting the env block before
 * the final startAgent lines.
 */
export function assembleShellBootstrap(config: BootstrapConfig): BootstrapScript {
  const snippets = collectSnippets(config);

  const sections: string[] = ["#!/bin/bash", "set -eu", "", "exec > /tmp/bootstrap.log 2>&1", ""];

  for (const snippet of snippets) {
    const snippetLines: string[] = [];

    // Render packages first
    if (snippet.packages && snippet.packages.length > 0) {
      snippetLines.push(renderPackages(snippet.packages));
    }

    // Render write_files as heredoc cat commands
    if (snippet.writeFiles) {
      for (const entry of snippet.writeFiles) {
        snippetLines.push(renderWriteFile(entry));
      }
    }

    // Render runcmd entries as shell commands
    if (snippet.runcmd) {
      for (const cmd of snippet.runcmd) {
        const rendered = renderCmd(cmd);
        if (rendered) snippetLines.push(rendered);
      }
    }

    if (snippetLines.length > 0) {
      sections.push(snippetLines.join("\n"));
    }
  }

  // The startAgent snippet is last in the sections array. Insert initScript
  // and env vars before it so the ordering matches the original inline bootstrap:
  //   ... setup ... | initScript | env exports | exec python3 agent.py
  const lastSection = sections.pop()!;

  // User init script: emit as raw shell with a comment (not via bash -c),
  // matching the original template which inlined the script text directly.
  if (config.initScript) {
    sections.push(`# User init script\n${config.initScript}`);
  }

  // Build environment variable exports block
  const envVars: Record<string, string> = {
    SKYREPL_CONTROL_PLANE_URL: config.controlPlaneUrl,
    SKYREPL_REGISTRATION_TOKEN: config.registrationToken,
    ...(config.environment ?? {}),
  };

  const envLines = Object.entries(envVars)
    .map(([key, value]) => `export ${key}="${value}"`)
    .join("\n");

  sections.push(envLines);
  sections.push(lastSection);

  const content = sections.join("\n") + "\n";
  const checksum = createHash("sha256").update(content).digest("hex");

  return { content, format: "shell" as const, checksum };
}
