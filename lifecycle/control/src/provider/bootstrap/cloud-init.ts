// provider/bootstrap/cloud-init.ts - Cloud-init assembler
//
// Assembles a complete #cloud-config YAML document from BootstrapConfig by
// collecting and merging cloud-init snippets, then serialising manually
// (no external YAML library — cloud-init's subset is structured enough to
// build directly).

import { createHash } from "crypto";
import type { BootstrapConfig, BootstrapScript } from "../types";
import type { CloudInitSnippet, WriteFileEntry } from "./snippets";
import {
  disableAptDaily,
  downloadAgent,
  installFeature,
  startAgent,
  userInitScript,
  writeAgentConfig,
} from "./snippets";

// =============================================================================
// YAML helpers
// =============================================================================

/**
 * Escape a YAML scalar as a single-quoted string.
 * Single-quoted YAML scalars only need ' doubled to ''.
 */
function yamlSingleQuote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/**
 * Serialise a string value as a YAML literal block scalar (the | style).
 * Each line of the content is indented by the given prefix.
 * A trailing newline is added if the content does not already end with one.
 */
function yamlLiteralBlock(content: string, indent: string): string {
  const normalised = content.endsWith("\n") ? content : content + "\n";
  return "|\n" + normalised.split("\n").map((line) => indent + line).join("\n");
}

/**
 * Serialise a single write_files entry.
 *
 * Example output:
 *   - path: /etc/skyrepl/config.json
 *     content: |
 *       {"controlPlaneUrl":"..."}
 *     permissions: '0644'
 *     owner: root:root
 */
function serialiseWriteFile(entry: WriteFileEntry): string {
  const lines: string[] = [];
  lines.push(`- path: ${entry.path}`);
  lines.push(`  content: ${yamlLiteralBlock(entry.content, "    ")}`);
  if (entry.permissions != null) {
    lines.push(`  permissions: ${yamlSingleQuote(entry.permissions)}`);
  }
  if (entry.owner != null) {
    lines.push(`  owner: ${entry.owner}`);
  }
  return lines.join("\n");
}

/**
 * Serialise a single runcmd entry (array of strings) as a YAML flow sequence.
 *
 * Example output:
 *   - ["systemctl", "disable", "--now", "apt-daily.timer"]
 */
function serialiseRuncmdEntry(cmd: string[]): string {
  const items = cmd.map((s) => JSON.stringify(s)).join(", ");
  return `- [${items}]`;
}

// =============================================================================
// Assembler
// =============================================================================

/**
 * Collect all snippets in canonical order and merge their write_files, runcmd,
 * and packages arrays into a single flat structure.
 */
function collectSnippets(config: BootstrapConfig): {
  writeFiles: WriteFileEntry[];
  runcmd: string[][];
  packages: string[];
} {
  const snippets: CloudInitSnippet[] = [
    disableAptDaily(),
    writeAgentConfig(config.controlPlaneUrl, config.registrationToken, config.environment),
    downloadAgent(config.controlPlaneUrl),
    ...(config.features ?? []).map((f) =>
      installFeature(f.name, f.config as Record<string, string> | undefined)
    ),
    ...(config.initScript ? [userInitScript(config.initScript)] : []),
    startAgent(),
  ];

  const writeFiles: WriteFileEntry[] = [];
  const runcmd: string[][] = [];
  const packages: string[] = [];

  for (const snippet of snippets) {
    if (snippet.writeFiles) writeFiles.push(...snippet.writeFiles);
    if (snippet.runcmd) runcmd.push(...snippet.runcmd);
    if (snippet.packages) packages.push(...snippet.packages);
  }

  return { writeFiles, runcmd, packages };
}

/**
 * Assemble a cloud-init user-data document from BootstrapConfig.
 *
 * Returns { content, format: "cloud-init", checksum } where:
 *   - content  is a valid #cloud-config YAML string
 *   - checksum is the SHA-256 hex digest of content
 */
export function assembleCloudInit(config: BootstrapConfig): BootstrapScript {
  const { writeFiles, runcmd, packages } = collectSnippets(config);

  const sections: string[] = ["#cloud-config"];

  if (packages.length > 0) {
    const pkgLines = packages.map((p) => `- ${p}`).join("\n");
    sections.push(`packages:\n${pkgLines}`);
  }

  if (writeFiles.length > 0) {
    const fileBlocks = writeFiles.map(serialiseWriteFile).join("\n");
    sections.push(`write_files:\n${fileBlocks}`);
  }

  if (runcmd.length > 0) {
    const cmdLines = runcmd.map(serialiseRuncmdEntry).join("\n");
    sections.push(`runcmd:\n${cmdLines}`);
  }

  const content = sections.join("\n") + "\n";
  const checksum = createHash("sha256").update(content).digest("hex");

  return { content, format: "cloud-init" as const, checksum };
}
