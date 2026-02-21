import { writeFileSync, mkdirSync } from 'fs';
import { join, basename } from 'path';
import { getControlPlaneUrl, isConnectionRefused, printTable, output } from '../config';
import { ApiClient } from '../client';
import { idToSlug, parseInputId } from '@skyrepl/contracts';

export async function artifactCommand(args: string[]): Promise<void> {
  const subcommand = args[0];

  switch (subcommand) {
    case 'list':
      await artifactList(args.slice(1));
      break;
    case 'download':
      await artifactDownload(args.slice(1));
      break;
    default:
      console.error('Usage: repl artifact <list|download>');
      console.error('  list <run-id>                    List artifacts for a run');
      console.error('  download <artifact-id> [--output=dir]  Download an artifact');
      process.exit(2);
  }
}

async function artifactList(args: string[]): Promise<void> {
  const runSlug = args[0];
  if (!runSlug) {
    console.error('Usage: repl artifact list <run-id>');
    process.exit(2);
  }

  const runIntId = parseInputId(runSlug);
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data } = await client.listArtifacts(String(runIntId));

    if (data.length === 0) {
      output([], () => { console.log(`No artifacts found for run ${runSlug}.`); });
      return;
    }

    const rows = data.map((a) => {
      const created = a.created_at
        ? new Date(a.created_at).toISOString().slice(0, 16).replace('T', ' ')
        : '-';
      const size = a.size_bytes > 0 ? formatBytes(a.size_bytes) : '-';
      return [idToSlug(a.id), a.path ?? '-', size, created];
    });

    output(data, () => {
      printTable(['ID', 'PATH', 'SIZE', 'CREATED'], rows, [6, 40, 10, 16]);
    });
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to list artifacts: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

async function artifactDownload(args: string[]): Promise<void> {
  if (args.length === 0) {
    console.error('Usage: repl artifact download <artifact-id> [--output=dir]');
    process.exit(2);
  }

  let artifactSlug = '';
  let outputDir = '.';

  for (const arg of args) {
    if (arg.startsWith('--output=')) {
      outputDir = arg.slice('--output='.length);
    } else if (!arg.startsWith('-')) {
      artifactSlug = arg;
    }
  }

  if (!artifactSlug) {
    console.error('Usage: repl artifact download <artifact-id> [--output=dir]');
    process.exit(2);
  }

  const artifactIntId = parseInputId(artifactSlug);
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data, filename } = await client.downloadArtifact(String(artifactIntId));

    mkdirSync(outputDir, { recursive: true });
    const outputPath = join(outputDir, filename);
    writeFileSync(outputPath, data);
    console.log(`Downloaded: ${outputPath} (${formatBytes(data.length)})`);
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to download artifact: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)}GB`;
}
