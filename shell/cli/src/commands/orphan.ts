// commands/orphan.ts — repl orphan whitelist add/remove/list

import { getControlPlaneUrl, isConnectionRefused, printTable, output } from '../config';
import { ApiClient } from '../client';

export async function orphanCommand(args: string[]): Promise<void> {
  const subcommand = args[0];

  switch (subcommand) {
    case 'whitelist':
      await orphanWhitelistCommand(args.slice(1));
      break;
    default:
      console.error('Usage: repl orphan whitelist <add|remove|list>');
      process.exit(2);
  }
}

async function orphanWhitelistCommand(args: string[]): Promise<void> {
  const subcommand = args[0];

  switch (subcommand) {
    case 'add':
      await orphanWhitelistAdd(args.slice(1));
      break;
    case 'remove':
      await orphanWhitelistRemove(args.slice(1));
      break;
    case 'list':
      await orphanWhitelistList(args.slice(1));
      break;
    default:
      console.error('Usage: repl orphan whitelist <add|remove|list>');
      process.exit(2);
  }
}

/**
 * repl orphan whitelist add <provider> <provider_id> --reason "..."
 */
async function orphanWhitelistAdd(args: string[]): Promise<void> {
  if (args.length < 2) {
    console.error('Usage: repl orphan whitelist add <provider> <provider_id> --reason "..."');
    process.exit(2);
  }

  const provider = args[0]!;
  const providerId = args[1]!;

  // Parse --reason flag
  let reason = '';
  for (let i = 2; i < args.length; i++) {
    if (args[i] === '--reason' && args[i + 1]) {
      reason = args[i + 1]!;
      i++;
    }
  }

  if (!reason) {
    console.error('--reason is required');
    process.exit(2);
  }

  const client = new ApiClient(getControlPlaneUrl());
  try {
    const result = await client.addOrphanWhitelist({ provider, provider_id: providerId, reason });
    output(result.data, () => {
      console.log(`Added ${provider}/${providerId} to orphan whitelist.`);
    }, 'orphan whitelist');
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to add to whitelist: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

/**
 * repl orphan whitelist remove <provider> <provider_id>
 */
async function orphanWhitelistRemove(args: string[]): Promise<void> {
  if (args.length < 2) {
    console.error('Usage: repl orphan whitelist remove <provider> <provider_id>');
    process.exit(2);
  }

  const provider = args[0]!;
  const providerId = args[1]!;

  const client = new ApiClient(getControlPlaneUrl());
  try {
    await client.removeOrphanWhitelist(provider, providerId);
    output({ provider, provider_id: providerId, removed: true }, () => {
      console.log(`Removed ${provider}/${providerId} from orphan whitelist.`);
    }, 'orphan whitelist');
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to remove from whitelist: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

/**
 * repl orphan whitelist list [--provider <name>]
 */
async function orphanWhitelistList(args: string[]): Promise<void> {
  let provider: string | undefined;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--provider' && args[i + 1]) {
      provider = args[i + 1]!;
      i++;
    }
  }

  const client = new ApiClient(getControlPlaneUrl());
  try {
    const result = await client.listOrphanWhitelist(provider);
    const { data } = result;

    if (data.length === 0) {
      output([], () => { console.log('No whitelist entries found.'); }, 'orphan whitelist');
      return;
    }

    const rows = data.map((entry: any) => {
      const ack = entry.acknowledged_at ? new Date(entry.acknowledged_at).toISOString().slice(0, 16).replace('T', ' ') : '-';
      return [
        entry.provider || '-',
        entry.provider_id || '-',
        entry.reason?.length > 40 ? entry.reason.slice(0, 37) + '...' : (entry.reason || '-'),
        entry.acknowledged_by || '-',
        ack,
      ];
    });

    output(data, () => {
      printTable(['PROVIDER', 'PROVIDER_ID', 'REASON', 'BY', 'AT'], rows, [10, 20, 42, 16, 16]);
    }, 'orphan whitelist');
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to list whitelist: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}
