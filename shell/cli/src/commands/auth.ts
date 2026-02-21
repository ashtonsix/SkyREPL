// commands/auth.ts — API key management (AUTH-04)

import { getControlPlaneUrl, isConnectionRefused, printTable, output } from '../config';
import { ApiClient } from '../client';

export async function authCommand(args: string[]): Promise<void> {
  const sub = args[0];
  switch (sub) {
    case 'create-key':
      await createKey(args.slice(1));
      break;
    case 'revoke-key':
      await revokeKey(args.slice(1));
      break;
    case 'list-keys':
      await listKeys();
      break;
    default:
      console.error('Usage: repl auth <create-key|revoke-key|list-keys>');
      console.error('');
      console.error('  create-key --name <name> [--role admin|member|viewer]');
      console.error('  revoke-key <key-id>');
      console.error('  list-keys');
      process.exit(2);
  }
}

async function createKey(args: string[]): Promise<void> {
  let name = '';
  let role = 'member';
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--name' && args[i + 1]) { name = args[++i]; continue; }
    if (args[i] === '--role' && args[i + 1]) { role = args[++i]; continue; }
  }
  if (!name) {
    console.error('Usage: repl auth create-key --name <name> [--role admin|member|viewer]');
    process.exit(2);
  }
  if (!['admin', 'member', 'viewer'].includes(role)) {
    console.error('Role must be one of: admin, member, viewer');
    process.exit(2);
  }

  const client = new ApiClient(getControlPlaneUrl());
  try {
    const result = await client.createKey(name, role);
    output(result, () => {
      console.log(`Created API key "${result.name}" (${result.role})`);
      console.log(`  Key ID:  ${result.id}`);
      console.log(`  Expires: ${new Date(result.expires_at).toISOString().slice(0, 10)}`);
      console.log('');
      console.log(`  ${result.raw_key}`);
      console.log('');
      console.log('  Store this key securely — it will not be shown again.');
    });
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to create key: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

async function revokeKey(args: string[]): Promise<void> {
  const idArg = args[0];
  if (!idArg) {
    console.error('Usage: repl auth revoke-key <key-id>');
    process.exit(2);
  }
  const id = parseInt(idArg, 10);
  if (isNaN(id)) {
    console.error('key-id must be a number');
    process.exit(2);
  }

  const client = new ApiClient(getControlPlaneUrl());
  try {
    await client.revokeKey(id);
    console.log(`API key ${id} revoked.`);
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to revoke key: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

async function listKeys(): Promise<void> {
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data } = await client.listKeys();
    if (data.length === 0) {
      output([], () => { console.log('No API keys found.'); });
      return;
    }
    const rows = data.map((k: any) => {
      const created = k.created_at ? new Date(k.created_at).toISOString().slice(0, 10) : '-';
      const lastUsed = k.last_used_at ? new Date(k.last_used_at).toISOString().slice(0, 10) : 'never';
      const status = k.revoked_at ? 'revoked' : (k.expires_at && k.expires_at < Date.now() ? 'expired' : 'active');
      return [String(k.id), k.name ?? '-', k.role ?? '-', status, lastUsed, created];
    });
    output(data, () => {
      printTable(['ID', 'NAME', 'ROLE', 'STATUS', 'LAST USED', 'CREATED'], rows, [4, 20, 8, 8, 12, 12]);
    });
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to list keys: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}
