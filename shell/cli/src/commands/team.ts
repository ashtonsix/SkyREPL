// commands/team.ts — Team management CLI (TENANT-02)

import { getControlPlaneUrl, isConnectionRefused, printTable, output } from '../config';
import { ApiClient } from '../client';

export async function teamCommand(args: string[]): Promise<void> {
  const sub = args[0];
  switch (sub) {
    case 'create':
      await teamCreate(args.slice(1));
      break;
    case 'invite':
      await teamInvite(args.slice(1));
      break;
    case 'remove':
      await teamRemove(args.slice(1));
      break;
    case 'list':
      await teamList();
      break;
    case 'info':
      await teamInfo(args.slice(1));
      break;
    case 'budget':
      await teamBudget(args.slice(1));
      break;
    default:
      console.error('Usage: repl team <create|invite|remove|list|info|budget>');
      console.error('');
      console.error('  create <name> [--seats N] [--budget USD]');
      console.error('  invite <email> [--role admin|member|viewer] [--budget USD]');
      console.error('  remove <user-id>');
      console.error('  list');
      console.error('  info');
      console.error('  budget [--team USD] [--user <user-id> USD]');
      process.exit(2);
  }
}

async function teamCreate(args: string[]): Promise<void> {
  let name = '';
  let seatCap: number | undefined;
  let budgetUsd: number | undefined;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--seats' && args[i + 1]) { seatCap = parseInt(args[++i], 10); continue; }
    if (args[i] === '--budget' && args[i + 1]) { budgetUsd = parseFloat(args[++i]); continue; }
    if (!name && !args[i].startsWith('--')) { name = args[i]; continue; }
  }

  if (!name) {
    console.error('Usage: repl team create <name> [--seats N] [--budget USD]');
    process.exit(2);
  }

  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data } = await client.createTenant(name, {
      seat_cap: seatCap,
      budget_usd: budgetUsd,
    });
    output(data, () => {
      console.log(`Team "${data.name}" created (ID: ${data.id})`);
      console.log(`  Seat cap: ${data.seat_cap}`);
      if (data.budget_usd !== null) {
        console.log(`  Budget:   $${data.budget_usd}`);
      }
    });
  } catch (err) {
    handleError(err, 'create team');
  }
}

async function teamInvite(args: string[]): Promise<void> {
  let email = '';
  let role = 'member';
  let budgetUsd: number | undefined;
  let displayName: string | undefined;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--role' && args[i + 1]) { role = args[++i]; continue; }
    if (args[i] === '--budget' && args[i + 1]) { budgetUsd = parseFloat(args[++i]); continue; }
    if (args[i] === '--name' && args[i + 1]) { displayName = args[++i]; continue; }
    if (!email && !args[i].startsWith('--')) { email = args[i]; continue; }
  }

  if (!email) {
    console.error('Usage: repl team invite <email> [--role admin|member|viewer] [--budget USD]');
    process.exit(2);
  }
  if (!['admin', 'member', 'viewer'].includes(role)) {
    console.error('Role must be one of: admin, member, viewer');
    process.exit(2);
  }

  // We need the caller's tenant ID — get it from /v1/usage (returns tenant_id)
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data: usage } = await client.getUsage();
    const tenantId = usage.tenant_id;
    const { data } = await client.inviteUser(tenantId, email, role, {
      display_name: displayName,
      budget_usd: budgetUsd,
    });
    output(data, () => {
      console.log(`Invited ${data.email} as ${data.role}`);
      console.log(`  User ID: ${data.id}`);
    });
  } catch (err) {
    handleError(err, 'invite user');
  }
}

async function teamRemove(args: string[]): Promise<void> {
  const idArg = args[0];
  if (!idArg) {
    console.error('Usage: repl team remove <user-id>');
    process.exit(2);
  }
  const userId = parseInt(idArg, 10);
  if (isNaN(userId)) {
    console.error('user-id must be a number');
    process.exit(2);
  }

  const client = new ApiClient(getControlPlaneUrl());
  try {
    await client.removeUser(userId);
    console.log(`User ${userId} removed from team.`);
  } catch (err) {
    handleError(err, 'remove user');
  }
}

async function teamList(): Promise<void> {
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data: usage } = await client.getUsage();
    const tenantId = usage.tenant_id;
    const { data: users } = await client.listTenantUsers(tenantId);

    if (users.length === 0) {
      output([], () => { console.log('No team members found.'); });
      return;
    }

    const rows = users.map((u: any) => {
      const created = u.created_at ? new Date(u.created_at).toISOString().slice(0, 10) : '-';
      const budget = u.budget_usd !== null ? `$${u.budget_usd}` : 'none';
      return [String(u.id), u.email ?? '-', u.role ?? '-', budget, created];
    });
    output(users, () => {
      printTable(['ID', 'EMAIL', 'ROLE', 'BUDGET', 'CREATED'], rows, [4, 30, 8, 10, 12]);
    });
  } catch (err) {
    handleError(err, 'list team');
  }
}

async function teamInfo(args: string[]): Promise<void> {
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data: usage } = await client.getUsage();
    const tenantId = usage.tenant_id;
    const { data: tenant } = await client.getTenant(tenantId);

    output({ ...tenant, usage }, () => {
      console.log(`Team: ${tenant.name} (ID: ${tenant.id})`);
      console.log(`  Members:  ${tenant.user_count}/${tenant.seat_cap}`);
      if (tenant.budget_usd !== null) {
        console.log(`  Budget:   $${usage.total_cost_usd.toFixed(2)} / $${tenant.budget_usd}`);
      } else {
        console.log(`  Spending: $${usage.total_cost_usd.toFixed(2)} (no cap)`);
      }

      if (usage.users && usage.users.length > 0) {
        console.log('');
        console.log('  Members:');
        for (const u of usage.users) {
          const cap = u.budget_usd !== null ? ` / $${u.budget_usd}` : '';
          console.log(`    ${u.email} (${u.role}): $${u.used_usd.toFixed(2)}${cap}`);
        }
      }
    });
  } catch (err) {
    handleError(err, 'get team info');
  }
}

async function teamBudget(args: string[]): Promise<void> {
  let teamBudget: number | undefined;
  let userId: number | undefined;
  let userBudget: number | undefined;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--team' && args[i + 1]) { teamBudget = parseFloat(args[++i]); continue; }
    if (args[i] === '--user' && args[i + 1] && args[i + 2]) {
      userId = parseInt(args[++i], 10);
      userBudget = parseFloat(args[++i]);
      continue;
    }
  }

  if (teamBudget === undefined && userId === undefined) {
    // Show current usage
    const client = new ApiClient(getControlPlaneUrl());
    try {
      const { data } = await client.getUsage();
      console.log(`Team: ${data.tenant_name}`);
      const teamCap = data.tenant_budget_usd !== null ? ` / $${data.tenant_budget_usd}` : ' (no cap)';
      console.log(`  Total spend: $${data.total_cost_usd.toFixed(2)}${teamCap}`);
      if (data.users.length > 0) {
        console.log('');
        for (const u of data.users) {
          const cap = u.budget_usd !== null ? ` / $${u.budget_usd}` : '';
          console.log(`  ${u.email}: $${u.used_usd.toFixed(2)}${cap}`);
        }
      }
    } catch (err) {
      handleError(err, 'get budget');
    }
    return;
  }

  const client = new ApiClient(getControlPlaneUrl());
  try {
    if (teamBudget !== undefined) {
      const { data: usage } = await client.getUsage();
      await client.updateTenant(usage.tenant_id, { budget_usd: teamBudget });
      console.log(`Team budget set to $${teamBudget}`);
    }
    if (userId !== undefined && userBudget !== undefined) {
      await client.updateUser(userId, { budget_usd: userBudget });
      console.log(`User ${userId} budget set to $${userBudget}`);
    }
  } catch (err) {
    handleError(err, 'set budget');
  }
}

function handleError(err: unknown, action: string): never {
  if (isConnectionRefused(err)) {
    console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
    process.exit(2);
  }
  console.error(`Failed to ${action}: ${err instanceof Error ? err.message : err}`);
  process.exit(2);
}
