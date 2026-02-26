import { getControlPlaneUrl, isConnectionRefused, printTable, displayState, output } from '../config';
import { ApiClient } from '../client';
import { idToSlug, parseInputId } from '@skyrepl/contracts';

export async function instanceCommand(args: string[]): Promise<void> {
  const subcommand = args[0];

  switch (subcommand) {
    case 'list':
      await instanceList();
      break;
    case 'terminate':
      await instanceTerminate(args.slice(1));
      break;
    default:
      console.error('Usage: repl instance <list|terminate>');
      process.exit(2);
  }
}

async function instanceList(): Promise<void> {
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data } = await client.listInstances();

    if (data.length === 0) {
      output([], () => { console.log('No instances found.'); }, 'instance');
      return;
    }

    const rows = data.map((inst: any) => {
      const created = inst.created_at ? new Date(inst.created_at).toISOString().slice(0, 16).replace('T', ' ') : '-';
      return [
        inst.display_name || idToSlug(inst.id),
        inst.provider || '-',
        inst.spec || '-',
        displayState(inst.workflow_state),
        inst.ip || '-',
        created,
      ];
    });

    output(data, () => {
      printTable(['NAME', 'PROVIDER', 'SPEC', 'STATE', 'IP', 'CREATED'], rows, [20, 10, 10, 14, 18, 16]);
    }, 'instance');
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to list instances: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}

async function instanceTerminate(args: string[]): Promise<void> {
  const idArg = args[0];
  if (!idArg) {
    console.error('Usage: repl instance terminate <instance-id>');
    process.exit(2);
  }

  const instanceId = parseInputId(idArg);

  const client = new ApiClient(getControlPlaneUrl());
  try {
    const result = await client.terminateInstance(instanceId);
    console.log(`Terminating instance ${idToSlug(instanceId)}... workflow ${idToSlug(result.workflow_id)}`);
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to terminate instance: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}
