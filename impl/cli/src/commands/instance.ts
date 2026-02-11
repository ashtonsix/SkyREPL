import { getControlPlaneUrl, isConnectionRefused, printTable, displayState } from '../config';
import { ApiClient } from '../client';

export async function instanceCommand(args: string[]): Promise<void> {
  const subcommand = args[0];

  switch (subcommand) {
    case 'list':
      await instanceList();
      break;
    default:
      console.error('Usage: repl instance <list>');
      process.exit(2);
  }
}

async function instanceList(): Promise<void> {
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data } = await client.listInstances();

    if (data.length === 0) {
      console.log('No instances found.');
      return;
    }

    const rows = data.map((inst: any) => {
      const created = inst.created_at ? new Date(inst.created_at).toISOString().slice(0, 16).replace('T', ' ') : '-';
      return [
        String(inst.id),
        inst.provider || '-',
        inst.spec || '-',
        displayState(inst.workflow_state),
        inst.ip || '-',
        created,
      ];
    });

    printTable(['ID', 'PROVIDER', 'SPEC', 'STATE', 'IP', 'CREATED'], rows, [4, 10, 10, 14, 18, 16]);
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to list instances: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}
