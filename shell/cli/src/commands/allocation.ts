import { getControlPlaneUrl, isConnectionRefused, printTable, output } from '../config';
import { ApiClient } from '../client';
import { idToSlug } from '@skyrepl/contracts';

export async function allocationCommand(args: string[]): Promise<void> {
  const subcommand = args[0];

  switch (subcommand) {
    case 'list':
      await allocationList();
      break;
    default:
      console.error('Usage: repl allocation <list>');
      process.exit(2);
  }
}

async function allocationList(): Promise<void> {
  const client = new ApiClient(getControlPlaneUrl());
  try {
    const { data } = await client.listAllocations();

    if (data.length === 0) {
      output([], () => { console.log('No allocations found.'); });
      return;
    }

    const rows = data.map((alloc: any) => {
      const created = alloc.created_at ? new Date(alloc.created_at).toISOString().slice(0, 16).replace('T', ' ') : '-';
      return [
        idToSlug(alloc.id),
        alloc.run_id != null ? idToSlug(alloc.run_id) : '-',
        idToSlug(alloc.instance_id),
        alloc.status || '-',
        created,
      ];
    });

    output(data, () => {
      printTable(['ID', 'RUN', 'INSTANCE', 'STATUS', 'CREATED'], rows, [4, 5, 10, 12, 16]);
    });
  } catch (err) {
    if (isConnectionRefused(err)) {
      console.error(`Control plane not reachable at ${getControlPlaneUrl()}. Start it with \`repl control start\`.`);
      process.exit(2);
    }
    console.error(`Failed to list allocations: ${err instanceof Error ? err.message : err}`);
    process.exit(2);
  }
}
