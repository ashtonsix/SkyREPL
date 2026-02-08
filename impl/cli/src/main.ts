#!/usr/bin/env bun

import { runCommand } from './commands/run';

function printUsage() {
  console.error('Usage: repl <command> [options]');
  console.error('');
  console.error('Commands:');
  console.error('  run [command]    Launch a run (Slice 1 only)');
  console.error('');
  process.exit(1);
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0) {
    printUsage();
  }

  const command = args[0];
  const commandArgs = args.slice(1);

  if (command === 'run') {
    await runCommand(commandArgs);
  } else {
    console.error(`Unknown command: ${command}`);
    printUsage();
  }
}

main().catch((error) => {
  console.error('Fatal error:', error.message);
  process.exit(1);
});
