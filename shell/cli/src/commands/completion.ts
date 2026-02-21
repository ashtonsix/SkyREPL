// commands/completion.ts — Shell completion script generator command
//
// `repl completion zsh|bash` prints a script to stdout.
// `repl completion install` writes the script to a file and adds a source line
// to the user's shell profile.

import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';
import { generateZshScript, generateBashScript } from '../completions';

export async function completionCommand(args: string[]): Promise<void> {
  const sub = args[0];
  // Resolve CLI path for embedding in completion scripts
  const cliPath = process.argv[1] || 'repl';

  switch (sub) {
    case 'zsh':
      console.log(generateZshScript(cliPath));
      break;
    case 'bash':
      console.log(generateBashScript(cliPath));
      break;
    case 'fish':
      console.log('# Fish completions not yet implemented');
      break;
    case 'install':
      await installCompletions(cliPath);
      break;
    default:
      console.error('Usage: repl completion <bash|zsh|fish|install>');
      process.exit(2);
  }
}

async function installCompletions(cliPath: string): Promise<void> {
  const shell = detectShell();
  const replDir = join(homedir(), '.repl');
  mkdirSync(replDir, { recursive: true });

  if (shell === 'zsh') {
    const scriptPath = join(replDir, 'completions.zsh');
    writeFileSync(scriptPath, generateZshScript(cliPath));
    const sourceLine = `[ -f "${scriptPath}" ] && source "${scriptPath}"`;
    const rcPath = join(homedir(), '.zshrc');
    if (addLineToFile(rcPath, sourceLine, scriptPath)) {
      console.log(`Wrote completion script to ${scriptPath}`);
      console.log(`Added source line to ${rcPath}`);
    } else {
      console.log(`Completion script updated: ${scriptPath}`);
      console.log(`${rcPath} already sources it.`);
    }
  } else if (shell === 'bash') {
    const scriptPath = join(replDir, 'completions.bash');
    writeFileSync(scriptPath, generateBashScript(cliPath));
    const sourceLine = `[ -f "${scriptPath}" ] && source "${scriptPath}"`;
    const rcPath = join(homedir(), '.bashrc');
    if (addLineToFile(rcPath, sourceLine, scriptPath)) {
      console.log(`Wrote completion script to ${scriptPath}`);
      console.log(`Added source line to ${rcPath}`);
    } else {
      console.log(`Completion script updated: ${scriptPath}`);
      console.log(`${rcPath} already sources it.`);
    }
  } else {
    console.error(`Unsupported shell: ${shell}. Use \`repl completion zsh\` or \`repl completion bash\` manually.`);
    process.exit(2);
  }

  console.log('Restart your shell or run: exec $SHELL');
}

function detectShell(): string {
  const shell = process.env.SHELL || '';
  if (shell.endsWith('/zsh')) return 'zsh';
  if (shell.endsWith('/bash')) return 'bash';
  if (shell.endsWith('/fish')) return 'fish';
  return shell.split('/').pop() || 'unknown';
}

/** Append sourceLine to rcPath if not already present. Returns true if line was added. */
function addLineToFile(rcPath: string, sourceLine: string, scriptPath: string): boolean {
  let content = '';
  if (existsSync(rcPath)) {
    content = readFileSync(rcPath, 'utf-8');
  }
  // Check if the rc file already references the script path
  if (content.includes(scriptPath)) {
    return false;
  }
  const nl = content.length > 0 && !content.endsWith('\n') ? '\n' : '';
  writeFileSync(rcPath, content + nl + '\n# repl shell completions\n' + sourceLine + '\n');
  return true;
}
