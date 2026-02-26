// commands/setup.ts — First-run setup and configuration
//
// Handles: control plane URL, SSH config Include, API key verification.
// Critical for cloud provider E2E — remote agents need a reachable control plane.

import { createInterface } from 'readline';
import { existsSync, readFileSync, writeFileSync, mkdirSync, appendFileSync, chmodSync } from 'fs';
import { join, dirname } from 'path';
import { homedir } from 'os';
import { execSync, spawn } from 'child_process';
import { REPL_DIR, getControlPlaneUrl, getApiKey } from '../config';

// =============================================================================
// Prompting
// =============================================================================

function prompt(question: string): Promise<string> {
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

async function promptYN(question: string, defaultYes = true): Promise<boolean> {
  const hint = defaultYes ? '[Y/n]' : '[y/N]';
  const answer = await prompt(`${question} ${hint} `);
  if (answer === '') return defaultYes;
  return answer.toLowerCase().startsWith('y');
}

// =============================================================================
// Tunnel Data Model
// =============================================================================

export interface TunnelConfig {
  type: 'tailscale' | 'cloudflared' | 'ngrok' | 'manual';
  url: string;
  pid?: number;
}

function loadTunnels(): TunnelConfig[] {
  const tunnelsFile = join(REPL_DIR, 'tunnels.json');
  if (!existsSync(tunnelsFile)) return [];
  try {
    return JSON.parse(readFileSync(tunnelsFile, 'utf-8'));
  } catch {
    return [];
  }
}

function saveTunnel(tunnel: TunnelConfig): void {
  const tunnelsFile = join(REPL_DIR, 'tunnels.json');
  mkdirSync(REPL_DIR, { recursive: true });
  const tunnels = loadTunnels().filter(t => t.url !== tunnel.url);
  tunnels.push(tunnel);
  writeFileSync(tunnelsFile, JSON.stringify(tunnels, null, 2) + '\n', { mode: 0o600 });
}

// =============================================================================
// Tunnel Detection
// =============================================================================

interface TunnelTool {
  name: string;
  binary: string;
  installed: boolean;
  preferred: number; // lower = more preferred
}

function detectTunnelTools(): TunnelTool[] {
  const tools: TunnelTool[] = [
    { name: 'Tailscale Funnel', binary: 'tailscale', installed: false, preferred: 1 },
    { name: 'Cloudflare Tunnel', binary: 'cloudflared', installed: false, preferred: 2 },
    { name: 'ngrok', binary: 'ngrok', installed: false, preferred: 3 },
  ];

  for (const tool of tools) {
    try {
      execSync(`which ${tool.binary} 2>/dev/null`, { stdio: 'pipe' });
      tool.installed = true;
    } catch {
      // not installed
    }
  }

  return tools;
}

// =============================================================================
// Control Plane URL Setup
// =============================================================================

async function setupControlPlaneUrl(): Promise<string | null> {
  const envFile = join(REPL_DIR, 'control.env');
  const current = getControlPlaneUrl();
  const isDefault = current === 'http://localhost:3000';

  console.log('\nStep 1: Control Plane URL');
  console.log('  Your control plane must be reachable from remote VMs.');

  if (!isDefault) {
    console.log(`  Current: ${current}`);
    const keep = await promptYN('  Keep current URL?');
    if (keep) return current;
  } else {
    console.log('  Current: http://localhost:3000 (default — unreachable from cloud VMs)');
  }

  const tools = detectTunnelTools();
  const installed = tools.filter(t => t.installed).sort((a, b) => a.preferred - b.preferred);

  console.log('');
  console.log('  Options:');
  console.log('    1. Provide a public URL manually');
  if (installed.length > 0) {
    for (let i = 0; i < installed.length; i++) {
      console.log(`    ${i + 2}. Use ${installed[i].name} (detected)`);
    }
  }
  console.log(`    ${installed.length + 2}. Skip (local/OrbStack only)`);

  const choice = await prompt('  > ');
  const choiceNum = parseInt(choice, 10);

  let url: string | null = null;
  let tunnelType: TunnelConfig['type'] = 'manual';

  if (choiceNum === 1 || choice === '') {
    // Manual URL entry
    const entered = await prompt('  Enter public URL (e.g., https://my-tunnel.example.com): ');
    if (!entered) {
      console.log('  Skipped.');
      return null;
    }
    url = entered.replace(/\/$/, ''); // strip trailing slash
    tunnelType = 'manual';
  } else if (choiceNum >= 2 && choiceNum <= installed.length + 1) {
    const tool = installed[choiceNum - 2];
    url = await startTunnel(tool);
    tunnelType = tool.binary as TunnelConfig['type'];
  } else {
    console.log('  Skipped — using localhost (local providers only).');
    return null;
  }

  if (url) {
    // Test connectivity
    console.log(`  Testing connectivity to ${url}...`);
    try {
      const resp = await fetch(`${url}/v1/health`, { signal: AbortSignal.timeout(10000) });
      if (resp.ok) {
        console.log('  Connectivity verified.');
      } else {
        console.log(`  Warning: health check returned ${resp.status}. The control plane may not be running yet.`);
      }
    } catch {
      console.log('  Warning: could not reach control plane. Make sure it is running.');
    }

    // Save to control.env
    saveEnvVar(envFile, 'SKYREPL_CONTROL_PLANE_URL', url);
    saveEnvVar(envFile, 'SKYREPL_TUNNEL_TYPE', tunnelType);
    process.env.SKYREPL_CONTROL_PLANE_URL = url;
    // Save to tunnels array
    saveTunnel({ type: tunnelType, url });
    console.log(`  Saved to ${envFile}`);
  }

  return url;
}

async function startTunnel(tool: TunnelTool): Promise<string | null> {
  const port = parseInt(process.env.SKYREPL_PORT ?? process.env.PORT ?? '3000', 10);

  if (tool.binary === 'tailscale') {
    return startTailscaleFunnel(port);
  } else if (tool.binary === 'cloudflared') {
    return startCloudflaredTunnel(port);
  } else if (tool.binary === 'ngrok') {
    return startNgrokTunnel(port);
  }

  return null;
}

async function startTailscaleFunnel(port: number): Promise<string | null> {
  console.log(`  Starting Tailscale Funnel on port ${port}...`);

  try {
    // Get the current tailscale status to find the machine's DNS name
    const statusJson = execSync('tailscale status --json 2>/dev/null', { encoding: 'utf-8' });
    const status = JSON.parse(statusJson);
    const dnsName = status.Self?.DNSName?.replace(/\.$/, '');

    if (!dnsName) {
      console.log('  Error: Could not determine Tailscale DNS name. Is Tailscale logged in?');
      return null;
    }

    // Start funnel in background
    // tailscale funnel --bg <port> enables HTTPS on the machine's DNS name
    execSync(`tailscale funnel --bg ${port}`, { stdio: 'pipe' });

    const url = `https://${dnsName}`;
    console.log(`  Tailscale Funnel started: ${url}`);
    return url;
  } catch (err) {
    console.log(`  Failed to start Tailscale Funnel: ${err instanceof Error ? err.message : err}`);
    console.log('  Make sure Tailscale is logged in (`tailscale login`) and Funnel is enabled.');
    const manual = await prompt('  Enter URL manually instead, or press Enter to skip: ');
    return manual || null;
  }
}

async function startCloudflaredTunnel(port: number): Promise<string | null> {
  console.log(`  Starting Cloudflare Tunnel on port ${port}...`);
  console.log('  (Using quick tunnel — no account required)');

  try {
    // cloudflared quick tunnel gives a random *.trycloudflare.com URL
    // We need to capture stdout to get the URL
    const proc = spawn('cloudflared', ['tunnel', '--url', `http://localhost:${port}`], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    // Wait for the URL to appear in output
    const url = await new Promise<string | null>((resolve) => {
      let output = '';
      const timeout = setTimeout(() => resolve(null), 30000);

      const handler = (data: Buffer) => {
        output += data.toString();
        const match = output.match(/https:\/\/[^\s]+\.trycloudflare\.com/);
        if (match) {
          clearTimeout(timeout);
          resolve(match[0]);
        }
      };

      proc.stdout?.on('data', handler);
      proc.stderr?.on('data', handler);
    });

    if (url) {
      // Keep tunnel running in background
      proc.unref();
      console.log(`  Cloudflare Tunnel started: ${url}`);
      // Save PID for later cleanup and record in tunnels array
      const pidFile = join(REPL_DIR, 'tunnel.pid');
      writeFileSync(pidFile, String(proc.pid));
      saveTunnel({ type: 'cloudflared', url, pid: proc.pid });
      return url;
    } else {
      console.log('  Timed out waiting for tunnel URL.');
      proc.kill();
      return null;
    }
  } catch (err) {
    console.log(`  Failed to start tunnel: ${err instanceof Error ? err.message : err}`);
    return null;
  }
}

async function startNgrokTunnel(port: number): Promise<string | null> {
  console.log(`  Starting ngrok tunnel on port ${port}...`);

  try {
    const proc = spawn('ngrok', ['http', String(port), '--log=stdout', '--log-format=json'], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    const url = await new Promise<string | null>((resolve) => {
      let output = '';
      const timeout = setTimeout(() => resolve(null), 30000);

      proc.stdout?.on('data', (data: Buffer) => {
        output += data.toString();
        // ngrok JSON log contains the public URL
        for (const line of output.split('\n')) {
          try {
            const log = JSON.parse(line);
            if (log.url && log.url.startsWith('https://')) {
              clearTimeout(timeout);
              resolve(log.url);
              return;
            }
          } catch { /* not JSON */ }
        }
      });
    });

    if (url) {
      proc.unref();
      console.log(`  ngrok tunnel started: ${url}`);
      const pidFile = join(REPL_DIR, 'tunnel.pid');
      writeFileSync(pidFile, String(proc.pid));
      saveTunnel({ type: 'ngrok', url, pid: proc.pid });
      return url;
    } else {
      console.log('  Timed out waiting for ngrok URL.');
      proc.kill();
      return null;
    }
  } catch (err) {
    console.log(`  Failed to start ngrok: ${err instanceof Error ? err.message : err}`);
    return null;
  }
}

// =============================================================================
// SSH Config Include
// =============================================================================

async function setupSSHConfigInclude(): Promise<void> {
  console.log('\nStep 2: SSH Config');

  const sshDir = join(homedir(), '.ssh');
  const sshConfig = join(sshDir, 'config');
  const replSSHConfig = join(REPL_DIR, 'ssh_config');
  const includeLine = `Include ${replSSHConfig}`;

  // Ensure ~/.repl/ssh_config exists (even if empty)
  if (!existsSync(replSSHConfig)) {
    mkdirSync(REPL_DIR, { recursive: true });
    writeFileSync(replSSHConfig, '# SkyREPL SSH config — auto-generated, do not edit manually\n# Regenerate with: repl ssh-config\n');
    chmodSync(replSSHConfig, 0o600);
  }

  // Check if Include already present
  if (existsSync(sshConfig)) {
    const content = readFileSync(sshConfig, 'utf-8');
    if (content.includes(replSSHConfig)) {
      console.log(`  Include already present in ${sshConfig}`);
      return;
    }
  }

  const add = await promptYN(`  Add \`${includeLine}\` to ${sshConfig}?`);
  if (!add) {
    console.log(`  Skipped. Add manually to ${sshConfig}:`);
    console.log(`    ${includeLine}`);
    return;
  }

  // Include must be at the top of ssh_config
  mkdirSync(sshDir, { recursive: true });

  if (existsSync(sshConfig)) {
    const existing = readFileSync(sshConfig, 'utf-8');
    writeFileSync(sshConfig, `${includeLine}\n\n${existing}`);
  } else {
    writeFileSync(sshConfig, `${includeLine}\n`, { mode: 0o600 });
  }

  console.log(`  Added to ${sshConfig}`);
}

// =============================================================================
// API Key Verification
// =============================================================================

function verifyApiKey(): void {
  console.log('\nStep 3: API Key');

  const key = getApiKey();
  if (key) {
    console.log('  API key found at ~/.repl/api-key');
  } else {
    console.log('  No API key found. Start the control plane with `repl control start` to auto-generate one.');
  }
}

// =============================================================================
// Helpers
// =============================================================================

function saveEnvVar(envFile: string, key: string, value: string): void {
  mkdirSync(dirname(envFile), { recursive: true });

  if (existsSync(envFile)) {
    const content = readFileSync(envFile, 'utf-8');
    const lines = content.split('\n');
    let found = false;
    const updated = lines.map((line) => {
      if (line.startsWith(`${key}=`)) {
        found = true;
        return `${key}=${value}`;
      }
      return line;
    });
    if (!found) updated.push(`${key}=${value}`);
    writeFileSync(envFile, updated.join('\n'));
  } else {
    writeFileSync(envFile, `${key}=${value}\n`, { mode: 0o600 });
  }
}

// =============================================================================
// Subcommands
// =============================================================================

async function setupTailscale(): Promise<void> {
  console.log('Tailscale Setup');
  console.log('===============\n');

  try {
    execSync('which tailscale', { stdio: 'pipe' });
  } catch {
    console.log('Tailscale is not installed.');
    console.log('Install it from: https://tailscale.com/download');
    process.exit(1);
  }

  // Check if logged in
  try {
    const statusJson = execSync('tailscale status --json 2>/dev/null', { encoding: 'utf-8' });
    const status = JSON.parse(statusJson);
    if (status.BackendState === 'Running') {
      console.log(`Tailscale is running (${status.Self?.DNSName?.replace(/\.$/, '')})`);
    } else {
      console.log('Tailscale is installed but not logged in.');
      console.log('Run: tailscale login');
      process.exit(1);
    }
  } catch {
    console.log('Tailscale is installed but not responding.');
    console.log('Run: sudo tailscale up');
    process.exit(1);
  }

  // Set up funnel for control plane
  const port = parseInt(process.env.SKYREPL_PORT ?? process.env.PORT ?? '3000', 10);
  const url = await startTailscaleFunnel(port);
  if (url) {
    const envFile = join(REPL_DIR, 'control.env');
    saveEnvVar(envFile, 'SKYREPL_CONTROL_PLANE_URL', url);
    saveEnvVar(envFile, 'SKYREPL_TUNNEL_TYPE', 'tailscale');
    process.env.SKYREPL_CONTROL_PLANE_URL = url;
    saveTunnel({ type: 'tailscale', url });
    console.log(`\nControl plane URL saved: ${url}`);
    console.log('Remote agents will connect via this URL.');
  }
}

// =============================================================================
// Entry Point
// =============================================================================

export async function setupCommand(args: string[]): Promise<void> {
  // Subcommands
  if (args[0] === 'tailscale') {
    await setupTailscale();
    return;
  }
  if (args[0] === 'providers') {
    await setupProviders();
    return;
  }

  console.log('SkyREPL Setup');
  console.log('=============');

  await setupControlPlaneUrl();
  await setupSSHConfigInclude();
  verifyApiKey();
  await detectProviders();

  console.log('\nSetup complete!');

  const url = getControlPlaneUrl();
  const isDefault = url === 'http://localhost:3000';

  if (isDefault) {
    console.log('Run `repl run -c "echo hello"` to test with OrbStack (local).');
  } else {
    console.log(`Run \`repl run -c "echo hello" --provider aws\` to test.`);
  }
}

// =============================================================================
// Provider Detection (D3)
// =============================================================================

interface DetectedProvider {
  name: string;
  detected: boolean;
  reason: string;
}

async function detectProviders(): Promise<void> {
  console.log('\nStep 4: Cloud Providers');

  const detected: DetectedProvider[] = [];

  // OrbStack: check for orbctl binary
  try {
    execSync('which orbctl 2>/dev/null', { stdio: 'pipe' });
    detected.push({ name: 'orbstack', detected: true, reason: 'orbctl found' });
  } catch {
    detected.push({ name: 'orbstack', detected: false, reason: 'orbctl not found' });
  }

  // AWS: check for credentials
  if (process.env.AWS_ACCESS_KEY_ID || existsSync(join(homedir(), '.aws', 'credentials'))) {
    detected.push({ name: 'aws', detected: true, reason: 'credentials found' });
  } else {
    detected.push({ name: 'aws', detected: false, reason: 'no credentials (set AWS_ACCESS_KEY_ID or configure ~/.aws/credentials)' });
  }

  // Lambda Labs: check for API key
  if (process.env.LAMBDA_API_KEY) {
    detected.push({ name: 'lambda', detected: true, reason: 'LAMBDA_API_KEY set' });
  } else {
    detected.push({ name: 'lambda', detected: false, reason: 'LAMBDA_API_KEY not set' });
  }

  // DigitalOcean: check for token
  if (process.env.DIGITALOCEAN_TOKEN) {
    detected.push({ name: 'digitalocean', detected: true, reason: 'DIGITALOCEAN_TOKEN set' });
  } else {
    detected.push({ name: 'digitalocean', detected: false, reason: 'DIGITALOCEAN_TOKEN not set' });
  }

  const available = detected.filter(p => p.detected);
  const unavailable = detected.filter(p => !p.detected);

  if (available.length > 0) {
    console.log('  Available:');
    for (const p of available) {
      console.log(`    - ${p.name} (${p.reason})`);
    }
  }

  if (unavailable.length > 0) {
    console.log('  Not configured:');
    for (const p of unavailable) {
      console.log(`    - ${p.name}: ${p.reason}`);
    }
  }

  // Offer to write providers.toml
  const configPath = join(REPL_DIR, 'providers.toml');
  if (available.length > 0 && !existsSync(configPath)) {
    const write = await promptYN('\n  Write providers.toml config?');
    if (write) {
      writeProvidersToml(configPath, detected);
      console.log(`  Saved to ${configPath}`);
    }
  }
}

async function setupProviders(): Promise<void> {
  console.log('Provider Setup');
  console.log('==============\n');
  await detectProviders();
}

function writeProvidersToml(configPath: string, detected: DetectedProvider[]): void {
  mkdirSync(dirname(configPath), { recursive: true });

  const lines: string[] = [
    '# SkyREPL Provider Configuration',
    '# Generated by `repl setup`. Edit as needed.',
    '# Values here override environment variables.',
    '',
  ];

  for (const p of detected) {
    lines.push(`[${p.name}]`);
    lines.push(`enabled = ${p.detected}`);

    switch (p.name) {
      case 'aws':
        lines.push(`# region = "us-east-1"`);
        lines.push(`# access_key_id = ""`);
        lines.push(`# secret_access_key = ""`);
        break;
      case 'lambda':
        lines.push(`# api_key = ""`);
        lines.push(`# region = "us-east-1"`);
        lines.push(`# ssh_key_name = ""`);
        break;
      case 'digitalocean':
        lines.push(`# token = ""`);
        lines.push(`# region = "nyc3"`);
        break;
      case 'orbstack':
        // No config needed
        break;
    }

    lines.push('');
  }

  writeFileSync(configPath, lines.join('\n'), { mode: 0o600 });
}
