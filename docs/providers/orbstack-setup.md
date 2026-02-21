# OrbStack Provider Setup

The OrbStack provider runs lightweight Linux VMs on macOS using OrbStack. It's the default for local development and E2E testing of SkyREPL itself — not for production workloads.

## Prerequisites

- macOS (OrbStack is macOS-only)
- OrbStack installed: https://orbstack.dev
- `orbctl` available in PATH (included with OrbStack)

## Verify Installation

```bash
orbctl version
orbctl list
```

If `orbctl` is not found, reinstall OrbStack from https://orbstack.dev.

## No Configuration Required

OrbStack needs no credentials or environment variables. The provider calls `orbctl` directly. On control plane startup, it runs `orbctl version` to verify the tool is available.

## Quick Start

```bash
# Launch a local run
repl run --provider orbstack --spec "ubuntu:noble:arm64" --command "echo hello" --max-duration 60000

# Check status
repl run list
```

The spec format is `distro:version:arch`. Supported values on OrbStack:
- Distro: `ubuntu` (default)
- Version: `noble` (24.04), `jammy` (22.04), `focal` (20.04)
- Arch: `arm64` (default on Apple Silicon), `amd64`

## How It Works

Each run gets its own OrbStack VM. The provider:

1. Calls `orbctl create {distro}:{version} {name} -a {arch}` to create the VM
2. Polls `orbctl info {name}` until state is `running`
3. Discovers the VM's IP via `orbctl info` → DNS (`{name}.orb.local`) → `orbctl run hostname -I` (fallback chain)
4. Executes bootstrap scripts via `orbctl run -m {name} -u root` if provided

VMs are named using the SkyREPL naming convention:

```
repl-{control_id}-{manifest_id}-{instance_id}
```

This naming survives total DB loss — the orphan scanner can identify and clean up any `repl-` VM even without a database record.

### Snapshots

OrbStack supports snapshots via VM cloning (`orbctl clone`). The cloned VM is stopped immediately to freeze it as a snapshot. This is available through `repl snapshot` commands.

### Tailscale

OrbStack has `tailscaleNative: true` — Tailscale is pre-integrated in OrbStack VMs and needs no extra configuration from SkyREPL's side.

## Limitations

- **macOS only** — not available on Linux or Windows
- **No spot instances** — OrbStack does not have spot pricing
- **No multi-region** — local only
- **No persistent volumes or warm volumes** — each VM is independent
- **No cost explorer** — OrbStack is free; no billing API
- **Single host** — all VMs share host CPU and memory; no resource limits per VM

OrbStack is the Testing-tier provider. It exists to validate the control plane against intent contracts so that any cloud provider implementing the same contracts inherits that validation.

## Troubleshooting

**"orbctl not found"**
Install or reinstall OrbStack from https://orbstack.dev. Ensure it has been launched at least once (OrbStack installs `orbctl` into PATH on first launch).

**"Missing IP address" / `orbctl create` fails with IP exhaustion**
OrbStack's internal DHCP pool can be exhausted by rapid create/delete cycles (leases expire after ~24h). Do NOT run recovery yourself. Exit this Claude session and run from the macOS host:

```bash
bash scaffold/scripts/orbstack-reset-networking.sh
```

Then resume your session.

**VMs not cleaning up**
SkyREPL terminates VMs via `repl run cancel <id>` or on timeout. To inspect and manually clean up:

```bash
orbctl list
orbctl delete -f repl-{control_id}-{manifest_id}-{instance_id}
```

The orphan scanner (`repl orphans`) also identifies any `repl-` VMs not tracked in the database.

**Bootstrap script not running**
Bootstrap failures are non-fatal. Check `/tmp/bootstrap.log` inside the VM:

```bash
orbctl run -m {vm-name} cat /tmp/bootstrap.log
```
