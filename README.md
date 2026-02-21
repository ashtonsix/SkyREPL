# SkyREPL

Cloud compute orchestration that tracks every resource from spawn to destruction. Run jobs across multiple cloud providers through a single interface. Every VM spawned is tracked, monitored, and guaranteed to be cleaned up. No zombie instances, no surprise bills.

Source-available under the [Business Source License 1.1](scaffold/legal/BSL-1.1.txt).

## Quickstart

```bash
bun install
export PATH="$PWD/shell/cli/bin:$PATH"
repl control start
repl run -c "echo hello"
```

## Cloud Setup

```bash
repl setup                        # Configure tunnel + SSH
repl run -c "echo hello" --provider aws
ssh repl-<allocation-id>          # SSH into your VM
```

Provider setup guides: [AWS](docs/providers/aws-setup.md) | [OrbStack](docs/providers/orbstack-setup.md)

## Architecture

See [ARCHITECTURE.txt](ARCHITECTURE.txt) for technical orientation.

| Zone         | Purpose          | Key components                                     |
| ------------ | ---------------- | -------------------------------------------------- |
| `contracts/` | Cross-zone types | TypeBox schemas, error hierarchy, timing constants |
| `lifecycle/` | The engine       | Control plane, agent, providers                    |
| `shell/`     | The interface    | CLI, proxy server, daemon                          |
| `orbital/`   | The brain        | Advisory/decision API                              |
| `scaffold/`  | Ground crew      | Scripts, demo, legal                               |

Stack: Bun, bun:sqlite (WAL), ElysiaJS, TypeBox, Python agent.

## Key Commands

- `repl run -c "cmd"` — Launch a run
- `repl run list` — List runs
- `repl run attach <id>` — Stream logs from a running job
- `repl run cancel <id>` — Cancel a workflow
- `repl instance list` — List instances
- `repl allocation list` — List allocations
- `repl setup` — First-run configuration
- `repl ssh-config` — Regenerate SSH config
- `repl control start|stop|status` — Manage control plane

## Development

```bash
cd lifecycle && bun test    # Run tests (~945 tests, <5s)
repl control start               # Start the control plane
```

## Project Status

Early access. AWS + OrbStack providers active. Multi-tenant with API key auth.
