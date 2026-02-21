# Providers

SkyREPL abstracts cloud compute behind a single `Provider` interface. Every provider implements the same contract — spawn, terminate, list, get, generateBootstrap — with optional methods gated by capability flags. The control plane never has provider-specific logic; providers are interchangeable.

See `docs/lifecycle/providers.txt` for the provider interface and implementation guide.

## Available Providers

| Provider  | Type       | Use case                          | Guide                                |
|-----------|------------|-----------------------------------|--------------------------------------|
| `aws`     | Cloud      | Production workloads, spot, AMIs  | [aws-setup.md](./aws-setup.md)       |
| `orbstack`| Local VM   | Development and E2E testing       | [orbstack-setup.md](./orbstack-setup.md) |

## Capability Summary

| Capability         | AWS | OrbStack |
|--------------------|-----|----------|
| Snapshots          | Y   | Y        |
| Spot instances     | Y   | N        |
| GPU                | Y   | N        |
| Multi-region       | Y   | N        |
| Persistent volumes | Y   | N        |
| Warm volumes       | Y   | N        |
| Hibernation        | Y   | N        |
| Cost explorer      | Y   | N        |
| Tailscale native   | N   | Y        |
| Idempotent spawn   | Y   | Y        |
| Custom networking  | Y   | N        |

## Selecting a Provider

Pass `--provider` to any run command:

```bash
repl run --provider aws     --spec "ubuntu:noble:amd64" --command "..." --max-duration 300000
repl run --provider orbstack --spec "ubuntu:noble:arm64" --command "..." --max-duration 60000
```

To see all options: `repl run --help`

## Provider Tiers

SkyREPL organizes providers into three implementation tiers:

**Testing (OrbStack):** The foundation of contract-based testing. Validates that the control plane handles intent contracts correctly — any cloud provider implementing the same contracts inherits that validation. ~700 LOC.

**Additional (Lambda Labs, RunPod, future: GCP, CoreWeave, SLURM):** Core interface methods plus basic lifecycle hooks. No complex platform integration required. ~500 LOC each.

**Reference (AWS):** Full feature depth — enhanced types, materialization functions, spot fleet, multi-AZ, snapshot management, full lifecycle hooks. ~3000 LOC. AWS is first-mover, not privileged; any provider gets the same depth where appropriate.

## Naming Convention

Every SkyREPL-managed instance is named:

```
repl-{control_id}-{manifest_id}-{instance_id}
```

This naming survives total DB loss. The orphan scanner identifies any `repl-` resource without a corresponding DB record, even across provider restarts.
