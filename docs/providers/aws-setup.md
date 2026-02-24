# AWS Provider Setup

SkyREPL's AWS provider targets EC2 on Ubuntu (noble/jammy/focal), supports spot instances, snapshots (AMIs), and multi-region operation.

## Prerequisites

- An AWS account with EC2 access
- AWS CLI installed (`brew install awscli` or https://aws.amazon.com/cli/)
- Credentials configured — either environment variables or `aws configure`
- Control plane URL reachable from the internet — AWS VMs connect back to
  the control plane for agent registration and SSE heartbeat. Set
  `SKYREPL_CONTROL_PLANE_URL` to a public address (not `localhost` or
  `127.0.0.1`). Run `repl setup` to configure this.

## IAM Policy

Create an IAM user or role with the following policy. These are the exact EC2 and STS actions the provider uses:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:RunInstances",
        "ec2:TerminateInstances",
        "ec2:DescribeInstances",
        "ec2:DescribeImages",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeKeyPairs",
        "ec2:CreateSecurityGroup",
        "ec2:AuthorizeSecurityGroupIngress",
        "ec2:CreateKeyPair",
        "ec2:CreateTags",
        "sts:GetCallerIdentity"
      ],
      "Resource": "*"
    }
  ]
}
```

For snapshots (AMIs), add:

```json
"ec2:CreateImage",
"ec2:DeregisterImage",
"ec2:DeleteSnapshot"
```

For spot instances, add:

```json
"ec2:RequestSpotInstances",
"ec2:CancelSpotInstanceRequests",
"ec2:DescribeSpotPriceHistory"
```

### Creating an IAM user (console)

1. Go to IAM > Users > Create user
2. Select "Attach policies directly" > Create inline policy
3. Paste the JSON above
4. Note the Access Key ID and Secret Access Key

## Environment Variables

```bash
# Required (or use ~/.aws/credentials via `aws configure`)
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1

# Optional: Tailscale SSH (see below)
export TAILSCALE_API_KEY=tskey-api-...
export TAILSCALE_TAILNET=your-tailnet.ts.net
```

Alternatively, run `aws configure` to write `~/.aws/credentials`. The provider uses the standard AWS credential chain — environment variables take precedence.

## Automatic Setup

On first use, the provider runs `startup()` which:

1. Calls `sts:GetCallerIdentity` to verify credentials
2. Creates a security group named `skyrepl-default` in the target region (if one doesn't exist), with SSH ingress on port 22 from `0.0.0.0/0` and `::/0`
3. Creates an EC2 key pair named `skyrepl-{region}` (if one doesn't exist) and saves the PEM to `~/.repl/keys/aws-{region}.pem`

This happens automatically on control plane startup — no manual steps required.

## Quick Start

```bash
# 1. Set credentials
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1

# 2. Launch a test run
repl run --provider aws --spec "ubuntu:noble:amd64" --command "echo hello world" --max-duration 300000

# 3. Check status
repl run list
```

The spec format is either an EC2 instance type (e.g. `t4g.nano`, `c8g.large`) or `distro:version:arch`. Supported values for the OS descriptor format:
- Distro: `ubuntu`
- Version: `noble` (24.04), `jammy` (22.04), `focal` (20.04)
- Arch: `amd64`, `arm64`

When an instance type is used as the spec, the architecture is inferred from the instance family (Graviton families like `c8g`, `t4g` → arm64, others → amd64). When an OS descriptor is used, instance types default to `t3.micro` (amd64) or `t4g.micro` (arm64).

## Spot Instances

Add `--spot` to use spot pricing — typically 60–90% cheaper for fault-tolerant workloads:

```bash
repl run --provider aws --spec "ubuntu:noble:amd64" --spot --command "python train.py" --max-duration 7200000
```

SkyREPL uses `RunInstances` with `InstanceMarketOptions.MarketType=spot` (not the legacy `RequestSpotInstances` path) when `--spot` is set without a separate spot request. The `requestSpotInstance()` method (persistent spot requests) is also available for the warm pool.

## Regions

The provider creates a separate EC2 client per region. To target a non-default region:

```bash
repl run --provider aws --spec "ubuntu:noble:amd64" --region eu-west-1 --command "echo hi"
```

The security group and key pair are created per-region on first use in that region.

## SSH Access

SSH keys are stored at `~/.repl/keys/aws-{region}.pem`. The provider tags each key pair as `skyrepl-{region}`.

### With Tailscale (recommended)

If `TAILSCALE_API_KEY` and `TAILSCALE_TAILNET` are set, instances join the tailnet during bootstrap. SSH then goes over Tailscale — no public port 22 exposure needed. The `skyrepl-default` security group is still created (for non-Tailscale workflows), but you can restrict its SSH rule to your own IP or remove it entirely if you use Tailscale exclusively.

## Troubleshooting

**AccessDenied / UnauthorizedAccess**
Check that the IAM policy includes all required actions. Run `aws sts get-caller-identity` to confirm the credentials are valid and identify which user/role is active.

**InsufficientInstanceCapacity**
EC2 capacity is AZ-specific. Try a different instance type or region. The error is retryable — SkyREPL backs off 30s automatically.

**VcpuLimitExceeded**
Your account has a per-region vCPU quota. Request an increase at: EC2 console > Limits > Request limit increase.

**Instances left running**
```bash
repl instance list              # show running instances
repl run cancel <run-id>        # terminate via SkyREPL
```

SkyREPL tags all instances with `skyrepl:managed=true`. To find orphaned instances directly:
```bash
aws ec2 describe-instances --filters "Name=tag:skyrepl:managed,Values=true" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name,Tags[?Key==`Name`].Value|[0]]' \
  --output table
```

**AMI resolution fails**
The provider resolves the latest Ubuntu AMI from Canonical's account (`099720109477`) and caches the result for 24 hours at `~/.cache/skyrepl/`. Delete the cache directory to force a fresh lookup.
