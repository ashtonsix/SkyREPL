#!/usr/bin/env bash
# ssh_fallback.sh — Catch-all ProxyCommand for unknown repl-* SSH hosts.
#
# Invoked by the catch-all `Host repl-*` entry in ~/.repl/ssh_config when
# no specific Host entry matches. This happens when:
#   - SSH config is stale (allocation state changed since last regeneration)
#   - User typed `ssh repl-<slug>` for an allocation not yet in the config
#
# Usage (via SSH ProxyCommand):
#   ProxyCommand ~/.repl/bin/ssh_fallback.sh %n
#
# Exit codes:
#   0  — connected (exec nc to instance IP)
#   1  — allocation not found or not SSH-accessible (user error)
#   2  — system error (network, auth, API)

set -euo pipefail

HOSTNAME="${1:-}"
if [[ -z "$HOSTNAME" ]]; then
  echo "ssh_fallback.sh: missing hostname argument" >&2
  exit 2
fi

# Strip the repl- prefix to get the slug
SLUG="${HOSTNAME#repl-}"

# Validate slug: base36 alphanumeric or display-name (hyphenated lowercase)
if ! [[ "$SLUG" =~ ^[0-9a-z]([0-9a-z-]*[0-9a-z])?$ ]]; then
  echo "ssh_fallback.sh: invalid slug format '${SLUG}' (expected base36 or display-name format)" >&2
  exit 1
fi

CONTROL_PLANE_URL="${SKYREPL_CONTROL_PLANE_URL:-http://localhost:3000}"
API_KEY_FILE="${HOME}/.repl/api-key"

# Load API key if available
API_KEY=""
if [[ -f "$API_KEY_FILE" ]]; then
  API_KEY="$(cat "$API_KEY_FILE" 2>/dev/null || true)"
fi

# Build auth header
AUTH_HEADER=""
if [[ -n "$API_KEY" ]]; then
  AUTH_HEADER="Authorization: Bearer ${API_KEY}"
fi

# Query the API for the matching allocation by slug
if [[ -n "$AUTH_HEADER" ]]; then
  RESPONSE=$(curl -s --max-time 10 \
    -H "$AUTH_HEADER" \
    "${CONTROL_PLANE_URL}/v1/allocations?slug=${SLUG}" 2>/dev/null) || {
    echo "Error: Cannot reach SkyREPL control plane. Is it running? Try: repl control start" >&2
    exit 1
  }
else
  RESPONSE=$(curl -s --max-time 10 \
    "${CONTROL_PLANE_URL}/v1/allocations?slug=${SLUG}" 2>/dev/null) || {
    echo "Error: Cannot reach SkyREPL control plane. Is it running? Try: repl control start" >&2
    exit 1
  }
fi

# Validate we got a non-empty response
if [[ -z "$RESPONSE" ]]; then
  echo "Error: Cannot reach SkyREPL control plane. Is it running? Try: repl control start" >&2
  exit 1
fi

# Validate response is JSON (not an HTML error page or other non-JSON body)
if ! echo "$RESPONSE" | grep -q '^[[:space:]]*[{[]'; then
  echo "Error: Cannot reach SkyREPL control plane. Is it running? Try: repl control start" >&2
  exit 1
fi

# Check for API error
if echo "$RESPONSE" | grep -q '"error"'; then
  MSG=$(echo "$RESPONSE" | grep -o '"message":"[^"]*"' | head -1 | sed 's/"message":"//;s/"//')
  echo "ssh_fallback.sh: API error: ${MSG:-unknown}" >&2
  exit 2
fi

# Count matching allocations
# Parse data array length from JSON without jq dependency
DATA_COUNT=$(echo "$RESPONSE" | grep -o '"id":[0-9]*' | wc -l | tr -d ' ')

NOW_MS=$(date +%s%3N 2>/dev/null || echo "0")

if [[ "$DATA_COUNT" -eq 0 ]]; then
  echo "ssh_fallback.sh: No allocation found matching 'repl-${SLUG}'" >&2
  echo "" >&2
  echo "  Run 'repl allocation list' to see active allocations." >&2
  echo "  Run 'repl ssh-config' to regenerate your SSH config." >&2
  exit 1
fi

if [[ "$DATA_COUNT" -gt 1 ]]; then
  echo "ssh_fallback.sh: Ambiguous slug '${SLUG}' — multiple allocations matched." >&2
  echo "  Use the full allocation slug to disambiguate." >&2
  echo "  Run 'repl allocation list' to see all allocations." >&2
  exit 1
fi

# Exactly one allocation — extract fields
ALLOC_STATUS=$(echo "$RESPONSE" | grep -o '"status":"[^"]*"' | head -1 | sed 's/"status":"//;s/"//')
ALLOC_HOLD=$(echo "$RESPONSE" | grep -o '"debug_hold_until":[0-9]*' | head -1 | sed 's/"debug_hold_until"://')
INSTANCE_IP=$(echo "$RESPONSE" | grep -o '"instance_ip":"[^"]*"' | head -1 | sed 's/"instance_ip":"//;s/"//')

# Check SSH accessibility
SSH_OK=0
if [[ "$ALLOC_STATUS" == "ACTIVE" ]]; then
  SSH_OK=1
elif [[ "$ALLOC_STATUS" == "COMPLETE" && -n "$ALLOC_HOLD" && "$ALLOC_HOLD" != "null" ]]; then
  if [[ "$ALLOC_HOLD" -gt "$NOW_MS" ]]; then
    SSH_OK=1
  fi
fi

if [[ "$SSH_OK" -eq 0 ]]; then
  echo "ssh_fallback.sh: Allocation 'repl-${SLUG}' is ${ALLOC_STATUS}, SSH not available." >&2
  if [[ "$ALLOC_STATUS" == "COMPLETE" ]]; then
    echo "  The debug hold has expired. Use 'repl extend ${SLUG}' to restore access." >&2
  fi
  exit 1
fi

# Check we have an IP
if [[ -z "$INSTANCE_IP" || "$INSTANCE_IP" == "null" ]]; then
  echo "ssh_fallback.sh: No IP address available for allocation 'repl-${SLUG}'." >&2
  echo "  The instance may still be provisioning. Try again shortly." >&2
  exit 1
fi

# Warn about stale config and suggest regeneration
echo "Warning: SSH config is stale. Run 'repl ssh-config' to update." >&2

# Connect via nc — SSH reads/writes stdin/stdout of ProxyCommand
exec nc "$INSTANCE_IP" 22
