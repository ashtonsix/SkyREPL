#!/usr/bin/env bash
# ssh_proxy.sh — ProxyCommand for per-allocation SSH entries in ~/.repl/ssh_config.
#
# Invoked by SSH for each `Host repl-<slug>` entry. Handles JIT Tailscale
# installation and IP resolution, then hands off to nc for the actual tunnel.
#
# Flow:
#   1. Extract allocation slug from hostname argument (%h from SSH config)
#   2. Query GET /v1/allocations?slug=<slug> for instance_id + IP fields
#   3. If tailscale_status is "ready" → connect via tailscale_ip
#   4. If tailscale_status is "not_installed" or "failed" → trigger JIT install
#      via POST /v1/instances/:id/tailscale-ensure, then poll until ready or timeout
#   5. Fall back to public IP if Tailscale unavailable
#   6. exec nc <ip> 22
#
# Usage (via SSH ProxyCommand):
#   ProxyCommand ~/.repl/bin/ssh_proxy.sh %h
#
# Exit codes:
#   0  — connected (exec nc to instance IP)
#   1  — allocation not found, not SSH-accessible, or no IP available
#   2  — system error (network, auth, API)

set -euo pipefail

HOSTNAME="${1:-}"
if [[ -z "$HOSTNAME" ]]; then
  echo "ssh_proxy.sh: missing hostname argument" >&2
  exit 2
fi

# Strip the repl- prefix to get the allocation slug
SLUG="${HOSTNAME#repl-}"

# Validate slug: base36 alphanumeric or display-name (hyphenated lowercase)
if ! [[ "$SLUG" =~ ^[0-9a-z]([0-9a-z-]*[0-9a-z])?$ ]]; then
  echo "ssh_proxy.sh: invalid slug format '${SLUG}' (expected base36 or display-name format)" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_KEY_FILE="${HOME}/.repl/api-key"
CONTROL_ENV_FILE="${HOME}/.repl/control.env"

# Load control plane URL from control.env if present, else use env or default
CONTROL_PLANE_URL="${SKYREPL_CONTROL_PLANE_URL:-}"
if [[ -z "$CONTROL_PLANE_URL" && -f "$CONTROL_ENV_FILE" ]]; then
  # Source only SKYREPL_CONTROL_PLANE_URL from the env file
  CONTROL_PLANE_URL="$(grep -E '^SKYREPL_CONTROL_PLANE_URL=' "$CONTROL_ENV_FILE" 2>/dev/null \
    | head -1 | sed 's/^SKYREPL_CONTROL_PLANE_URL=//' | tr -d "'\"")" || true
fi
CONTROL_PLANE_URL="${CONTROL_PLANE_URL:-http://localhost:3000}"

# Load API key
API_KEY=""
if [[ -f "$API_KEY_FILE" ]]; then
  API_KEY="$(cat "$API_KEY_FILE" 2>/dev/null || true)"
fi

# Timing constants (match docs/shell/ssh.txt Reference > Timing table)
POLL_INTERVAL=2        # seconds between tailscale-status polls
INSTALL_TIMEOUT=120    # total seconds to wait for Tailscale install

# ---------------------------------------------------------------------------
# Helper: authenticated curl
# ---------------------------------------------------------------------------

api_get() {
  local url="$1"
  if [[ -n "$API_KEY" ]]; then
    curl -s --max-time 10 \
      -H "Authorization: Bearer ${API_KEY}" \
      "$url" 2>/dev/null
  else
    curl -s --max-time 10 "$url" 2>/dev/null
  fi
}

api_post() {
  local url="$1"
  if [[ -n "$API_KEY" ]]; then
    curl -s --max-time 10 \
      -X POST \
      -H "Authorization: Bearer ${API_KEY}" \
      -H "Content-Type: application/json" \
      -d '{}' \
      "$url" 2>/dev/null
  else
    curl -s --max-time 10 \
      -X POST \
      -H "Content-Type: application/json" \
      -d '{}' \
      "$url" 2>/dev/null
  fi
}

# ---------------------------------------------------------------------------
# Helper: validate JSON response
# ---------------------------------------------------------------------------

validate_response() {
  local resp="$1" context="$2"
  if [[ -z "$resp" ]] || ! echo "$resp" | grep -qE '^[[:space:]]*[{[]'; then
    echo "ssh_proxy.sh: Cannot reach SkyREPL control plane (${context}). Is it running? Try: repl control start" >&2
    exit 2
  fi
  if echo "$resp" | grep -q '"error"'; then
    local msg
    msg="$(echo "$resp" | grep -o '"message":"[^"]*"' | head -1 | sed 's/"message":"//;s/"//')"
    echo "ssh_proxy.sh: API error (${context}): ${msg:-unknown}" >&2
    exit 2
  fi
}

# ---------------------------------------------------------------------------
# Step 1: Resolve allocation → instance_id, public IP, tailscale fields
# ---------------------------------------------------------------------------

ALLOC_RESP="$(api_get "${CONTROL_PLANE_URL}/v1/allocations?slug=${SLUG}")" || {
  echo "ssh_proxy.sh: Cannot reach SkyREPL control plane. Is it running? Try: repl control start" >&2
  exit 2
}
validate_response "$ALLOC_RESP" "GET /v1/allocations"

if command -v jq >/dev/null 2>&1; then
  DATA_COUNT="$(echo "$ALLOC_RESP" | jq '.data | length' 2>/dev/null || echo "$ALLOC_RESP" | grep -o '"id":[0-9]*' | wc -l | tr -d ' ')"
else
  DATA_COUNT="$(echo "$ALLOC_RESP" | grep -o '"id":[0-9]*' | wc -l | tr -d ' ')"
fi

if [[ "$DATA_COUNT" -eq 0 ]]; then
  echo "ssh_proxy.sh: No allocation found for 'repl-${SLUG}'" >&2
  echo "  Run 'repl allocation list' to see active allocations." >&2
  exit 1
fi

if [[ "$DATA_COUNT" -gt 1 ]]; then
  echo "ssh_proxy.sh: Ambiguous slug '${SLUG}' — multiple allocations matched." >&2
  echo "  Use the full allocation slug. Run 'repl allocation list' to see all allocations." >&2
  exit 1
fi

# Extract fields from allocation (includes joined instance_ip)
ALLOC_STATUS="$(echo "$ALLOC_RESP" | grep -o '"status":"[^"]*"' | head -1 | sed 's/"status":"//;s/"//')"
ALLOC_HOLD="$(echo "$ALLOC_RESP" | grep -o '"debug_hold_until":[0-9]*' | head -1 | sed 's/"debug_hold_until"://')"
INSTANCE_ID="$(echo "$ALLOC_RESP" | grep -o '"instance_id":[0-9]*' | head -1 | sed 's/"instance_id"://')"
PUBLIC_IP="$(echo "$ALLOC_RESP" | grep -o '"instance_ip":"[^"]*"' | head -1 | sed 's/"instance_ip":"//;s/"//')"

# Check SSH accessibility (ACTIVE always; COMPLETE only while debug hold is valid)
NOW_MS="$(date +%s%3N 2>/dev/null || echo '0')"
SSH_OK=0
if [[ "$ALLOC_STATUS" == "ACTIVE" ]]; then
  SSH_OK=1
elif [[ "$ALLOC_STATUS" == "COMPLETE" && -n "$ALLOC_HOLD" && "$ALLOC_HOLD" != "null" ]]; then
  if [[ "$ALLOC_HOLD" -gt "$NOW_MS" ]]; then
    SSH_OK=1
  fi
fi

if [[ "$SSH_OK" -eq 0 ]]; then
  echo "ssh_proxy.sh: Allocation 'repl-${SLUG}' is ${ALLOC_STATUS}, SSH not available." >&2
  if [[ "$ALLOC_STATUS" == "COMPLETE" ]]; then
    echo "  The debug hold has expired. Use 'repl extend ${SLUG}' to restore access." >&2
  fi
  exit 1
fi

if [[ -z "$INSTANCE_ID" || "$INSTANCE_ID" == "null" ]]; then
  echo "ssh_proxy.sh: No instance_id in allocation for 'repl-${SLUG}'." >&2
  exit 2
fi

# ---------------------------------------------------------------------------
# Step 2: Query tailscale status from the instance record
# ---------------------------------------------------------------------------

TS_RESP="$(api_get "${CONTROL_PLANE_URL}/v1/instances/${INSTANCE_ID}/tailscale-status")" || {
  echo "ssh_proxy.sh: Cannot reach SkyREPL control plane (tailscale-status). Is it running?" >&2
  exit 2
}
validate_response "$TS_RESP" "GET /v1/instances/${INSTANCE_ID}/tailscale-status"

TAILSCALE_STATUS="$(echo "$TS_RESP" | grep -o '"tailscale_status":"[^"]*"' | head -1 | sed 's/"tailscale_status":"//;s/"//')"
TAILSCALE_IP="$(echo "$TS_RESP" | grep -o '"tailscale_ip":"[^"]*"' | head -1 | sed 's/"tailscale_ip":"//;s/"//')"

# Treat missing/null status as not_installed
if [[ -z "$TAILSCALE_STATUS" || "$TAILSCALE_STATUS" == "null" ]]; then
  TAILSCALE_STATUS="not_installed"
fi
if [[ "$TAILSCALE_IP" == "null" ]]; then
  TAILSCALE_IP=""
fi

# ---------------------------------------------------------------------------
# Step 3: JIT Tailscale install if needed
# ---------------------------------------------------------------------------

if [[ "$TAILSCALE_STATUS" == "ready" && -n "$TAILSCALE_IP" ]]; then
  # Fast path: Tailscale already ready
  exec nc "$TAILSCALE_IP" 22
fi

if [[ "$TAILSCALE_STATUS" == "not_installed" || "$TAILSCALE_STATUS" == "failed" ]]; then
  echo "Installing Tailscale on instance... (typically 15-30s)" >&2

  ENSURE_RESP="$(api_post "${CONTROL_PLANE_URL}/v1/instances/${INSTANCE_ID}/tailscale-ensure")" || {
    echo "ssh_proxy.sh: Failed to trigger Tailscale install." >&2
    TAILSCALE_STATUS="failed"
  }

  if [[ -n "$ENSURE_RESP" ]]; then
    # Check for 503 (Tailscale not configured server-side) — fall through to public IP
    if echo "$ENSURE_RESP" | grep -q '"TAILSCALE_NOT_CONFIGURED"'; then
      echo "Warning: Tailscale not configured on control plane, using public IP." >&2
      TAILSCALE_STATUS="failed"
    else
      TAILSCALE_STATUS="installing"
    fi
  fi
fi

if [[ "$TAILSCALE_STATUS" == "installing" ]]; then
  ELAPSED=0
  while [[ "$ELAPSED" -lt "$INSTALL_TIMEOUT" ]]; do
    sleep "$POLL_INTERVAL"
    ELAPSED=$((ELAPSED + POLL_INTERVAL))

    POLL_RESP="$(api_get "${CONTROL_PLANE_URL}/v1/instances/${INSTANCE_ID}/tailscale-status")" || continue

    if ! echo "$POLL_RESP" | grep -qE '^[[:space:]]*[{[]'; then
      continue
    fi

    TAILSCALE_STATUS="$(echo "$POLL_RESP" | grep -o '"tailscale_status":"[^"]*"' | head -1 | sed 's/"tailscale_status":"//;s/"//')"
    TAILSCALE_IP="$(echo "$POLL_RESP" | grep -o '"tailscale_ip":"[^"]*"' | head -1 | sed 's/"tailscale_ip":"//;s/"//')"

    if [[ "$TAILSCALE_IP" == "null" ]]; then
      TAILSCALE_IP=""
    fi

    if [[ "$TAILSCALE_STATUS" == "ready" && -n "$TAILSCALE_IP" ]]; then
      echo "Tailscale ready. Connecting..." >&2
      exec nc "$TAILSCALE_IP" 22
    fi

    if [[ "$TAILSCALE_STATUS" == "failed" ]]; then
      echo "Warning: Tailscale installation failed, falling back to public IP." >&2
      break
    fi

    # Show progress every ~10s
    if (( ELAPSED % 10 == 0 )); then
      echo "  Waiting for Tailscale... (${ELAPSED}s / ${INSTALL_TIMEOUT}s)" >&2
    fi
  done

  if [[ "$TAILSCALE_STATUS" != "ready" ]]; then
    echo "Warning: Tailscale timed out after ${INSTALL_TIMEOUT}s, falling back to public IP." >&2
  fi
fi

# ---------------------------------------------------------------------------
# Step 4: Fall back to public IP
# ---------------------------------------------------------------------------

if [[ -n "$PUBLIC_IP" && "$PUBLIC_IP" != "null" ]]; then
  echo "Warning: Tailscale unavailable, using public IP." >&2
  exec nc "$PUBLIC_IP" 22
fi

echo "ssh_proxy.sh: No IP address available for 'repl-${SLUG}'." >&2
echo "  The instance may still be provisioning, or Tailscale failed and no public IP is set." >&2
echo "  Try: repl status ${SLUG}" >&2
exit 1
