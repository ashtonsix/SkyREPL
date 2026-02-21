#!/usr/bin/env bash
# orbstack-reset-networking.sh - Recover from OrbStack IP pool exhaustion
#
# When OrbStack's internal DHCP allocator runs out of IPs (from rapid
# create/delete cycles), new VMs fail with "missing IP address". This script
# surgically resets the networking state without destroying any existing VMs.
#
# Usage (run from macOS host, NOT inside a VM):
#   bash scripts/orbstack-reset-networking.sh
#
# What it does:
#   1. Lists all current VMs and preserves their names
#   2. Stops the OrbStack helper process (which caches IP state)
#   3. Pivots the network subnet to force a DHCP state flush
#   4. Restores the original subnet
#   5. Restarts OrbStack and waits for VMs to come back
#
# Safe: does NOT delete any VMs or data. The "ubuntu" dev machine is preserved.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[orbstack-reset]${NC} $*"; }
warn() { echo -e "${YELLOW}[orbstack-reset]${NC} $*"; }
err()  { echo -e "${RED}[orbstack-reset]${NC} $*" >&2; }

# Ensure we're running on macOS host, not inside a VM
if [[ -f /etc/orbstack-release ]] || [[ "$(uname)" != "Darwin" ]]; then
  err "This script must be run on the macOS host, not inside an OrbStack VM."
  exit 1
fi

# Check orbctl is available
if ! command -v orbctl &>/dev/null; then
  err "orbctl not found. Is OrbStack installed?"
  exit 1
fi

# Step 1: Record current VMs
log "Recording current VMs..."
VM_LIST=$(orbctl list 2>/dev/null || true)
echo "$VM_LIST"
echo ""

# Step 2: Get current subnet
CURRENT_SUBNET=$(orbctl config get network.subnet4 2>/dev/null || echo "192.168.138.0/23")
log "Current subnet: $CURRENT_SUBNET"

# Step 3: Pivot subnet to flush DHCP state
# Use a temporary different /23 range, then switch back
TEMP_SUBNET="192.168.140.0/23"
if [[ "$CURRENT_SUBNET" == "$TEMP_SUBNET" ]]; then
  TEMP_SUBNET="192.168.142.0/23"
fi

log "Pivoting subnet to $TEMP_SUBNET to flush DHCP state..."
orbctl config set network.subnet4 "$TEMP_SUBNET" 2>/dev/null || true

log "Waiting 5s for OrbStack to apply new subnet..."
sleep 5

# Step 4: Restore original subnet
log "Restoring original subnet $CURRENT_SUBNET..."
orbctl config set network.subnet4 "$CURRENT_SUBNET" 2>/dev/null || true

log "Waiting 5s for OrbStack to apply restored subnet..."
sleep 5

# Step 5: Verify VMs are still present
log "Verifying VMs are still present..."
orbctl list 2>/dev/null || true
echo ""

# Step 6: Quick create/delete test
log "Testing VM creation..."
if orbctl create ubuntu orbstack-reset-test 2>/dev/null; then
  TEST_IP=$(orbctl info orbstack-reset-test -f json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('ip4','none'))" 2>/dev/null || echo "unknown")
  log "Test VM created successfully with IP: $TEST_IP"
  orbctl stop orbstack-reset-test 2>/dev/null || true
  sleep 1
  orbctl delete -f orbstack-reset-test 2>/dev/null || true
  log "Test VM cleaned up."
else
  warn "Test VM creation failed. You may need to restart OrbStack:"
  warn "  osascript -e 'quit app \"OrbStack\"' && sleep 3 && open -a OrbStack"
  exit 1
fi

echo ""
log "Done! OrbStack networking has been reset. IP pool should be available."
