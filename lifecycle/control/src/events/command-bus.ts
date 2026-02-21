// events/command-bus.ts - Late-bound command dispatch
//
// Breaks the L5/L3 → L7 import cycle: workflow nodes and providers need to
// send commands to agents, but should not depend on SSEManager directly.
//
// Usage:
//   import { commandBus } from "../events/command-bus";
//   await commandBus.sendCommand(instanceId, { type: "cancel_run", ... });
//
// SSEManager registers itself at startup via setCommandBus().

import type { ControlToAgentMessage } from "@skyrepl/contracts";

// =============================================================================
// Interface
// =============================================================================

export interface CommandBus {
  sendCommand(instanceId: string, command: ControlToAgentMessage): Promise<boolean>;
}

// =============================================================================
// No-op default (pre-registration)
// =============================================================================

const noopBus: CommandBus = {
  async sendCommand(instanceId, command) {
    console.warn("[command-bus] sendCommand called before SSEManager registered", {
      instanceId,
      type: command.type,
    });
    return false;
  },
};

// =============================================================================
// Singleton + registration
// =============================================================================

let _impl: CommandBus = noopBus;

/** Register the backing implementation (called once at startup by SSEManager). */
export function setCommandBus(impl: CommandBus): void {
  _impl = impl;
}

/** Singleton command bus. Backed by a no-op until setCommandBus() is called. */
export const commandBus: CommandBus = {
  sendCommand(instanceId, command) {
    return _impl.sendCommand(instanceId, command);
  },
};
