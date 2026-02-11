// workflow/state-events.ts - In-process event bus for state transitions
// Replaces DB polling in AgentBridge.waitForEvent() with EventEmitter.
// Singleton — import { stateEvents } from "./state-events" anywhere in the control plane.

import { EventEmitter } from "events";

// Event payload types
export interface AllocationStatusChangedEvent {
  allocationId: number;
  runId: number | null;
  fromStatus: string;
  toStatus: string;
}

export interface RunFinishedEvent {
  runId: number;
  exitCode: number | null;
  spotInterrupted: boolean;
}

// Event name constants
export const STATE_EVENT = {
  ALLOCATION_STATUS_CHANGED: "allocation:status_changed",
  RUN_FINISHED: "run:finished",
} as const;

// Singleton event bus
export const stateEvents = new EventEmitter();
stateEvents.setMaxListeners(100); // Allow many concurrent waiters
