// workflow/events.ts - Typed EventEmitter for workflow engine events
//
// The engine emits events here. The API layer subscribes and bridges to SSE.
// This file has NO imports from api/ — the layering is strictly one-way.

import { EventEmitter } from "events";

// =============================================================================
// Event Payload Types
// =============================================================================

export interface NodeStartedPayload {
  workflowId: number;
  nodeId: string;
  nodeType: string;
  timestamp: number;
}

export interface NodeCompletedPayload {
  workflowId: number;
  nodeId: string;
  nodeType: string;
  output: unknown;
  timestamp: number;
}

export interface NodeFailedPayload {
  workflowId: number;
  nodeId: string;
  nodeType: string;
  error: string;
  timestamp: number;
}

export interface WorkflowCompletedPayload {
  workflowId: number;
  output: unknown;
  timestamp: number;
}

export interface WorkflowFailedPayload {
  workflowId: number;
  error: string;
  nodeId?: string;
  timestamp: number;
}

export interface ProgressPayload {
  workflowId: number;
  completed: number;
  total: number;
  percentage: number;
}

// =============================================================================
// Event Map
// =============================================================================

export interface WorkflowEventMap {
  node_started: [NodeStartedPayload];
  node_completed: [NodeCompletedPayload];
  node_failed: [NodeFailedPayload];
  workflow_completed: [WorkflowCompletedPayload];
  workflow_failed: [WorkflowFailedPayload];
  progress: [ProgressPayload];
}

// =============================================================================
// Typed Emitter
// =============================================================================

class WorkflowEventEmitter extends EventEmitter {
  emit<K extends keyof WorkflowEventMap>(
    event: K,
    ...args: WorkflowEventMap[K]
  ): boolean {
    return super.emit(event, ...args);
  }

  on<K extends keyof WorkflowEventMap>(
    event: K,
    listener: (...args: WorkflowEventMap[K]) => void
  ): this {
    return super.on(event, listener as (...args: any[]) => void);
  }

  off<K extends keyof WorkflowEventMap>(
    event: K,
    listener: (...args: WorkflowEventMap[K]) => void
  ): this {
    return super.off(event, listener as (...args: any[]) => void);
  }

  once<K extends keyof WorkflowEventMap>(
    event: K,
    listener: (...args: WorkflowEventMap[K]) => void
  ): this {
    return super.once(event, listener as (...args: any[]) => void);
  }
}

/** Singleton engine event emitter. Engine emits; API layer subscribes. */
export const workflowEvents = new WorkflowEventEmitter();

// Raise the default max listener limit — each SSE stream for a given workflow
// adds one listener, so with many concurrent workflows this can exceed 10.
workflowEvents.setMaxListeners(200);

// =============================================================================
// State Transition Events
// =============================================================================
// In-process event bus for state transitions.
// Replaces DB polling in AgentBridge.waitForEvent() with EventEmitter.
// Singleton — import { stateEvents, STATE_EVENT } from "./events" anywhere in the control plane.

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

export const STATE_EVENT = {
  ALLOCATION_STATUS_CHANGED: "allocation:status_changed",
  RUN_FINISHED: "run:finished",
} as const;

/** Singleton state transition event bus. Control plane emits; waiters subscribe. */
export const stateEvents = new EventEmitter();
stateEvents.setMaxListeners(100); // Allow many concurrent waiters
