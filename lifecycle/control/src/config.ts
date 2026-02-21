// config.ts - Shared Configuration Utilities

/**
 * Get the unique identifier for this control plane instance.
 * Used for agent heartbeat acknowledgments and control plane identity tracking.
 */
export function getControlPlaneId(): string {
  return process.env.CONTROL_PLANE_ID ?? "cp-default";
}
