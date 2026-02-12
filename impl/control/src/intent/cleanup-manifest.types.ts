// intent/cleanup-manifest.types.ts - Cleanup Manifest Intent Types

// =============================================================================
// Cleanup Manifest Input
// =============================================================================

export interface CleanupManifestInput {
  /** ID of the manifest to clean up */
  manifestId: number;
}

// =============================================================================
// Cleanup Manifest Output
// =============================================================================

export interface CleanupManifestOutput {
  /** ID of the manifest that was cleaned up */
  manifestId: number;
  /** Number of resources cleaned */
  resourcesCleaned: number;
  /** Whether the manifest was deleted */
  deleted: boolean;
}
