// background/log-streaming.ts — 3-tier log streaming model
//
// Tier 1: In-memory ring buffer (transient, for live SSE streaming)
// Tier 2: Chunk storage (inline blobs <64KB, immutable once written)
// Tier 3: Gather — concatenates completed run chunks into larger gathered blobs

import { createBlob, queryOne, queryMany, getDatabase, deleteBlobBatch, getRun, getBlob } from "../material/db"; // raw-db: log_chunks CRUD (Bucket D, owns table), see WL-057
import { getControlId } from "../material/control-id";
import { getInline } from "../material/storage";
import { getBlobProvider } from "../provider/storage/registry";
import { SBO_THRESHOLD } from "../provider/storage/types";

// =============================================================================
// Types
// =============================================================================

export interface LogChunk {
  id: number;
  run_id: number;
  stream: string; // "stdout" | "stderr"
  chunk_seq: number;
  byte_offset_start: number;
  blob_id: number;
  start_ms: number;
  end_ms: number;
  size_bytes: number;
  created_at: number;
}

// =============================================================================
// Tier 1: In-Memory Ring Buffer
// =============================================================================

const RING_BUFFER_CAP = 65536; // 64KB
const CHUNK_FLUSH_THRESHOLD = 4096; // 4KB
const CHUNK_FLUSH_INTERVAL_MS = 30_000; // 30s

interface RunBuffer {
  runId: number;
  stream: string;
  data: Buffer;
  chunkSeq: number;
  byteOffset: number;
  lastFlushMs: number;
  tenantId: number;
  manifestId: number | null;
}

// Active run buffers: key = `${runId}:${stream}`
const runBuffers = new Map<string, RunBuffer>();

export function appendLog(
  runId: number,
  stream: string,
  data: string | Buffer,
  tenantId: number = 1,
  manifestId: number | null = null
): void {
  const key = `${runId}:${stream}`;
  let buf = runBuffers.get(key);

  if (!buf) {
    buf = {
      runId,
      stream,
      data: Buffer.alloc(0),
      chunkSeq: getNextChunkSeq(runId, stream),
      byteOffset: getByteOffset(runId, stream),
      lastFlushMs: Date.now(),
      tenantId,
      manifestId,
    };
    runBuffers.set(key, buf);
  }

  const incoming = typeof data === "string" ? Buffer.from(data, "utf-8") : data;

  // Append to buffer
  buf.data = Buffer.concat([buf.data, incoming]);

  // Ring semantics: evict oldest if over cap
  if (buf.data.length > RING_BUFFER_CAP) {
    buf.data = buf.data.subarray(buf.data.length - RING_BUFFER_CAP);
  }

  // Check if we should flush
  if (buf.data.length >= CHUNK_FLUSH_THRESHOLD) {
    flushBuffer(key, buf);
  }
}

function flushBuffer(key: string, buf: RunBuffer): void {
  if (buf.data.length === 0) return;

  const now = Date.now();
  const chunkData = Buffer.from(buf.data); // Copy

  // Create blob (always inline — chunks are <64KB)
  const blob = createBlob(
    {
      bucket: "log-chunks",
      checksum: `log-${buf.runId}-${buf.stream}-${buf.chunkSeq}`,
      checksum_bytes: chunkData.length,
      s3_key: null,
      s3_bucket: null,
      payload: chunkData,
      size_bytes: chunkData.length,
      last_referenced_at: now,
    },
    buf.tenantId
  );

  // Insert log_chunk record
  const db = getDatabase();
  db.prepare(
    `INSERT INTO log_chunks (run_id, stream, chunk_seq, byte_offset_start, blob_id, start_ms, end_ms, size_bytes, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).run(
    buf.runId,
    buf.stream,
    buf.chunkSeq,
    buf.byteOffset,
    blob.id,
    buf.lastFlushMs,
    now,
    chunkData.length,
    now
  );

  // Advance state
  buf.byteOffset += chunkData.length;
  buf.chunkSeq++;
  buf.data = Buffer.alloc(0);
  buf.lastFlushMs = now;
}

/** Flush all buffers for a run (called on run completion) */
export function flushRunBuffers(runId: number): void {
  for (const [key, buf] of runBuffers) {
    if (buf.runId === runId) {
      flushBuffer(key, buf);
      runBuffers.delete(key);
    }
  }
}

/** Periodic flush: flush buffers that haven't flushed in 30s */
export function periodicFlush(): void {
  const now = Date.now();
  for (const [key, buf] of runBuffers) {
    if (buf.data.length > 0 && now - buf.lastFlushMs >= CHUNK_FLUSH_INTERVAL_MS) {
      flushBuffer(key, buf);
    }
  }
}

/** Get live buffer content for SSE streaming */
export function getLiveBuffer(runId: number, stream: string): Buffer | null {
  const key = `${runId}:${stream}`;
  const buf = runBuffers.get(key);
  return buf?.data.length ? Buffer.from(buf.data) : null;
}

// =============================================================================
// Helpers
// =============================================================================

function getNextChunkSeq(runId: number, stream: string): number {
  const row = queryOne<{ max_seq: number | null }>(
    "SELECT MAX(chunk_seq) as max_seq FROM log_chunks WHERE run_id = ? AND stream = ?",
    [runId, stream]
  );
  return (row?.max_seq ?? -1) + 1;
}

function getByteOffset(runId: number, stream: string): number {
  const row = queryOne<{ total: number | null }>(
    "SELECT SUM(size_bytes) as total FROM log_chunks WHERE run_id = ? AND stream = ?",
    [runId, stream]
  );
  return row?.total ?? 0;
}

// =============================================================================
// Download API (Tier 2 reads)
// =============================================================================

export async function downloadLogs(
  runId: number,
  opts?: {
    stream?: string;
    fromMs?: number;
    toMs?: number;
    maxBytes?: number;
  }
): Promise<Buffer> {
  const stream = opts?.stream ?? "stdout";
  const fromMs = opts?.fromMs ?? 0;
  const toMs = opts?.toMs ?? Number.MAX_SAFE_INTEGER;

  // First flush any live buffer
  const key = `${runId}:${stream}`;
  const buf = runBuffers.get(key);
  if (buf && buf.data.length > 0) {
    flushBuffer(key, buf);
  }

  // Lazy gather: if run is complete and has >1 chunk per stream, compact first
  const run = getRun(runId);
  if (run && isCompletedRun(run.workflow_state)) {
    const chunkCount = queryOne<{ cnt: number }>(
      "SELECT COUNT(*) as cnt FROM log_chunks WHERE run_id = ? AND stream = ?",
      [runId, stream]
    );
    if (chunkCount && chunkCount.cnt > 1) {
      await compactRunLogs(runId);
    }
  }

  // Query chunks in range
  const chunks = queryMany<{ blob_id: number; chunk_seq: number; size_bytes: number }>(
    `SELECT blob_id, chunk_seq, size_bytes FROM log_chunks
     WHERE run_id = ? AND stream = ? AND start_ms <= ? AND end_ms >= ?
     ORDER BY chunk_seq ASC`,
    [runId, stream, toMs, fromMs]
  );

  const buffers: Buffer[] = [];

  // Read chunks (inline blobs directly, S3 blobs via provider)
  for (const chunk of chunks) {
    const payload = getInline(chunk.blob_id);
    if (payload) {
      buffers.push(payload);
    } else {
      // Gathered blobs >= SBO_THRESHOLD are stored in S3 with payload=null
      const blob = getBlob(chunk.blob_id);
      if (blob?.s3_key) {
        const data = await getBlobProvider(blob.tenant_id).download(blob.s3_key);
        buffers.push(data);
      } else {
        console.warn(`[log-streaming] Missing payload for blob_id=${chunk.blob_id} (run=${runId}, stream=${stream}, seq=${chunk.chunk_seq})`);
      }
    }
  }

  let result = Buffer.concat(buffers);

  // max_bytes: truncate from START (keep most recent, tail semantics)
  if (opts?.maxBytes && result.length > opts.maxBytes) {
    result = result.subarray(result.length - opts.maxBytes);
  }

  return result;
}

// =============================================================================
// Tier 3: Gather (log compaction)
// =============================================================================

const COMPLETED_STATES = ["launch-run:complete", "launch-run:failed", "launch-run:timeout"];

function isCompletedRun(workflowState: string): boolean {
  return COMPLETED_STATES.some(s => workflowState.startsWith(s));
}

/**
 * Compact all log chunks for a single run into one gathered blob per stream.
 * Idempotent: if a stream already has only 1 chunk, it is skipped.
 */
export async function compactRunLogs(runId: number): Promise<void> {
  const run = getRun(runId);
  if (!run) return;

  const tenantId = run.tenant_id;

  // Find all streams for this run that have >1 chunk
  const streams = queryMany<{ stream: string; cnt: number }>(
    `SELECT stream, COUNT(*) as cnt FROM log_chunks WHERE run_id = ? GROUP BY stream HAVING cnt > 1`,
    [runId]
  );

  if (streams.length === 0) return;

  for (const { stream } of streams) {
    // Fetch all chunks ordered by sequence
    const chunks = queryMany<{
      id: number;
      blob_id: number;
      chunk_seq: number;
      byte_offset_start: number;
      start_ms: number;
      end_ms: number;
      size_bytes: number;
    }>(
      `SELECT id, blob_id, chunk_seq, byte_offset_start, start_ms, end_ms, size_bytes
       FROM log_chunks WHERE run_id = ? AND stream = ?
       ORDER BY chunk_seq ASC`,
      [runId, stream]
    );

    if (chunks.length <= 1) continue;

    // Concatenate all chunk payloads
    const parts: Buffer[] = [];
    for (const chunk of chunks) {
      const payload = getInline(chunk.blob_id);
      if (payload) {
        parts.push(payload);
      } else {
        console.warn(`[log-compaction] Missing payload for blob_id=${chunk.blob_id} (run=${runId}, stream=${stream}, seq=${chunk.chunk_seq})`);
      }
    }
    const gathered = Buffer.concat(parts);

    const startMs = Math.min(...chunks.map(c => c.start_ms));
    const endMs = Math.max(...chunks.map(c => c.end_ms));
    const now = Date.now();

    const db = getDatabase();

    // Create the gathered blob
    let newBlobId: number;
    if (gathered.length >= SBO_THRESHOLD) {
      // Upload to blob provider; store reference via s3_key.
      // Key embeds controlId for orphan recovery. manifest_id is omitted here:
      // log-gathered blobs span a run (not a manifest), so there is no single
      // meaningful manifest_id to embed — run_id serves the same locating purpose.
      const controlId = getControlId(getDatabase());
      const s3Key = `log-gathered/${controlId}/${tenantId}/${runId}/${stream}`;
      const provider = getBlobProvider(tenantId);
      await provider.upload(s3Key, gathered, { contentType: "application/octet-stream" });

      const blob = createBlob(
        {
          bucket: "log-chunks",
          checksum: `log-gathered-${runId}-${stream}`,
          checksum_bytes: gathered.length,
          s3_key: s3Key,
          s3_bucket: null,
          payload: null,
          size_bytes: gathered.length,
          last_referenced_at: now,
        },
        tenantId
      );
      newBlobId = blob.id;
    } else {
      // Small enough to stay inline
      const blob = createBlob(
        {
          bucket: "log-chunks",
          checksum: `log-gathered-${runId}-${stream}`,
          checksum_bytes: gathered.length,
          s3_key: null,
          s3_bucket: null,
          payload: gathered,
          size_bytes: gathered.length,
          last_referenced_at: now,
        },
        tenantId
      );
      newBlobId = blob.id;
    }

    // Atomically replace chunks: insert gathered chunk, delete originals + their blobs
    const oldChunkIds = chunks.map(c => c.id);
    const oldBlobIds = chunks.map(c => c.blob_id);

    db.transaction(() => {
      // Insert new gathered chunk (chunk_seq=0, byte_offset_start=0)
      db.prepare(
        `INSERT INTO log_chunks (run_id, stream, chunk_seq, byte_offset_start, blob_id, start_ms, end_ms, size_bytes, created_at)
         VALUES (?, ?, 0, 0, ?, ?, ?, ?, ?)`
      ).run(runId, stream, newBlobId, startMs, endMs, gathered.length, now);

      // Delete old chunk records
      const chunkPlaceholders = oldChunkIds.map(() => "?").join(",");
      db.prepare(`DELETE FROM log_chunks WHERE id IN (${chunkPlaceholders})`).run(...oldChunkIds);

      // Delete old blobs atomically (no orphan window on crash)
      deleteBlobBatch(oldBlobIds);
    })();
  }
}

/**
 * Background log compaction task (Tier 3 gather).
 * Finds completed runs with >1 chunk per stream and compacts them.
 * Processes at most 10 runs per cycle to bound latency.
 */
export async function periodicLogCompaction(): Promise<void> {
  // Find completed runs that still have streams with >1 chunk
  const runs = queryMany<{ run_id: number }>(
    `SELECT DISTINCT lc.run_id
     FROM log_chunks lc
     JOIN runs r ON r.id = lc.run_id
     WHERE (r.workflow_state LIKE 'launch-run:complete%'
         OR r.workflow_state LIKE 'launch-run:failed%'
         OR r.workflow_state LIKE 'launch-run:timeout%')
     GROUP BY lc.run_id, lc.stream
     HAVING COUNT(*) > 1
     LIMIT 10`
  );

  if (runs.length === 0) return;

  for (const { run_id } of runs) {
    try {
      await compactRunLogs(run_id);
    } catch (err) {
      console.error(`[log-compaction] Failed to compact run ${run_id}:`, err);
    }
  }

  console.log(`[log-compaction] Compacted ${runs.length} run(s)`);
}

// =============================================================================
// Test helpers
// =============================================================================

/** Clear all in-memory buffers (for testing) */
export function _clearBuffers(): void {
  runBuffers.clear();
}
