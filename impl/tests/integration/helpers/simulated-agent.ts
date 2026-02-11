// helpers/simulated-agent.ts - In-process agent that speaks the real HTTP/SSE protocol
//
// Replaces the Python agent for fast E2E testing. Connects to the control plane
// SSE endpoint, receives start_run commands, and POSTs back sync_complete, logs,
// and completion status — exactly like the real agent, but in-process and sub-second.

export interface SimulatedAgentConfig {
  baseUrl: string;
  instanceId: string;
  authToken: string;
  /** Simulated command behavior. Default: echo the command string to stdout. */
  behavior?: AgentBehavior;
}

export interface AgentBehavior {
  /** Lines to emit on stdout (default: derived from command) */
  stdout?: string[];
  /** Lines to emit on stderr (default: none) */
  stderr?: string[];
  /** Exit code (default: 0) */
  exitCode?: number;
  /** Delay in ms before posting sync_complete (default: 0) */
  syncDelayMs?: number;
  /** Delay in ms between log lines (default: 0) */
  logDelayMs?: number;
  /** If true, fail sync (default: false) */
  failSync?: boolean;
}

interface StartRunCommand {
  type: "start_run";
  command_id: number;
  run_id: number;
  allocation_id: number;
  command: string;
  workdir: string;
  files: Array<{ path: string; checksum: string; url: string }>;
}

export class SimulatedAgent {
  private config: SimulatedAgentConfig;
  private abortController: AbortController | null = null;
  private connected = false;

  constructor(config: SimulatedAgentConfig) {
    this.config = config;
  }

  /** Connect to SSE and start processing commands. Non-blocking. */
  async connect(): Promise<void> {
    this.abortController = new AbortController();
    const { baseUrl, instanceId, authToken } = this.config;
    const sseUrl = `${baseUrl}/v1/agent/commands?instance_id=${instanceId}&token=${authToken}`;

    try {
      const res = await fetch(sseUrl, {
        signal: this.abortController.signal,
        headers: { Accept: "text/event-stream", "Cache-Control": "no-cache" },
      });

      if (!res.ok || !res.body) {
        console.error(`[sim-agent] SSE connect failed: ${res.status}`);
        return;
      }

      this.connected = true;
      // Process SSE stream in background
      this.processStream(res.body).catch((err) => {
        if (!String(err).includes("abort")) {
          console.error(`[sim-agent] Stream error:`, err);
        }
      });
    } catch (err) {
      if (!String(err).includes("abort")) {
        console.error(`[sim-agent] Connect error:`, err);
      }
    }
  }

  /** Disconnect from SSE. */
  disconnect(): void {
    this.connected = false;
    this.abortController?.abort();
    this.abortController = null;
  }

  isConnected(): boolean {
    return this.connected;
  }

  // ---------------------------------------------------------------------------
  // SSE Stream Processing
  // ---------------------------------------------------------------------------

  private async processStream(body: ReadableStream<Uint8Array>): Promise<void> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let currentEvent = "";
    let currentData: string[] = [];

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? ""; // Keep incomplete line in buffer

        for (const line of lines) {
          const trimmed = line.trim();

          if (trimmed === "") {
            // Empty line = end of event
            if (currentData.length > 0) {
              const dataStr = currentData.join("\n");
              try {
                const parsed = JSON.parse(dataStr);
                await this.handleEvent(currentEvent || "message", parsed);
              } catch {
                // Ignore malformed data
              }
            }
            currentEvent = "";
            currentData = [];
            continue;
          }

          if (trimmed.startsWith("event:")) {
            currentEvent = trimmed.slice(6).trim();
          } else if (trimmed.startsWith("data:")) {
            currentData.push(trimmed.slice(5).trim());
          }
          // Ignore comments and other lines
        }
      }
    } catch (err) {
      if (!String(err).includes("abort")) throw err;
    } finally {
      reader.releaseLock();
      this.connected = false;
    }
  }

  private async handleEvent(event: string, data: Record<string, unknown>): Promise<void> {
    const msgType = data.type ?? event;

    if (msgType === "heartbeat_ack") {
      // Connection confirmed, nothing to do
      return;
    }

    if (msgType === "start_run") {
      await this.handleStartRun(data as unknown as StartRunCommand);
      return;
    }

    // Ignore other event types (cancel_run, etc.)
  }

  // ---------------------------------------------------------------------------
  // Command Execution Simulation
  // ---------------------------------------------------------------------------

  private async handleStartRun(cmd: StartRunCommand): Promise<void> {
    const behavior = this.config.behavior ?? {};
    const { baseUrl, authToken } = this.config;

    // Brief delay to simulate sync
    if (behavior.syncDelayMs) {
      await Bun.sleep(behavior.syncDelayMs);
    }

    // Post sync_complete
    await this.post("/v1/agent/logs", {
      run_id: cmd.run_id,
      stream: "sync_complete",
      data: behavior.failSync ? "sync failed" : "sync complete",
      timestamp: Date.now(),
      sync_success: !behavior.failSync,
    });

    if (behavior.failSync) return;

    // Determine output
    const stdoutLines = behavior.stdout ?? this.deriveOutput(cmd.command);
    const stderrLines = behavior.stderr ?? [];
    const exitCode = behavior.exitCode ?? 0;

    // Post stdout lines
    for (const line of stdoutLines) {
      if (behavior.logDelayMs) await Bun.sleep(behavior.logDelayMs);
      await this.post("/v1/agent/logs", {
        run_id: cmd.run_id,
        stream: "stdout",
        data: line.endsWith("\n") ? line : line + "\n",
        timestamp: Date.now(),
      });
    }

    // Post stderr lines
    for (const line of stderrLines) {
      if (behavior.logDelayMs) await Bun.sleep(behavior.logDelayMs);
      await this.post("/v1/agent/logs", {
        run_id: cmd.run_id,
        stream: "stderr",
        data: line.endsWith("\n") ? line : line + "\n",
        timestamp: Date.now(),
      });
    }

    // Post completion
    await this.post("/v1/agent/status", {
      run_id: cmd.run_id,
      status: exitCode === 0 ? "completed" : "failed",
      exit_code: exitCode,
    });
  }

  /** Derive simulated output from command string. */
  private deriveOutput(command: string): string[] {
    // Handle common patterns
    if (command.startsWith("echo ")) {
      return [command.slice(5)];
    }
    if (command.startsWith("exit ")) {
      return [];
    }
    // For anything else, return the command as output
    return [command];
  }

  // ---------------------------------------------------------------------------
  // HTTP Helpers
  // ---------------------------------------------------------------------------

  private async post(path: string, body: Record<string, unknown>): Promise<void> {
    const { baseUrl, authToken } = this.config;
    const url = `${baseUrl}${path}`;
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (authToken) {
      headers["Authorization"] = `Bearer ${authToken}`;
    }

    const res = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const text = await res.text();
      console.warn(`[sim-agent] POST ${path} failed: ${res.status} ${text}`);
    }
  }
}
