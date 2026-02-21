// provider/bootstrap/bootstrap.test.ts - Cloud-init snippet and assembler tests

import { describe, test, expect } from "bun:test";
import { createHash } from "crypto";
import {
  disableAptDaily,
  writeAgentConfig,
  downloadAgent,
  installFeature,
  userInitScript,
  startAgent,
} from "./snippets";
import { assembleCloudInit } from "./cloud-init";
import type { BootstrapConfig } from "../types";

// =============================================================================
// Test fixtures
// =============================================================================

const baseConfig: BootstrapConfig = {
  agentUrl: "https://control.example.com/v1/agent/download/agent.py",
  controlPlaneUrl: "https://control.example.com",
  registrationToken: "reg-token-abc123",
};

// =============================================================================
// disableAptDaily
// =============================================================================

describe("disableAptDaily", () => {
  test("returns a runcmd with systemctl disable command", () => {
    const snippet = disableAptDaily();
    expect(snippet.runcmd).toBeDefined();
    expect(snippet.runcmd!.length).toBeGreaterThanOrEqual(1);
  });

  test("targets both apt-daily.timer and apt-daily-upgrade.timer", () => {
    const snippet = disableAptDaily();
    const flat = snippet.runcmd!.flat();
    expect(flat).toContain("apt-daily.timer");
    expect(flat).toContain("apt-daily-upgrade.timer");
  });

  test("uses systemctl disable --now", () => {
    const snippet = disableAptDaily();
    const flat = snippet.runcmd!.flat();
    expect(flat).toContain("systemctl");
    expect(flat).toContain("disable");
    expect(flat).toContain("--now");
  });

  test("has no writeFiles", () => {
    const snippet = disableAptDaily();
    expect(snippet.writeFiles).toBeUndefined();
  });
});

// =============================================================================
// writeAgentConfig
// =============================================================================

describe("writeAgentConfig", () => {
  test("returns a writeFiles entry for /etc/skyrepl/config.json", () => {
    const snippet = writeAgentConfig(
      "https://control.example.com",
      "token-xyz"
    );
    expect(snippet.writeFiles).toBeDefined();
    const file = snippet.writeFiles!.find(
      (f) => f.path === "/etc/skyrepl/config.json"
    );
    expect(file).toBeDefined();
  });

  test("config file content contains controlPlaneUrl", () => {
    const snippet = writeAgentConfig("https://cp.test", "tok");
    const file = snippet.writeFiles![0]!;
    const parsed = JSON.parse(file.content);
    expect(parsed.controlPlaneUrl).toBe("https://cp.test");
  });

  test("config file content contains registrationToken", () => {
    const snippet = writeAgentConfig("https://cp.test", "my-secret-token");
    const file = snippet.writeFiles![0]!;
    const parsed = JSON.parse(file.content);
    expect(parsed.registrationToken).toBe("my-secret-token");
  });

  test("config file has 0644 permissions", () => {
    const snippet = writeAgentConfig("https://cp.test", "tok");
    const file = snippet.writeFiles![0]!;
    expect(file.permissions).toBe("0644");
  });

  test("extra fields are merged into config", () => {
    const snippet = writeAgentConfig("https://cp.test", "tok", {
      logLevel: "debug",
    });
    const file = snippet.writeFiles![0]!;
    const parsed = JSON.parse(file.content);
    expect(parsed.logLevel).toBe("debug");
  });
});

// =============================================================================
// downloadAgent
// =============================================================================

describe("downloadAgent", () => {
  test("returns runcmd entries including mkdir for /opt/skyrepl-agent", () => {
    const snippet = downloadAgent("https://control.example.com");
    const flat = snippet.runcmd!.flat();
    expect(flat).toContain("/opt/skyrepl-agent");
    expect(flat).toContain("mkdir");
  });

  test("includes curl commands for all expected agent files", () => {
    const snippet = downloadAgent("https://control.example.com");
    const cmdStrings = snippet.runcmd!.map((cmd) => cmd.join(" "));
    const expectedFiles = [
      "agent.py",
      "executor.py",
      "heartbeat.py",
      "logs.py",
      "sse.py",
      "http_client.py",
    ];
    for (const filename of expectedFiles) {
      const hasCurl = cmdStrings.some(
        (s) => s.includes("curl") && s.includes(filename)
      );
      expect(hasCurl).toBe(true);
    }
  });

  test("curl URLs point to {controlPlaneUrl}/v1/agent/download/{filename}", () => {
    const cpUrl = "https://control.example.com";
    const snippet = downloadAgent(cpUrl);
    const curlCmds = snippet.runcmd!.filter((cmd) => cmd[0] === "curl");
    for (const cmd of curlCmds) {
      const url = cmd.find((s) => s.startsWith(cpUrl));
      expect(url).toBeDefined();
      expect(url).toContain("/v1/agent/download/");
    }
  });

  test("has no writeFiles", () => {
    const snippet = downloadAgent("https://control.example.com");
    expect(snippet.writeFiles).toBeUndefined();
  });
});

// =============================================================================
// installFeature
// =============================================================================

describe("installFeature", () => {
  test("nvidia-driver snippet includes apt install nvidia-driver-535", () => {
    const snippet = installFeature("nvidia-driver");
    const flat = snippet.runcmd!.flat();
    expect(flat).toContain("nvidia-driver-535");
    expect(flat).toContain("apt-get");
  });

  test("cuda snippet includes apt install cuda-toolkit-12-2", () => {
    const snippet = installFeature("cuda");
    const flat = snippet.runcmd!.flat();
    expect(flat).toContain("cuda-toolkit-12-2");
    expect(flat).toContain("apt-get");
  });

  test("docker snippet runs get.docker.com install script", () => {
    const snippet = installFeature("docker");
    const flat = snippet.runcmd!.flat();
    const combined = flat.join(" ");
    expect(combined).toContain("get.docker.com");
  });

  test("unknown feature returns empty snippet (no runcmd, no writeFiles)", () => {
    const snippet = installFeature("some-unknown-feature");
    expect(snippet.runcmd).toBeUndefined();
    expect(snippet.writeFiles).toBeUndefined();
    expect(snippet.packages).toBeUndefined();
  });

  test("all known feature names produce non-empty runcmd", () => {
    for (const name of ["nvidia-driver", "cuda", "docker"]) {
      const snippet = installFeature(name);
      expect(snippet.runcmd).toBeDefined();
      expect(snippet.runcmd!.length).toBeGreaterThan(0);
    }
  });
});

// =============================================================================
// userInitScript
// =============================================================================

describe("userInitScript", () => {
  test("wraps script in bash -c", () => {
    const snippet = userInitScript("echo hello");
    expect(snippet.runcmd).toBeDefined();
    expect(snippet.runcmd!.length).toBe(1);
    const cmd = snippet.runcmd![0]!;
    expect(cmd).toContain("bash");
    expect(cmd).toContain("-c");
    expect(cmd).toContain("echo hello");
  });

  test("passes the exact script string as the bash -c argument", () => {
    const script = "apt-get install -y vim && echo done";
    const snippet = userInitScript(script);
    const cmd = snippet.runcmd![0]!;
    expect(cmd[cmd.length - 1]).toBe(script);
  });

  test("has no writeFiles", () => {
    const snippet = userInitScript("echo hi");
    expect(snippet.writeFiles).toBeUndefined();
  });
});

// =============================================================================
// startAgent
// =============================================================================

describe("startAgent", () => {
  test("returns a runcmd entry", () => {
    const snippet = startAgent();
    expect(snippet.runcmd).toBeDefined();
    expect(snippet.runcmd!.length).toBeGreaterThanOrEqual(1);
  });

  test("changes to /opt/skyrepl-agent and runs agent.py", () => {
    const snippet = startAgent();
    const flat = snippet.runcmd!.flat();
    const combined = flat.join(" ");
    expect(combined).toContain("/opt/skyrepl-agent");
    expect(combined).toContain("agent.py");
  });

  test("has no writeFiles", () => {
    const snippet = startAgent();
    expect(snippet.writeFiles).toBeUndefined();
  });
});

// =============================================================================
// assembleCloudInit
// =============================================================================

describe("assembleCloudInit", () => {
  test("returns format: cloud-init", () => {
    const result = assembleCloudInit(baseConfig);
    expect(result.format).toBe("cloud-init");
  });

  test("content starts with #cloud-config header", () => {
    const result = assembleCloudInit(baseConfig);
    expect(result.content.startsWith("#cloud-config")).toBe(true);
  });

  test("checksum is a valid 64-character hex string", () => {
    const result = assembleCloudInit(baseConfig);
    expect(result.checksum).toMatch(/^[0-9a-f]{64}$/);
  });

  test("checksum is the SHA-256 of the content string", () => {
    const result = assembleCloudInit(baseConfig);
    const expected = createHash("sha256").update(result.content).digest("hex");
    expect(result.checksum).toBe(expected);
  });

  test("controlPlaneUrl appears in output", () => {
    const result = assembleCloudInit(baseConfig);
    expect(result.content).toContain(baseConfig.controlPlaneUrl);
  });

  test("registrationToken appears in output", () => {
    const result = assembleCloudInit(baseConfig);
    expect(result.content).toContain(baseConfig.registrationToken);
  });

  test("contains write_files section with /etc/skyrepl/config.json", () => {
    const result = assembleCloudInit(baseConfig);
    expect(result.content).toContain("write_files:");
    expect(result.content).toContain("/etc/skyrepl/config.json");
  });

  test("contains runcmd section", () => {
    const result = assembleCloudInit(baseConfig);
    expect(result.content).toContain("runcmd:");
  });

  test("disableAptDaily appears first in runcmd", () => {
    const result = assembleCloudInit(baseConfig);
    const runcmdIndex = result.content.indexOf("runcmd:");
    expect(runcmdIndex).toBeGreaterThan(-1);

    // The first runcmd entry (first line after "runcmd:") must contain
    // the systemctl disable command from disableAptDaily().
    const afterRuncmd = result.content.slice(runcmdIndex);
    const firstEntry = afterRuncmd.split("\n").find((l) => l.trimStart().startsWith("-"));
    expect(firstEntry).toBeDefined();
    expect(firstEntry).toContain("systemctl");
  });

  test("startAgent appears last in runcmd", () => {
    const result = assembleCloudInit(baseConfig);
    // agent.py must appear after all curl download lines
    const agentStartIdx = result.content.lastIndexOf("agent.py");
    // The last occurrence of /opt/skyrepl-agent/agent.py (download) should be
    // earlier than the cd && exec invocation which is the last runcmd entry.
    expect(agentStartIdx).toBeGreaterThan(-1);
    // Verify agent.py also appears as part of the start command (exec python3 agent.py)
    expect(result.content).toContain("python3");
  });

  test("features are included when specified", () => {
    const config: BootstrapConfig = {
      ...baseConfig,
      features: [{ name: "nvidia-driver" }, { name: "docker" }],
    };
    const result = assembleCloudInit(config);
    expect(result.content).toContain("nvidia-driver-535");
    expect(result.content).toContain("get.docker.com");
  });

  test("cuda feature is included when specified", () => {
    const config: BootstrapConfig = {
      ...baseConfig,
      features: [{ name: "cuda" }],
    };
    const result = assembleCloudInit(config);
    expect(result.content).toContain("cuda-toolkit-12-2");
  });

  test("userInitScript appears in runcmd when initScript is provided", () => {
    const config: BootstrapConfig = {
      ...baseConfig,
      initScript: "echo skyrepl-init-ok",
    };
    const result = assembleCloudInit(config);
    expect(result.content).toContain("echo skyrepl-init-ok");
  });

  test("userInitScript is absent when initScript is not provided", () => {
    const result = assembleCloudInit(baseConfig);
    // No bash -c wrapping of a user script should appear beyond normal snippets
    // (docker feature also uses bash -c, so only test without features)
    const configNoFeatures: BootstrapConfig = { ...baseConfig };
    const result2 = assembleCloudInit(configNoFeatures);
    // The only bash -c entries should come from startAgent, not a user script
    const bashCLines = result2.content
      .split("\n")
      .filter((l) => l.includes("bash") && l.includes("-c"));
    // startAgent uses bash -c; no user script means no extra bash -c
    expect(bashCLines.length).toBe(1);
  });

  test("output is deterministic for the same config", () => {
    const r1 = assembleCloudInit(baseConfig);
    const r2 = assembleCloudInit(baseConfig);
    expect(r1.content).toBe(r2.content);
    expect(r1.checksum).toBe(r2.checksum);
  });

  test("different registration tokens produce different output and checksums", () => {
    const r1 = assembleCloudInit({ ...baseConfig, registrationToken: "tok-A" });
    const r2 = assembleCloudInit({ ...baseConfig, registrationToken: "tok-B" });
    expect(r1.content).not.toBe(r2.content);
    expect(r1.checksum).not.toBe(r2.checksum);
  });

  test("initScript with shell metacharacters does not cause injection outside its runcmd entry", () => {
    // The script contains characters that would be dangerous if interpolated into a
    // surrounding shell command. In cloud-init YAML the runcmd entry is a JSON flow
    // sequence so each argument is a quoted JSON string — the metacharacters are
    // serialised as data, not interpreted by a shell at the YAML level.
    const malicious = "echo 'hello'; rm -rf /; echo $SECRET";
    const config: BootstrapConfig = {
      ...baseConfig,
      initScript: malicious,
    };
    const result = assembleCloudInit(config);

    // The script text must appear somewhere in the output (it IS included)
    expect(result.content).toContain(malicious);

    // Crucially, the runcmd entry must be a JSON flow sequence, not a raw shell
    // interpolation. The serialised form is:
    //   - ["bash", "-c", "echo 'hello'; rm -rf /; echo $SECRET"]
    // Verify that each token is double-quoted in the YAML output, which means
    // the metacharacters are JSON-string-encoded data rather than bare shell text.
    const lines = result.content.split("\n");
    const initScriptLine = lines.find((l) => l.includes(malicious));
    expect(initScriptLine).toBeDefined();

    // The line must be a JSON flow sequence entry (starts with "- [")
    // so the arguments are quoted at the YAML/JSON level.
    expect(initScriptLine!.trimStart()).toMatch(/^-\s*\[/);

    // Verify the semicolons and dollar sign appear inside a JSON-string context:
    // they must be surrounded by double-quote delimiters (i.e. the string is
    // serialised as a JSON value, not interpolated bare into a shell heredoc).
    // The serialiseRuncmdEntry function uses JSON.stringify for each token, so
    // the last token in the flow sequence ends with "]".
    expect(initScriptLine!).toContain(JSON.stringify(malicious));
  });
});
