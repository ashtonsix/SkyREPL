// tests/preload-quiet.ts — Redirect console output to a log file during tests.
//
// Captures console.log/info/debug/warn to /tmp/skyrepl-test-detail.log so `bun test` output
// stays clean. Only console.error passes through to stderr (for real problems).
// Set SKYREPL_TEST_VERBOSE=1 to disable and see everything.
//
// Also injects test boundary markers (▶/◀ with full describe > test paths) into
// the log file via a Bun.plugin onLoad transform that rewrites bun:test imports
// in test files to use a shim (tests/bun-test-shim.ts).

import { writeFileSync, appendFileSync } from "node:fs";
import { plugin } from "bun";
import { afterAll } from "bun:test";

const logPath = "/tmp/skyrepl-test-detail.log";

if (!process.env.SKYREPL_TEST_VERBOSE) {
  // Truncate on first load
  writeFileSync(logPath, "");

  const ts = () => new Date().toISOString().slice(11, 23); // HH:mm:ss.SSS

  const write = (level: string, args: unknown[]) => {
    const msg = args
      .map((a) => (typeof a === "string" ? a : JSON.stringify(a, null, 2)))
      .join(" ");
    appendFileSync(logPath, `${ts()} [${level}] ${msg}\n`);
  };

  console.log = (...args: unknown[]) => write("log", args);
  console.info = (...args: unknown[]) => write("info", args);
  console.debug = (...args: unknown[]) => write("debug", args);
  console.warn = (...args: unknown[]) => write("warn", args);
  // console.error intentionally left alone — real problems only

  // Rewrite bun:test imports in test files to use our shim that wraps
  // describe/test/it with START/END markers. bun:test is a built-in module
  // that can't be overridden via build.module or onResolve, and the globals
  // it installs bypass Object.defineProperty, so onLoad source transform
  // is the only way to inject wrappers.
  const shimPath = import.meta.dir + "/bun-test-shim.ts";
  plugin({
    name: "test-markers",
    setup(build) {
      build.onLoad({ filter: /\.test\.ts$/ }, async (args) => {
        const source = await Bun.file(args.path).text();
        const transformed = source.replace(
          /from\s+(["'])bun:test\1/g,
          `from "${shimPath}"`,
        );
        return { contents: transformed, loader: "ts" };
      });
    },
  });

  // Show log location after tests complete
  afterAll(() => {
    process.stderr.write(`  Detailed test logs: ${logPath}\n`);
  });
}
