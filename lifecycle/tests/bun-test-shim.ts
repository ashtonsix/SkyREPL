// tests/bun-test-shim.ts — Wraps bun:test describe/test/it with log markers.
//
// Test files are transparently redirected here by the preload-quiet.ts onLoad
// plugin. This file re-exports everything from bun:test, overriding describe,
// test, and it with versions that write ▶ START / ◀ END markers to the log file.

import { appendFileSync } from "node:fs";
import * as bunTest from "bun:test";

// Re-export everything unchanged first
export {
  expect,
  mock,
  spyOn,
  jest,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
  setSystemTime,
} from "bun:test";

const logPath = "/tmp/skyrepl-test-detail.log";
const ts = () => new Date().toISOString().slice(11, 23);

const describeStack: string[] = [];

const copyStatics = (from: any, to: any) => {
  // Copy own properties
  for (const key of Object.getOwnPropertyNames(from)) {
    if (key !== "length" && key !== "name" && key !== "prototype") {
      try {
        to[key] = from[key];
      } catch {}
    }
  }
  // Forward known conditional modifiers (may be getters/prototype-based)
  for (const key of ["skip", "skipIf", "only", "todo", "todoIf", "if", "each"]) {
    if (key in from && !(key in to)) {
      try {
        to[key] = typeof from[key] === "function" ? from[key].bind(from) : from[key];
      } catch {}
    }
  }
};

// Wrap describe to track nesting
const _describe = bunTest.describe;
const wrappedDescribe = ((name: string, fn: () => void) =>
  _describe(name, () => {
    describeStack.push(name);
    try {
      fn();
    } finally {
      describeStack.pop();
    }
  })) as typeof bunTest.describe;
copyStatics(_describe, wrappedDescribe);

// Wrap test/it to log START/END with full describe > test path
const wrapTestFn = (orig: typeof bunTest.test): typeof bunTest.test => {
  const wrapped = ((name: string, fn: (...a: any[]) => any, timeout?: number) => {
    const path = [...describeStack, name].join(" > ");
    return orig(
      name,
      (...args: any[]) => {
        appendFileSync(logPath, `\n${ts()} ▶ ${path}\n`);
        let result: any;
        try {
          result = fn(...args);
        } catch (e) {
          appendFileSync(logPath, `${ts()} ◀ ${path}\n`);
          throw e;
        }
        if (result && typeof result.then === "function") {
          return result.finally(() => {
            appendFileSync(logPath, `${ts()} ◀ ${path}\n`);
          });
        }
        appendFileSync(logPath, `${ts()} ◀ ${path}\n`);
        return result;
      },
      timeout,
    );
  }) as typeof bunTest.test;
  copyStatics(orig, wrapped);
  return wrapped;
};

export const describe = wrappedDescribe;
export const test = wrapTestFn(bunTest.test);
export const it = wrapTestFn(bunTest.it);
