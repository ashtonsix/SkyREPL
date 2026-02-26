// tests/unit/ssh-slug.test.ts
//
// Unit tests for SSH slug resolution priority chain (BL-33):
//   alias (from config) > display_name > base36 slug

import { describe, test, expect } from 'bun:test';
import { resolveSSHSlug } from '../.././../shell/cli/src/ssh';
import { idToSlug } from '@skyrepl/contracts';

// =============================================================================
// resolveSSHSlug priority chain
// =============================================================================

describe("resolveSSHSlug — priority chain: alias > display_name > slug", () => {
  test("returns display_name when present and no alias", () => {
    const alloc = { id: 1, display_name: "serene-otter" };
    expect(resolveSSHSlug(alloc)).toBe("serene-otter");
  });

  test("returns base36 slug when no display_name and no alias", () => {
    const alloc = { id: 42, display_name: null };
    expect(resolveSSHSlug(alloc)).toBe(idToSlug(42));
  });

  test("returns base36 slug when display_name is undefined", () => {
    const alloc = { id: 42 };
    expect(resolveSSHSlug(alloc)).toBe(idToSlug(42));
  });

  test("alias takes priority over display_name", () => {
    const alloc = { id: 1, display_name: "serene-otter" };
    const aliases = { "mybox": "serene-otter" };
    expect(resolveSSHSlug(alloc, aliases)).toBe("mybox");
  });

  test("alias matched by slug also works (no display_name)", () => {
    const slug = idToSlug(5);
    const alloc = { id: 5, display_name: null };
    const aliases = { "devbox": slug };
    expect(resolveSSHSlug(alloc, aliases)).toBe("devbox");
  });

  test("non-matching alias does not affect result — display_name used", () => {
    const alloc = { id: 1, display_name: "bold-falcon" };
    const aliases = { "other-alias": "serene-otter" }; // different target
    expect(resolveSSHSlug(alloc, aliases)).toBe("bold-falcon");
  });

  test("non-matching alias does not affect result — slug used when no display_name", () => {
    const alloc = { id: 7, display_name: null };
    const aliases = { "other-alias": "serene-otter" }; // different target
    expect(resolveSSHSlug(alloc, aliases)).toBe(idToSlug(7));
  });

  test("empty aliases map falls through to display_name", () => {
    const alloc = { id: 3, display_name: "swift-hawk" };
    expect(resolveSSHSlug(alloc, {})).toBe("swift-hawk");
  });

  test("empty aliases map falls through to slug", () => {
    const alloc = { id: 3, display_name: null };
    expect(resolveSSHSlug(alloc, {})).toBe(idToSlug(3));
  });

  test("multiple aliases — first matching wins", () => {
    const alloc = { id: 2, display_name: "calm-wolf" };
    const aliases = {
      "box-a": "other-thing",
      "box-b": "calm-wolf",  // matches display_name
      "box-c": "calm-wolf",  // also matches
    };
    // Should return first matching alias (Object.entries order)
    expect(resolveSSHSlug(alloc, aliases)).toBe("box-b");
  });
});
