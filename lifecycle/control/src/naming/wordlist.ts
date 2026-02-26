// naming/wordlist.ts — Adjective-noun display name generator + collision detection
//
// Generates human-readable names like "serene-otter", "bold-falcon".
// 175 adjectives × 157 nouns = 27,475 combinations.
// Per-tenant uniqueness is enforced against active (non-terminated) instances/manifests.

import { queryOne } from "../material/db/helpers";

// =============================================================================
// Word Lists
// =============================================================================

const ADJECTIVES: string[] = [
  "agile", "amber", "ancient", "arctic", "ardent",
  "azure", "bare", "bold", "brave", "bright",
  "brisk", "broad", "calm", "candid", "careful",
  "cedar", "celestial", "cheerful", "clean", "clear",
  "clever", "coastal", "cold", "crisp", "crystal",
  "curious", "dark", "dawn", "deep", "deft",
  "dense", "devoted", "distant", "divine", "durable",
  "eager", "early", "easy", "ember", "emerald",
  "epic", "even", "fair", "fast", "fertile",
  "fiery", "firm", "flint", "fluid", "flying",
  "fond", "forest", "free", "fresh", "frosty",
  "gentle", "glad", "golden", "graceful", "grand",
  "green", "happy", "hardy", "hollow", "honest",
  "hopeful", "humble", "icy", "idle", "iron",
  "jovial", "joyful", "keen", "kind", "large",
  "leafy", "light", "lively", "lone", "loyal",
  "lunar", "lush", "magnetic", "majestic", "marble",
  "mellow", "merry", "midnight", "mighty", "misty",
  "modest", "moonlit", "mossy", "mountain", "noble",
  "northern", "oak", "ocean", "open", "orange",
  "pacific", "pale", "patient", "peaceful", "peak",
  "pine", "plain", "playful", "polar", "prime",
  "proud", "purple", "quick", "quiet", "radiant",
  "rapid", "ready", "regal", "rich", "rooted",
  "rugged", "rural", "sacred", "sage", "sandy",
  "serene", "shady", "sharp", "silent", "silver",
  "simple", "sleek", "slim", "smooth", "snowy",
  "solar", "solid", "sonic", "southern", "spacious",
  "sparkling", "speedy", "steady", "stellar", "still",
  "stone", "stormy", "strong", "sturdy", "subtle",
  "sunny", "swift", "tender", "tiny", "topaz",
  "tranquil", "true", "trusty", "twilight", "unique",
  "upbeat", "vast", "verdant", "vibrant", "warm",
  "wary", "wild", "willing", "windy", "wise",
  "witty", "wooden", "young", "zealous", "zenith",
];

const NOUNS: string[] = [
  "albatross", "antelope", "ape", "armadillo", "arrow",
  "atlas", "aurora", "badger", "bat", "bear",
  "beaver", "bison", "boar", "bobcat", "brook",
  "buck", "buffalo", "canyon", "capybara", "caribou",
  "cedar", "cheetah", "chimney", "chipmunk", "cliff",
  "cloud", "comet", "condor", "coral", "cougar",
  "crane", "creek", "crow", "crystal", "deer",
  "delta", "dingo", "dove", "dune", "eagle",
  "elk", "falcon", "fern", "finch", "fjord",
  "flamingo", "flint", "forest", "fox", "gale",
  "gazelle", "gecko", "glacier", "gopher", "gorge",
  "grove", "gull", "hare", "hawk", "heron",
  "ibis", "jackal", "jaguar", "juniper", "kestrel",
  "kingfisher", "kite", "lagoon", "lark", "lemur",
  "leopard", "lichen", "lily", "lion", "lynx",
  "magpie", "maple", "marten", "meadow", "merlin",
  "mesa", "mink", "mole", "mongoose", "moose",
  "moth", "mountain", "mouse", "newt", "nighthawk",
  "oak", "ocelot", "osprey", "otter", "owl",
  "panther", "parrot", "peak", "pelican", "penguin",
  "peregrine", "petal", "pine", "plover", "porcupine",
  "puma", "quail", "rabbit", "raccoon", "raven",
  "reed", "ridge", "river", "robin", "rock",
  "roebuck", "sapling", "seal", "sequoia", "shrew",
  "sierra", "skunk", "slate", "snow", "snipe",
  "sparrow", "spring", "spruce", "squirrel", "starling",
  "stork", "stream", "summit", "swallow", "swan",
  "swift", "teal", "thrush", "timber", "toad",
  "torrent", "tortoise", "trout", "tundra", "turtle",
  "valley", "viper", "vole", "vulture", "warbler",
  "weasel", "wolf", "wolverine", "wombat", "wren",
  "yak", "zebra",
];

// =============================================================================
// Name Generation
// =============================================================================

/**
 * Generate a random adjective-noun name like "serene-otter".
 * Uses crypto.getRandomValues for good randomness.
 */
export function generateCandidateName(): string {
  const bytes = new Uint8Array(4);
  crypto.getRandomValues(bytes);
  const n = ((bytes[0]! << 24 | bytes[1]! << 16 | bytes[2]! << 8 | bytes[3]!) >>> 0);
  const adjIdx = n % ADJECTIVES.length;
  const nounIdx = Math.floor(n / ADJECTIVES.length) % NOUNS.length;
  return `${ADJECTIVES[adjIdx]}-${NOUNS[nounIdx]}`;
}

// =============================================================================
// Collision Detection
// =============================================================================

/**
 * Check if a display_name is already in use by an active instance for this tenant.
 * "Active" means workflow_state does not start with "terminate:".
 */
function isNameTakenByInstance(tenantId: number, name: string): boolean {
  const row = queryOne<{ id: number }>(
    `SELECT id FROM instances
     WHERE tenant_id = ? AND display_name = ?
     AND workflow_state NOT LIKE 'terminate:%'`,
    [tenantId, name]
  );
  return row !== null;
}

/**
 * Check if a display_name is already in use by an active manifest for this tenant.
 * "Active" means status is not 'sealed', 'expired', or 'deleted'.
 */
function isNameTakenByManifest(tenantId: number, name: string): boolean {
  const row = queryOne<{ id: number }>(
    `SELECT id FROM manifests
     WHERE tenant_id = ? AND display_name = ?
     AND status NOT IN ('sealed', 'expired', 'deleted')`,
    [tenantId, name]
  );
  return row !== null;
}

/**
 * Generate a unique display_name for this tenant, checking both instances and manifests.
 * Tries up to maxAttempts random names before throwing.
 */
export function generateUniqueName(
  tenantId: number,
  resourceType: "instance" | "manifest",
  maxAttempts = 20
): string {
  for (let i = 0; i < maxAttempts; i++) {
    const candidate = generateCandidateName();
    const takenByInstance = isNameTakenByInstance(tenantId, candidate);
    const takenByManifest = isNameTakenByManifest(tenantId, candidate);
    if (!takenByInstance && !takenByManifest) {
      return candidate;
    }
  }
  // Fallback: append a random 4-digit suffix to break collision; check DB, retry up to 3 times
  for (let j = 0; j < 3; j++) {
    const base = generateCandidateName();
    const suffix = Math.floor(Math.random() * 9000 + 1000).toString();
    const candidate = `${base}-${suffix}`;
    if (!isNameTakenByInstance(tenantId, candidate) && !isNameTakenByManifest(tenantId, candidate)) {
      return candidate;
    }
  }
  throw new Error(`generateUniqueName: exhausted all attempts for tenant ${tenantId}`);
}

// =============================================================================
// Exports for testing
// =============================================================================

export { ADJECTIVES, NOUNS };
