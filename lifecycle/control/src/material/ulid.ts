// material/ulid.ts — Shared ULID generator
//
// Crockford base32, 48-bit timestamp + 80-bit random.
// Monotonically increasing within the same millisecond (random portion is incremented).
//
// Extracted from db/audit.ts and billing/settlement.ts to eliminate duplication.

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

let lastMs = 0;
let lastRand = new Uint8Array(10);

export function generateULID(): string {
  const nowMs = Date.now();
  if (nowMs <= lastMs) {
    // Increment random portion for monotonicity within same millisecond
    for (let i = 9; i >= 0; i--) {
      if (lastRand[i] < 255) { lastRand[i]++; break; }
      lastRand[i] = 0;
    }
  } else {
    lastMs = nowMs;
    crypto.getRandomValues(lastRand);
  }

  let ts = nowMs;
  let timePart = "";
  for (let i = 0; i < 10; i++) {
    timePart = CROCKFORD[ts % 32] + timePart;
    ts = Math.floor(ts / 32);
  }

  // Extract 5 bits at a time from the 80-bit (10-byte) random buffer
  const bits: number[] = [];
  for (const byte of lastRand) {
    for (let b = 7; b >= 0; b--) {
      bits.push((byte >> b) & 1);
    }
  }
  let randomPart = "";
  for (let i = 0; i < 16; i++) {
    let val = 0;
    for (let b = 0; b < 5; b++) {
      val = (val << 1) | (bits[i * 5 + b] ?? 0);
    }
    randomPart += CROCKFORD[val];
  }

  return timePart + randomPart;
}
