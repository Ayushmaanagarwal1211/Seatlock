import { createHash } from 'node:crypto';

// Fingerprint of a request. If a client reuses an idempotency key for a different request,
// the hashes differ and we reject it instead of silently returning the old result.
// Callers pass the fields in a fixed order, so the same request always gives the same hash.
export function hashRequest(fields: unknown[]): string {
  return createHash('sha256').update(JSON.stringify(fields)).digest('hex');
}
