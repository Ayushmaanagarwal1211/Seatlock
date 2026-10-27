import { createHash } from 'node:crypto';

// Fixed namespace for Seatlock's name-based ids (any constant UUID works).
const NAMESPACE = Buffer.from('6f1c2b0e9a4d4c1e8f3a5b7d9e0c1a2b', 'hex');

/**
 * A UUID derived from a name (RFC 9562 version 5): the same name always gives the same id.
 *
 * Used where retries must land on the same row instead of creating a new one: a hold's id
 * comes from (user, idempotency key), a flash-sale order's id from (sale, user).
 */
export function uuidFromName(name: string): string {
  const hash = createHash('sha1').update(NAMESPACE).update(name).digest();
  hash[6] = (hash[6] & 0x0f) | 0x50; // version 5
  hash[8] = (hash[8] & 0x3f) | 0x80; // RFC variant
  const hex = hash.subarray(0, 16).toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
