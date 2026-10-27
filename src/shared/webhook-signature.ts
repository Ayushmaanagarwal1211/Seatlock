import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * Webhook signatures, the way Stripe does them.
 *
 *   Gateway-Signature: t=1767225600,v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a0e56ff536d0ce8e108d8bd
 *
 * v1 = HMAC-SHA256(secret, "<t>.<raw body>"). Only someone with the shared secret can make
 * a valid signature, so nobody can fake a "payment succeeded" call to our webhook.
 *
 * The timestamp is signed too, and we reject signatures older than the tolerance (5 min).
 * Without that, an attacker who captured one real webhook could replay it forever.
 */
export const SIGNATURE_HEADER = 'Gateway-Signature';

export function signPayload(secret: string, rawBody: string, timestamp = Math.floor(Date.now() / 1000)): string {
  const signature = createHmac('sha256', secret).update(`${timestamp}.${rawBody}`).digest('hex');
  return `t=${timestamp},v1=${signature}`;
}

export type VerifyResult = { ok: true } | { ok: false; reason: 'missing' | 'malformed' | 'expired' | 'mismatch' };

export function verifySignature(
  secret: string,
  rawBody: string,
  header: string | undefined,
  toleranceSeconds: number,
  now = Math.floor(Date.now() / 1000),
): VerifyResult {
  if (!header) return { ok: false, reason: 'missing' };
  const parts = Object.fromEntries(header.split(',').map((part) => part.split('=', 2) as [string, string]));
  const timestamp = Number(parts.t);
  if (!Number.isInteger(timestamp) || !/^[0-9a-f]{64}$/.test(parts.v1 ?? '')) return { ok: false, reason: 'malformed' };
  if (Math.abs(now - timestamp) > toleranceSeconds) return { ok: false, reason: 'expired' };

  const expected = createHmac('sha256', secret).update(`${timestamp}.${rawBody}`).digest();
  // Constant-time comparison: a normal === stops at the first different byte, and an
  // attacker could time our responses to guess a valid signature byte by byte.
  return timingSafeEqual(expected, Buffer.from(parts.v1, 'hex')) ? { ok: true } : { ok: false, reason: 'mismatch' };
}
