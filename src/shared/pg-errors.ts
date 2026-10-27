// Postgres error codes the application reacts to.
// https://www.postgresql.org/docs/current/errcodes-appendix.html
export const UNIQUE_VIOLATION = '23505';
export const CHECK_VIOLATION = '23514';
export const DEADLOCK_DETECTED = '40P01';
export const SERIALIZATION_FAILURE = '40001';

export function pgErrorCode(err: unknown): string | undefined {
  const code = (err as { code?: unknown })?.code;
  return typeof code === 'string' ? code : undefined;
}
