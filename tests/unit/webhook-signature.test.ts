import { signPayload, verifySignature } from '../../src/shared/webhook-signature';

describe('webhook signatures', () => {
  const secret = 'whsec_unit';
  const body = '{"id":"evt_1"}';
  const now = 1_800_000_000;

  it('accepts a signature made with the same secret', () => {
    expect(verifySignature(secret, body, signPayload(secret, body, now), 300, now)).toEqual({ ok: true });
  });

  it('rejects a changed body, another secret, and garbage', () => {
    const header = signPayload(secret, body, now);
    expect(verifySignature(secret, '{"id":"evt_2"}', header, 300, now)).toEqual({ ok: false, reason: 'mismatch' });
    expect(verifySignature('other', body, header, 300, now)).toEqual({ ok: false, reason: 'mismatch' });
    expect(verifySignature(secret, body, 't=abc,v1=zz', 300, now)).toEqual({ ok: false, reason: 'malformed' });
    expect(verifySignature(secret, body, undefined, 300, now)).toEqual({ ok: false, reason: 'missing' });
  });

  it('rejects a signature older than the tolerance (a replayed webhook)', () => {
    const header = signPayload(secret, body, now - 301);
    expect(verifySignature(secret, body, header, 300, now)).toEqual({ ok: false, reason: 'expired' });
  });
});
