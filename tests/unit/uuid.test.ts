import { uuidFromName } from '../../src/shared/uuid';

describe('uuidFromName', () => {
  it('gives the same valid version-5 UUID for the same name, and different ones otherwise', () => {
    const id = uuidFromName('hold:alice:key-1');
    expect(id).toBe(uuidFromName('hold:alice:key-1'));
    expect(id).not.toBe(uuidFromName('hold:alice:key-2'));
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });
});
