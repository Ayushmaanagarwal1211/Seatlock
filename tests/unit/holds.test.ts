import { rowLabel } from '../../src/api/events/events.service';
import { checkSeats, HoldAttempt, SeatState } from '../../src/api/holds/hold-strategy';
import { hashRequest } from '../../src/api/request-hash';

describe('rowLabel', () => {
  it('counts rows like a cinema: A..Z, then AA, AB', () => {
    expect([0, 1, 25, 26, 27, 51, 52].map(rowLabel)).toEqual(['A', 'B', 'Z', 'AA', 'AB', 'AZ', 'BA']);
  });
});

describe('hashRequest', () => {
  it('is stable for the same fields and different for different ones', () => {
    expect(hashRequest(['e', 1, 2])).toBe(hashRequest(['e', 1, 2]));
    expect(hashRequest(['e', 1, 2])).not.toBe(hashRequest(['e', 1, 3]));
  });
});

describe('checkSeats', () => {
  const attempt: HoldAttempt = {
    holdId: 'h',
    eventId: 'event-1',
    userId: 'u',
    idempotencyKey: 'k',
    requestHash: 'x',
    seatIds: [1, 2],
    holdSeconds: 600,
  };
  const seat = (id: number, overrides: Partial<SeatState> = {}): SeatState => ({
    id: String(id),
    event_id: 'event-1',
    price_paise: 500,
    version: 0,
    free: true,
    ...overrides,
  });

  it('adds up the price when every seat is free', () => {
    expect(checkSeats(attempt, [seat(1), seat(2)])).toEqual({ kind: 'ok', amountPaise: 1000 });
  });

  it('lists the taken seats', () => {
    expect(checkSeats(attempt, [seat(1), seat(2, { free: false })])).toEqual({ kind: 'unavailable', seatIds: [2] });
  });

  it('rejects missing seats and seats of another event before anything else', () => {
    expect(checkSeats(attempt, [seat(1, { event_id: 'other', free: false })])).toEqual({
      kind: 'invalid_seats',
      seatIds: [1, 2],
    });
  });
});
