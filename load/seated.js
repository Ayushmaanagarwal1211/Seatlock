// k6: thousands of users trying to book seats for the same event.
// Don't run this directly; use `npm run stress -- seated` (it starts everything, runs k6,
// waits for payments and emails to settle, then checks every booking).
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const API_URL = __ENV.API_URL || 'http://localhost:4000';
const EVENT_ID = __ENV.EVENT_ID;
const RATE = Number(__ENV.RATE || 200); // new users per second
const DURATION = __ENV.DURATION || '60s';
const HOLD_SECONDS = Number(__ENV.HOLD_SECONDS || 15);
// What users do after holding seats
const PAY_FAIL_SHARE = Number(__ENV.PAY_FAIL_SHARE || 0.05); // card declined (then they retry once)
const ABANDON_SHARE = Number(__ENV.ABANDON_SHARE || 0.1); // never pay; seats go back on sale
const LATE_SHARE = Number(__ENV.LATE_SHARE || 0.03); // pay after the hold expired
// Most users want the best seats: HOT_SHARE of them aim at the front HOT_SEATS share of the stadium.
const HOT_SHARE = Number(__ENV.HOT_SHARE || 0.6);
const HOT_SEATS = Number(__ENV.HOT_SEATS || 0.1);

const holdCreated = new Counter('holds_created');
const holdConflict = new Counter('holds_conflict');
const holdError = new Counter('holds_error');
const paid = new Counter('payments_succeeded');
const declined = new Counter('payments_declined');
const abandoned = new Counter('holds_abandoned');
const latePaid = new Counter('payments_late');
const sawSoldOut = new Counter('saw_sold_out'); // no free seats left on the map
const holdLatency = new Trend('hold_latency', true);

export const options = {
  discardResponseBodies: false,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    users: {
      executor: 'constant-arrival-rate', // users arrive at this rate whatever the server does
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.min(Math.max(50, RATE), 1000),
      // Late payers wait HOLD_SECONDS, so many users can be in progress at once
      maxVUs: Math.min(Math.max(200, RATE * (HOLD_SECONDS + 5)), 5000),
    },
  },
};

export function setup() {
  const res = http.get(`${API_URL}/v1/events/${EVENT_ID}/seats`);
  return { seatIds: res.json('seats').map((s) => s.id) };
}

// Like a user on the seat picker: look at what's free (the availability list, up to ~1s old),
// prefer the front section, and pick 1-4 free seats side by side.
function pickSeats(data) {
  const res = http.get(`${API_URL}/v1/events/${EVENT_ID}/availability`, { tags: { name: 'availability' } });
  if (res.status !== 200) return null;
  const available = res.json('availableSeatIds');
  if (available.length === 0) return null;
  const free = new Set(available);
  const hotLimit = data.seatIds[Math.floor(data.seatIds.length * HOT_SEATS)];
  const hot = available.filter((id) => id <= hotLimit);
  const candidates = Math.random() < HOT_SHARE && hot.length > 0 ? hot : available;

  const count = 1 + Math.floor(Math.random() * 4);
  const start = candidates[Math.floor(Math.random() * candidates.length)];
  const seats = [start];
  while (seats.length < count && free.has(seats[seats.length - 1] + 1)) seats.push(seats[seats.length - 1] + 1);
  return seats;
}

function post(path, body, headers) {
  return http.post(`${API_URL}${path}`, JSON.stringify(body), {
    headers: Object.assign({ 'Content-Type': 'application/json' }, headers),
    tags: { name: path.split('/').slice(0, 3).join('/') },
  });
}

export default function (data) {
  const user = `user-${__VU}-${__ITER}`;

  // Up to 3 tries: if the seats are taken, pick others (like a user would)
  let hold = null;
  for (let attempt = 0; attempt < 3 && !hold; attempt++) {
    const seatIds = pickSeats(data);
    if (!seatIds) {
      sawSoldOut.add(1);
      return;
    }
    const res = post(`/v1/events/${EVENT_ID}/holds`, { seatIds }, {
      'X-User-Id': user,
      'Idempotency-Key': `${user}-${attempt}`,
    });
    holdLatency.add(res.timings.duration);
    if (res.status === 201) {
      holdCreated.add(1);
      hold = res.json();
    } else if (res.status === 409) {
      holdConflict.add(1);
    } else {
      holdError.add(1);
    }
  }
  if (!hold) return;

  const r = Math.random();
  if (r < ABANDON_SHARE) {
    abandoned.add(1);
    return;
  }
  const late = r < ABANDON_SHARE + LATE_SHARE;
  const declinedFirst = !late && r < ABANDON_SHARE + LATE_SHARE + PAY_FAIL_SHARE;

  let checkout = post(`/v1/holds/${hold.id}/checkout`, { email: `${user}@example.com` }, { 'X-User-Id': user });
  if (checkout.status !== 201) return;

  if (late) {
    sleep(HOLD_SECONDS + 2); // the hold has expired by the time they pay
    latePaid.add(1);
  } else if (declinedFirst) {
    http.post(checkout.json('payUrl'), JSON.stringify({ outcome: 'failed' }), { headers: { 'Content-Type': 'application/json' } });
    declined.add(1);
    sleep(0.5);
    checkout = post(`/v1/holds/${hold.id}/checkout`, { email: `${user}@example.com` }, { 'X-User-Id': user });
    if (checkout.status !== 201) return;
  }
  const pay = http.post(checkout.json('payUrl'), JSON.stringify({ outcome: 'succeeded' }), {
    headers: { 'Content-Type': 'application/json' },
  });
  if (pay.status === 200) paid.add(1);
}

export function handleSummary(data) {
  const m = data.metrics;
  const count = (name) => (m[name] ? m[name].values.count : 0);
  const hold = m.hold_latency ? m.hold_latency.values : {};
  const result = {
    holdsCreated: count('holds_created'),
    holdsConflict: count('holds_conflict'),
    holdsError: count('holds_error'),
    paymentsSucceeded: count('payments_succeeded'),
    paymentsDeclined: count('payments_declined'),
    paymentsLate: count('payments_late'),
    holdsAbandoned: count('holds_abandoned'),
    sawSoldOut: count('saw_sold_out'),
    holdP50: hold.med,
    holdP95: hold['p(95)'],
    holdP99: hold['p(99)'],
    requests: m.http_reqs.values.count,
    dropped: count('dropped_iterations'),
  };
  return { stdout: `\nK6_RESULT ${JSON.stringify(result)}\n` };
}
