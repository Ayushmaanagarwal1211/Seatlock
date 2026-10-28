// k6: a flash-sale crowd. Every iteration is a new buyer; some buyers click twice.
// Don't run this directly; use `npm run stress -- flash`.
import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';

const API_URL = __ENV.API_URL || 'http://localhost:4000';
const SALE_ID = __ENV.SALE_ID;
const RATE = Number(__ENV.RATE || 2000); // buy attempts per second
const DURATION = __ENV.DURATION || '30s';
const DOUBLE_CLICK_SHARE = Number(__ENV.DOUBLE_CLICK_SHARE || 0.05);

const reserved = new Counter('buys_reserved'); // 202: got one
const replayed = new Counter('buys_replayed'); // 200: clicked again, same order
const soldOut = new Counter('buys_sold_out'); // 409
const busy = new Counter('buys_busy'); // 503
const failed = new Counter('buys_error');
const buyLatency = new Trend('buy_latency', true);

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    crowd: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.min(Math.max(50, RATE / 5), 2000),
      maxVUs: Math.min(Math.max(100, RATE * 2), 4000),
    },
  },
};

function buy(user) {
  const res = http.post(`${API_URL}/v1/flash-sales/${SALE_ID}/orders`, null, { headers: { 'X-User-Id': user } });
  buyLatency.add(res.timings.duration);
  if (res.status === 202) reserved.add(1);
  else if (res.status === 200) replayed.add(1);
  else if (res.status === 409) soldOut.add(1);
  else if (res.status === 503) busy.add(1);
  else failed.add(1);
  return res.status;
}

export default function () {
  const user = `buyer-${__VU}-${__ITER}`;
  const status = buy(user);
  // An impatient user clicks again: must get the same order, not a second one.
  if (status === 202 && Math.random() < DOUBLE_CLICK_SHARE) buy(user);
}

export function handleSummary(data) {
  const m = data.metrics;
  const count = (name) => (m[name] ? m[name].values.count : 0);
  const t = m.buy_latency.values;
  const result = {
    reserved: count('buys_reserved'),
    replayed: count('buys_replayed'),
    soldOut: count('buys_sold_out'),
    busy: count('buys_busy'),
    errors: count('buys_error'),
    p50: t.med,
    p95: t['p(95)'],
    p99: t['p(99)'],
    max: t.max,
    requests: m.http_reqs.values.count,
    rate: m.http_reqs.values.rate,
    dropped: count('dropped_iterations'),
  };
  return { stdout: `\nK6_RESULT ${JSON.stringify(result)}\n` };
}
