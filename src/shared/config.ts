import { z } from 'zod';

// Load .env if it exists. Variables already set in the shell win.
try {
  process.loadEnvFile();
} catch {
  // No .env file: rely on the shell environment.
}

const schema = z.object({
  PORT: z.coerce.number().int().positive().default(4000),
  // How many API processes to run (Node uses one CPU core per process; see src/api/server.ts)
  API_PROCESSES: z.coerce.number().int().positive().default(1),
  DATABASE_URL: z.string().min(1),
  // Max Postgres connections per process
  DB_POOL_SIZE: z.coerce.number().int().positive().default(10),
  REDIS_URL: z.string().min(1).default('redis://localhost:6380'),

  // How seat holds are protected against double booking (see docs/design.md):
  // pessimistic = SELECT ... FOR UPDATE, optimistic = version column, redis = Redis gate + optimistic
  HOLD_STRATEGY: z.enum(['pessimistic', 'optimistic', 'redis']).default('pessimistic'),
  // How long each API process reuses a computed seat map
  SEAT_MAP_CACHE_MS: z.coerce.number().int().nonnegative().default(1000),

  // Payment gateway (the mock in src/gateway in development)
  GATEWAY_URL: z.string().min(1).default('http://localhost:4100'),
  GATEWAY_API_KEY: z.string().min(1).default('sk_test_seatlock'),
  GATEWAY_WEBHOOK_SECRET: z.string().min(1).default('whsec_test_seatlock'),
  // Reject webhook signatures older than this (replay protection)
  WEBHOOK_TOLERANCE_SECONDS: z.coerce.number().int().positive().default(300),

  // Reconciler: asks the gateway about payments still pending after RECONCILE_AFTER_MS
  RECONCILE_INTERVAL_MS: z.coerce.number().int().positive().default(10_000),
  RECONCILE_AFTER_MS: z.coerce.number().int().nonnegative().default(120_000),

  // Outbox dispatcher: confirmation emails through Relay, refunds through the gateway.
  // Without RELAY_URL, emails are only logged.
  RELAY_URL: z.string().min(1).optional(),
  DISPATCH_INTERVAL_MS: z.coerce.number().int().positive().default(200),
  DISPATCH_BATCH_SIZE: z.coerce.number().int().positive().default(100),
  DISPATCH_CONCURRENCY: z.coerce.number().int().positive().default(20),

  // Flash sales. postgres = the hot-row baseline; redis-kafka = Redis gate, Kafka, batched writer
  FLASH_STRATEGY: z.enum(['postgres', 'redis-kafka']).default('redis-kafka'),
  KAFKA_BROKERS: z
    .string()
    .default('localhost:9092')
    .transform((s) => s.split(',').map((b) => b.trim())),
  FLASH_ORDERS_TOPIC: z.string().min(1).default('flash-orders'),
  FLASH_TOPIC_PARTITIONS: z.coerce.number().int().positive().default(6),
  // How long the API waits to fill a batch of orders before sending it to Kafka (like linger.ms)
  PRODUCER_LINGER_MS: z.coerce.number().int().nonnegative().default(5),
  ORDER_WRITER_GROUP: z.string().min(1).default('flash-order-writer'),
  ORDER_WRITER_BATCH_SIZE: z.coerce.number().int().positive().default(500),
  ORDER_WRITER_FLUSH_MS: z.coerce.number().int().positive().default(50),
});

// Fail fast at startup if config is missing, instead of failing on the first request.
const parsed = schema.safeParse(process.env);
if (!parsed.success) {
  console.error('Invalid environment variables:', z.flattenError(parsed.error).fieldErrors);
  process.exit(1);
}

export const config = parsed.data;
export type Config = typeof config;
