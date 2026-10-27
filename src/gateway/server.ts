import { z } from 'zod';
import { createGateway } from './gateway-app';

// The mock payment gateway runs as its own process, like a real third-party service would.
try {
  process.loadEnvFile();
} catch {
  // No .env file: rely on the shell environment.
}

const env = z
  .object({
    GATEWAY_PORT: z.coerce.number().int().positive().default(4100),
    GATEWAY_API_KEY: z.string().min(1).default('sk_test_seatlock'),
    GATEWAY_WEBHOOK_SECRET: z.string().min(1).default('whsec_test_seatlock'),
    GATEWAY_WEBHOOK_URL: z.url().default('http://localhost:4000/v1/webhooks/gateway'),
    WEBHOOK_DUPLICATE_RATE: z.coerce.number().min(0).max(1).default(0),
    WEBHOOK_MAX_DELAY_MS: z.coerce.number().int().nonnegative().default(0),
    WEBHOOK_DROP_RATE: z.coerce.number().min(0).max(1).default(0),
    PAYMENT_EXPIRY_MS: z.coerce.number().int().positive().default(15 * 60_000),
  })
  .parse(process.env);

const gateway = createGateway({
  apiKey: env.GATEWAY_API_KEY,
  webhookUrl: env.GATEWAY_WEBHOOK_URL,
  webhookSecret: env.GATEWAY_WEBHOOK_SECRET,
  chaos: {
    duplicateRate: env.WEBHOOK_DUPLICATE_RATE,
    maxDelayMs: env.WEBHOOK_MAX_DELAY_MS,
    dropRate: env.WEBHOOK_DROP_RATE,
  },
  paymentExpiryMs: env.PAYMENT_EXPIRY_MS,
});

const server = gateway.app.listen(env.GATEWAY_PORT, () => {
  console.log(
    `Mock payment gateway on http://localhost:${env.GATEWAY_PORT} ` +
      `(webhooks: ${env.WEBHOOK_DUPLICATE_RATE * 100}% duplicated, up to ${env.WEBHOOK_MAX_DELAY_MS}ms late, ` +
      `${env.WEBHOOK_DROP_RATE * 100}% dropped)`,
  );
});

const shutdown = () => {
  gateway.close();
  server.close(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
