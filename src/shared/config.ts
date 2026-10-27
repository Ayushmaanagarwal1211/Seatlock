import { z } from 'zod';

// Load .env if it exists. Variables already set in the shell win.
try {
  process.loadEnvFile();
} catch {
  // No .env file: rely on the shell environment.
}

const schema = z.object({
  DATABASE_URL: z.string().min(1),
  // Max Postgres connections per process
  DB_POOL_SIZE: z.coerce.number().int().positive().default(10),
});

// Fail fast at startup if config is missing, instead of failing on the first request.
const parsed = schema.safeParse(process.env);
if (!parsed.success) {
  console.error('Invalid environment variables:', z.flattenError(parsed.error).fieldErrors);
  process.exit(1);
}

export const config = parsed.data;
