import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true, // describe/it/expect without imports
    projects: [
      {
        test: { name: 'unit', include: ['tests/unit/**/*.test.ts'] },
      },
      {
        // Needs Postgres, Redis and Kafka (npm run infra:up). Uses the separate seatlock_test
        // database and Redis database 1. Files run one at a time: they share the database.
        test: {
          name: 'integration',
          include: ['tests/integration/**/*.test.ts'],
          globalSetup: ['tests/integration/global-setup.ts'],
          fileParallelism: false,
          testTimeout: 30_000,
          hookTimeout: 30_000,
        },
      },
    ],
  },
});
