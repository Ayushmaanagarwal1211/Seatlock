// Integration tests use their own database (seatlock_test) and Redis database (1) so they
// never wipe your dev data.
function loadEnv() {
  try {
    process.loadEnvFile();
  } catch {
    // No .env: fall back to the docker-compose defaults below.
  }
}

export function testDatabaseUrl(): string {
  loadEnv();
  return process.env.TEST_DATABASE_URL ?? 'postgres://seatlock:seatlock@localhost:5433/seatlock_test';
}

export function testRedisUrl(): string {
  loadEnv();
  return process.env.TEST_REDIS_URL ?? 'redis://localhost:6380/1';
}
