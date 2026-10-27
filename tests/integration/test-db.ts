// Integration tests use their own database (seatlock_test) so they never wipe your dev data.
export function testDatabaseUrl(): string {
  try {
    process.loadEnvFile();
  } catch {
    // No .env: fall back to the docker-compose default below.
  }
  return process.env.TEST_DATABASE_URL ?? 'postgres://seatlock:seatlock@localhost:5433/seatlock_test';
}
