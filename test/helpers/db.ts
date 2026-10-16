import { inject } from "vitest";
import { createPool, migrate, type Pool } from "../../src/index.js";

export async function setupDb(): Promise<Pool> {
  const pool = createPool(inject("databaseUrl"));
  await migrate(pool);
  return pool;
}

export async function truncateAll(pool: Pool): Promise<void> {
  await pool.query(
    `truncate idempotency_keys, provider_operations, webhook_deliveries, webhook_events, webhook_endpoints`,
  );
}
