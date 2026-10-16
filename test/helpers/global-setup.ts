import type { GlobalSetupContext } from "vitest/node";

declare module "vitest" {
  export interface ProvidedContext {
    databaseUrl: string;
  }
}

/**
 * Starts a throwaway PostgreSQL in Docker via Testcontainers. Set DATABASE_URL
 * to use an existing server instead (CI without Docker, or a local instance).
 */
export default async function setup({ provide }: GlobalSetupContext): Promise<() => Promise<void>> {
  const fromEnv = process.env["DATABASE_URL"];
  if (fromEnv) {
    provide("databaseUrl", fromEnv);
    return async () => undefined;
  }
  const { PostgreSqlContainer } = await import("@testcontainers/postgresql");
  const container = await new PostgreSqlContainer("postgres:16-alpine").start();
  provide("databaseUrl", container.getConnectionUri());
  return async () => {
    await container.stop();
  };
}
