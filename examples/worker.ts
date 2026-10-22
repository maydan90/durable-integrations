/**
 * A runnable worker process.
 *
 *   npm run build
 *   DATABASE_URL=postgresql://user:pass@localhost:5432/db node dist/examples/worker.js
 *
 * It applies the schema, seeds a few operations against a scripted provider
 * (one of which loses its answer on the first try), and then runs the loop
 * until SIGTERM or SIGINT. Ctrl-C once: the attempt in progress finishes,
 * the lease is released by the completion itself, and the process exits.
 * Anything still pending is picked up by the next worker that starts.
 */

import { hostname } from "node:os";
import { OperationStore, ScriptedProvider, createPool, migrate, runWorker, systemClock } from "../src/index.js";

const url = process.env["DATABASE_URL"];
if (!url) {
  console.error("set DATABASE_URL");
  process.exit(2);
}

const pool = createPool(url);
await migrate(pool);

const ops = new OperationStore(pool, { clock: systemClock, leaseMs: 30_000, maxAttempts: 5 });
const owner = `${hostname()}:${process.pid}`;

// A stand-in for the third party. The second write applies but its answer is
// lost, so the worker's next attempt will look it up instead of writing twice.
const provider = new ScriptedProvider(["ok", "lost_after_apply", "refuse", "ok"]);

if (process.env["SEED"] !== "0") {
  for (const table of [1, 2, 3]) {
    await ops.enqueue("booking.create", { table, party: 2, at: "2026-03-01T19:00:00Z" });
  }
}

const worker = runWorker(ops, provider, {
  owner,
  onResult: (op) => console.log(`${op.kind} ${op.id.slice(0, 8)} -> ${op.state} after ${op.attempts} attempt(s)`),
  onError: (err, op) => console.error(`${op ? op.id.slice(0, 8) : "claim"}:`, err instanceof Error ? err.message : err),
});
console.log(`worker ${owner} started; Ctrl-C to stop`);

let stopping = false;
const shutdown = async (signal: string) => {
  if (stopping) return;
  stopping = true;
  console.log(`${signal}: finishing the current attempt, then exiting`);
  await worker.stop();
  await pool.end();
  process.exit(0);
};
process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));

await worker.done;
