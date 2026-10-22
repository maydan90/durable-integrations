import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  OperationStore,
  ScriptedProvider,
  StaleLeaseError,
  runWorker,
  systemClock,
  type Operation,
  type Pool,
  type Provider,
} from "../src/index.js";
import { setupDb, truncateAll } from "./helpers/db.js";

/**
 * These tests use the system clock on purpose. Lease renewal is about real
 * elapsed time while a provider is slow, so the worker is run against a
 * short lease (300 ms) and a provider that answers after 800 ms.
 */

let pool: Pool;
let ops: OperationStore;

const payload = { table: 7, party: 2, at: "2026-03-01T19:00:00Z" };
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Wraps a provider so every perform takes `delayMs` before answering. */
function slow<P, R>(inner: Provider<P, R>, delayMs: number): Provider<P, R> {
  return {
    async perform(kind, p, requestId) {
      await sleep(delayMs);
      return inner.perform(kind, p, requestId);
    },
    lookup: (requestId) => inner.lookup(requestId),
  };
}

beforeAll(async () => {
  pool = await setupDb();
});
afterAll(async () => {
  await pool.end();
});
beforeEach(async () => {
  await truncateAll(pool);
  ops = new OperationStore(pool, { clock: systemClock, leaseMs: 300, maxAttempts: 3 });
});

describe("worker loop", () => {
  it("without renewal, an attempt slower than the lease cannot record its outcome", async () => {
    const provider = slow(new ScriptedProvider(["ok"]), 800);
    const op = await ops.enqueue("booking.create", payload);
    await expect(ops.runOne("worker-1", provider)).rejects.toBeInstanceOf(StaleLeaseError);
    // The row is still in_flight with an expired lease: the next claim gets it, in recovery mode.
    const again = await ops.claim("worker-2");
    expect(again?.id).toBe(op.id);
    expect(again?.recovering).toBe(true);
  });

  it("keeps the lease alive while the provider is slow, then records success", async () => {
    const scripted = new ScriptedProvider(["ok"]);
    const results: Operation[] = [];
    const errors: unknown[] = [];
    let handle!: ReturnType<typeof runWorker>;
    handle = runWorker(ops, slow(scripted, 800), {
      owner: "worker-1",
      renewEveryMs: 100,
      idleMs: 50,
      onResult: (op) => {
        results.push(op);
        void handle.stop();
      },
      onError: (err) => errors.push(err),
    });
    const op = await ops.enqueue("booking.create", payload);
    await handle.done;

    expect(errors).toEqual([]);
    expect(results.map((r) => [r.id, r.state])).toEqual([[op.id, "succeeded"]]);
    expect(scripted.performCalls).toHaveLength(1);
    expect((await ops.get(op.id))?.state).toBe("succeeded");
  });

  it("stops after the attempt in progress and leaves the rest for the next worker", async () => {
    const scripted = new ScriptedProvider(["ok", "ok", "ok"]);
    const results: Operation[] = [];
    let handle!: ReturnType<typeof runWorker>;
    for (let i = 0; i < 3; i++) await ops.enqueue("booking.create", { ...payload, table: i });
    handle = runWorker(ops, slow(scripted, 50), {
      owner: "worker-1",
      onResult: (op) => {
        results.push(op);
        void handle.stop(); // asked to stop while the first attempt was in progress
      },
    });
    await handle.done;

    expect(results).toHaveLength(1);
    expect(results[0]?.state).toBe("succeeded");
    const left = await pool.query(`select state from provider_operations where state = 'pending'`);
    expect(left.rowCount).toBe(2);
    // Nothing is leased by the stopped worker.
    const leased = await pool.query(`select 1 from provider_operations where lease_owner = 'worker-1'`);
    expect(leased.rowCount).toBe(0);
  });

  it("processes the queue with no callbacks at all", async () => {
    const op = await ops.enqueue("booking.create", payload);
    const handle = runWorker(ops, new ScriptedProvider(["ok"]), { owner: "worker-1", idleMs: 50 });
    const deadline = Date.now() + 5_000;
    while ((await ops.get(op.id))?.state !== "succeeded" && Date.now() < deadline) await sleep(20);
    await handle.stop();
    expect((await ops.get(op.id))?.state).toBe("succeeded");
  });

  it("wakes from an idle sleep as soon as stop() is called", async () => {
    const handle = runWorker(ops, new ScriptedProvider([]), { owner: "worker-1", idleMs: 30_000 });
    await sleep(50); // let it find the queue empty and go to sleep
    const started = Date.now();
    await handle.stop();
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  it("reports a lost lease through onError instead of crashing the loop", async () => {
    const scripted = new ScriptedProvider(["ok"]);
    const errors: Array<{ err: unknown; op: Operation | null }> = [];
    let handle!: ReturnType<typeof runWorker>;
    const op = await ops.enqueue("booking.create", payload);
    handle = runWorker(ops, slow(scripted, 500), {
      owner: "worker-1",
      renewEveryMs: 100,
      onError: (err, o) => {
        errors.push({ err, op: o });
        void handle.stop();
      },
    });
    // Once worker-1 holds the lease, another process steals the row mid-attempt
    // (what a reaper or an operator might do).
    while ((await ops.get(op.id))?.leaseOwner !== "worker-1") await sleep(10);
    await pool.query(`update provider_operations set lease_owner = 'worker-2' where id = $1`, [op.id]);
    await handle.done;

    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors.every((e) => e.err instanceof StaleLeaseError)).toBe(true);
    expect(errors.some((e) => e.op?.id === op.id)).toBe(true);
    // worker-1 wrote nothing after losing the lease.
    const row = await ops.get(op.id);
    expect(row?.leaseOwner).toBe("worker-2");
    expect(row?.state).toBe("in_flight");
  });
});
