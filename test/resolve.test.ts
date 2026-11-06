import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ManualClock, NotStuckError, OperationStore, ScriptedProvider, reconcile, type Pool } from "../src/index.js";
import { setupDb, truncateAll } from "./helpers/db.js";

let pool: Pool;
let clock: ManualClock;
let ops: OperationStore;

const payload = { table: 12, party: 4, at: "2026-03-01T19:00:00Z" };
const DAY = 24 * 3600 * 1000;
const start = new Date("2026-01-01T00:00:00Z");

beforeAll(async () => {
  pool = await setupDb();
});
afterAll(async () => {
  await pool.end();
});
beforeEach(async () => {
  await truncateAll(pool);
  clock = new ManualClock(start);
  ops = new OperationStore(pool, {
    clock,
    leaseMs: 10_000,
    maxAttempts: 3,
    backoff: { baseMs: 1_000, maxMs: 8_000, random: () => 1 },
  });
});

/** One attempt, answer lost, attempt budget of one: the row goes straight to stuck. */
async function stuckOperation(provider: ScriptedProvider) {
  const op = await ops.enqueue("booking.create", payload, { maxAttempts: 1 });
  expect((await ops.runOne("worker-1", provider))?.state).toBe("stuck");
  return op;
}

describe("resolve: a person settles a stuck operation", () => {
  it("records 'applied' as succeeded with the provider's result, who decided and why", async () => {
    const provider = new ScriptedProvider(["lost_after_apply"], clock);
    const op = await stuckOperation(provider);
    clock.advance(60_000);

    const resolved = await ops.resolve(op.id, {
      outcome: "applied",
      result: { providerRef: "ref_from_dashboard" },
      by: "yuri",
      note: "Booking is in the provider dashboard, created 00:00:01",
    });

    expect(resolved.state).toBe("succeeded");
    expect(resolved.result).toEqual({ providerRef: "ref_from_dashboard" });
    expect(resolved.resolution).toEqual({
      by: "yuri",
      at: clock.now(),
      note: "Booking is in the provider dashboard, created 00:00:01",
    });
    expect(await ops.stuck()).toEqual([]);
    expect((await ops.get(op.id))?.resolution?.by).toBe("yuri");
  });

  it("records 'not_applied' as refused (nothing changed) and never resends", async () => {
    const provider = new ScriptedProvider(["lost_before_apply"], clock);
    const op = await stuckOperation(provider);

    const resolved = await ops.resolve(op.id, {
      outcome: "not_applied",
      by: "yuri",
      note: "No booking under this request id; support confirmed",
    });

    expect(resolved.state).toBe("refused");
    expect(resolved.result).toEqual({
      refusal: { code: "not_applied", message: "No booking under this request id; support confirmed" },
    });
    // Finished: no worker picks it up again, and the provider saw one attempt.
    clock.advance(DAY);
    expect(await ops.runOne("worker-1", provider)).toBeNull();
    expect(provider.performCalls).toHaveLength(1);
  });

  it("refuses rows that are not stuck, and changes nothing", async () => {
    const provider = new ScriptedProvider(["lost_before_apply", "ok"], clock);
    const unknown = await ops.enqueue("booking.create", payload);
    expect((await ops.runOne("worker-1", provider))?.state).toBe("unknown"); // a worker will retry this one
    const done = await ops.enqueue("booking.create", payload);
    clock.advance(500); // the unknown row is still backing off, so the worker takes the new one
    expect((await ops.runOne("worker-1", provider))?.state).toBe("succeeded");

    const input = { outcome: "not_applied" as const, by: "yuri", note: "checked" };
    for (const [id, state] of [
      [unknown.id, "unknown"],
      [done.id, "succeeded"],
    ] as const) {
      const err = await ops.resolve(id, input).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NotStuckError);
      expect((err as NotStuckError).state).toBe(state);
      expect((await ops.get(id))?.state).toBe(state);
      expect((await ops.get(id))?.resolution).toBeNull();
    }

    const missing = await ops.resolve("00000000-0000-0000-0000-000000000000", input).catch((e: unknown) => e);
    expect(missing).toBeInstanceOf(NotStuckError);
    expect((missing as NotStuckError).state).toBeNull();
  });

  it("requires who decided and a note saying what they checked", async () => {
    const op = await stuckOperation(new ScriptedProvider(["lost_before_apply"], clock));

    await expect(ops.resolve(op.id, { outcome: "not_applied", by: " ", note: "checked" })).rejects.toThrow(RangeError);
    await expect(ops.resolve(op.id, { outcome: "not_applied", by: "yuri", note: "" })).rejects.toThrow(RangeError);
    expect((await ops.get(op.id))?.state).toBe("stuck");
  });

  it("loses to a reconciliation that healed the row first: the person sees what happened", async () => {
    const provider = new ScriptedProvider(["lost_after_apply"], clock);
    const op = await stuckOperation(provider);

    const report = await reconcile(pool, provider, { since: start, until: new Date(start.getTime() + DAY), clock });
    expect(report.healed.map((h) => h.operationId)).toEqual([op.id]);

    const err = await ops
      .resolve(op.id, { outcome: "not_applied", by: "yuri", note: "could not find it" })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NotStuckError);
    expect((err as NotStuckError).state).toBe("succeeded");
    expect((await ops.get(op.id))?.resolution).toBeNull();
  });

  it("a wrong 'not_applied' is caught by the next reconciliation, not hidden", async () => {
    // The write did apply; the person looked in the wrong place.
    const provider = new ScriptedProvider(["lost_after_apply"], clock);
    const op = await stuckOperation(provider);
    await ops.resolve(op.id, { outcome: "not_applied", by: "yuri", note: "not in the dashboard" });

    const report = await reconcile(pool, provider, { since: start, until: new Date(start.getTime() + DAY), clock });

    expect(report.healed).toEqual([]);
    expect(report.findings).toEqual([
      { kind: "applied_but_refused", operationId: op.id, requestId: op.requestId, appliedAt: start },
    ]);
  });
});
