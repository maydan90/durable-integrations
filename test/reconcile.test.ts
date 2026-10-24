import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ManualClock, OperationStore, ScriptedProvider, reconcile, type Pool } from "../src/index.js";
import { setupDb, truncateAll } from "./helpers/db.js";

let pool: Pool;
let clock: ManualClock;
let ops: OperationStore;

const payload = { table: 12, party: 4, at: "2026-03-01T19:00:00Z" };
const DAY = 24 * 3600 * 1000;
const start = new Date("2026-01-01T00:00:00Z");
const day1 = { since: start, until: new Date(start.getTime() + DAY) };

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

const run = (provider: ScriptedProvider, window = day1) => reconcile(pool, provider, { ...window, clock });

describe("reconciliation", () => {
  it("heals an unknown operation the provider applied: succeeded with the provider's result, and no second write", async () => {
    const provider = new ScriptedProvider(["lost_after_apply"], clock);
    const op = await ops.enqueue("booking.create", payload);
    expect((await ops.runOne("worker-1", provider))?.state).toBe("unknown");

    const report = await run(provider);

    expect(report.healed).toEqual([{ operationId: op.id, requestId: op.requestId, from: "unknown" }]);
    expect(report.findings).toEqual([]);
    const healed = await ops.get(op.id);
    expect(healed?.state).toBe("succeeded");
    expect(healed?.result).toEqual({ providerRef: `ref_${op.requestId.slice(0, 8)}` });

    // Nothing left for a worker to retry, and the provider saw one write.
    clock.advance(8_000);
    expect(await ops.runOne("worker-1", provider)).toBeNull();
    expect(provider.performCalls).toHaveLength(1);
  });

  it("heals a stuck operation, which no worker will ever retry", async () => {
    const provider = new ScriptedProvider(["lost_after_apply"], clock);
    const op = await ops.enqueue("booking.create", payload, { maxAttempts: 1 });
    await ops.runOne("worker-1", provider);
    expect((await ops.stuck()).map((o) => o.id)).toEqual([op.id]);

    const report = await run(provider);

    expect(report.healed).toEqual([{ operationId: op.id, requestId: op.requestId, from: "stuck" }]);
    expect(await ops.stuck()).toEqual([]);
    expect((await ops.get(op.id))?.state).toBe("succeeded");
  });

  it("is safe to run twice: the second run over the same window only matches", async () => {
    const provider = new ScriptedProvider(["lost_after_apply", "ok"], clock);
    await ops.enqueue("booking.create", payload);
    await ops.enqueue("booking.create", payload);
    await ops.runOne("worker-1", provider);
    await ops.runOne("worker-1", provider);

    const first = await run(provider);
    const second = await run(provider);

    expect(first.healed).toHaveLength(1);
    expect(first.matched).toBe(1);
    expect(second.healed).toEqual([]);
    expect(second.matched).toBe(2);
    expect(second.findings).toEqual([]);
  });

  it("leaves an operation a worker holds to that worker, whose fenced write still lands", async () => {
    const provider = new ScriptedProvider(["lost_after_apply"], clock);
    const op = await ops.enqueue("booking.create", payload);
    await ops.runOne("worker-1", provider);
    clock.advance(1_000);
    const claimed = await ops.claim("worker-2");
    expect(claimed?.state).toBe("in_flight");

    const report = await run(provider);

    expect(report.healed).toEqual([]);
    expect(report.leftToWorkers).toBe(1);
    expect((await ops.get(op.id))?.state).toBe("in_flight");
    // The worker finishes through its own lookup; reconciliation did not fence it out.
    const done = await ops.execute(claimed!, "worker-2", provider);
    expect(done.state).toBe("succeeded");
    expect(provider.performCalls).toHaveLength(1);
  });

  it("never lets a heal and a concurrent claim both take the same row", async () => {
    for (let i = 0; i < 10; i++) {
      await truncateAll(pool);
      const provider = new ScriptedProvider(["lost_after_apply"], clock);
      const op = await ops.enqueue("booking.create", payload);
      await ops.runOne("worker-1", provider);
      clock.advance(8_000); // due again, so a worker may claim it

      const [report, claimed] = await Promise.all([run(provider, { since: start, until: new Date(clock.now().getTime() + DAY) }), ops.claim("worker-2")]);

      const healed = report.healed.length === 1;
      expect(healed).toBe(claimed === null);
      expect((await ops.get(op.id))?.state).toBe(healed ? "succeeded" : "in_flight");
    }
  });

  it("reports a write we recorded as refused but the provider applied, and changes nothing", async () => {
    const provider = new ScriptedProvider(["refuse"], clock);
    const op = await ops.enqueue("booking.create", payload);
    expect((await ops.runOne("worker-1", provider))?.state).toBe("refused");
    provider.applyElsewhere(op.requestId); // say the provider applied it and still answered with an error

    const report = await run(provider);

    expect(report.findings).toEqual([
      { kind: "applied_but_refused", operationId: op.id, requestId: op.requestId, appliedAt: start },
    ]);
    expect(report.healed).toEqual([]);
    expect((await ops.get(op.id))?.state).toBe("refused");
  });

  it("reports a provider write we have no operation for", async () => {
    const provider = new ScriptedProvider([], clock);
    provider.applyElsewhere("written-by-someone-else");

    const report = await run(provider);

    expect(report.findings).toEqual([{ kind: "orphan_at_provider", requestId: "written-by-someone-else", appliedAt: start }]);
  });

  it("reports a success the provider has no record of, after confirming it with a lookup", async () => {
    const ours = new ScriptedProvider(["ok"], clock);
    const op = await ops.enqueue("booking.create", payload);
    expect((await ops.runOne("worker-1", ours))?.state).toBe("succeeded");
    // The provider's side no longer has the write (voided, rolled back on their end).
    const theirs = new ScriptedProvider([], clock);

    const report = await run(theirs);

    expect(report.findings).toEqual([{ kind: "missing_at_provider", operationId: op.id, requestId: op.requestId }]);
    expect(theirs.lookupCalls).toEqual([op.requestId]);
    expect((await ops.get(op.id))?.state).toBe("succeeded");
  });

  it("does not report a success whose write applied before the window, even though the provider did not list it", async () => {
    const provider = new ScriptedProvider(["lost_after_apply"], clock);
    const op = await ops.enqueue("booking.create", payload);
    await ops.runOne("worker-1", provider); // applied at 00:00, answer lost
    clock.advance(3600 * 1000);
    expect((await ops.runOne("worker-1", provider))?.state).toBe("succeeded"); // success recorded at 01:00

    const report = await run(provider, { since: new Date(start.getTime() + 1800 * 1000), until: day1.until });

    expect(report.providerWrites).toBe(0);
    expect(report.findings).toEqual([]);
    expect(provider.lookupCalls).toEqual([op.requestId, op.requestId]); // the worker's recovery, then the confirmation
  });
});
