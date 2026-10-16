import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ManualClock, OperationStore, ScriptedProvider, StaleLeaseError, type Pool } from "../src/index.js";
import { setupDb, truncateAll } from "./helpers/db.js";

let pool: Pool;
let clock: ManualClock;
let ops: OperationStore;

const payload = { table: 12, party: 4, at: "2026-03-01T19:00:00Z" };

beforeAll(async () => {
  pool = await setupDb();
});
afterAll(async () => {
  await pool.end();
});
beforeEach(async () => {
  await truncateAll(pool);
  clock = new ManualClock();
  ops = new OperationStore(pool, {
    clock,
    leaseMs: 10_000,
    maxAttempts: 3,
    backoff: { baseMs: 1_000, maxMs: 8_000, random: () => 1 },
  });
});

describe("durable provider operations", () => {
  it("records a confirmed write as succeeded, with the provider's reference", async () => {
    const provider = new ScriptedProvider(["ok"]);
    const op = await ops.enqueue("booking.create", payload);
    const done = await ops.runOne("worker-1", provider);
    expect(done?.id).toBe(op.id);
    expect(done?.state).toBe("succeeded");
    expect(done?.result).toEqual({ providerRef: `ref_${op.requestId.slice(0, 8)}` });
    expect(provider.performCalls).toHaveLength(1);
    expect(provider.lookupCalls).toHaveLength(0);
  });

  it("records a definitive refusal as refused: nothing changed and nothing is retried", async () => {
    const provider = new ScriptedProvider(["refuse"]);
    await ops.enqueue("booking.create", payload);
    const done = await ops.runOne("worker-1", provider);
    expect(done?.state).toBe("refused");
    expect(done?.result).toEqual({ refusal: { code: "slot_unavailable", message: "that time is not available" } });
    expect(await ops.runOne("worker-1", provider)).toBeNull();
    expect(provider.appliedWrites()).toEqual([]);
  });

  it("marks a lost answer as unknown and backs off instead of failing", async () => {
    const provider = new ScriptedProvider(["lost_before_apply"]);
    await ops.enqueue("booking.create", payload);
    const after = await ops.runOne("worker-1", provider);
    expect(after?.state).toBe("unknown");
    expect(after?.attempts).toBe(1);
    expect(after?.lastError).toContain("connection reset");
    expect(after!.nextAttemptAt.getTime()).toBe(clock.now().getTime() + 1_000);
    // Not due yet: nothing to claim.
    expect(await ops.runOne("worker-1", provider)).toBeNull();
  });

  it("recovers an unknown outcome by asking the provider before writing again: the write applied, so no second write", async () => {
    const provider = new ScriptedProvider(["lost_after_apply", "ok"]);
    const op = await ops.enqueue("booking.create", payload);

    expect((await ops.runOne("worker-1", provider))?.state).toBe("unknown");
    clock.advance(1_000);
    const recovered = await ops.runOne("worker-2", provider);

    expect(recovered?.state).toBe("succeeded");
    expect(provider.lookupCalls).toEqual([op.requestId]);
    expect(provider.performCalls).toHaveLength(1); // the second attempt never called perform
    expect(provider.appliedWrites()).toEqual([op.requestId]);
  });

  it("recovers an unknown outcome by resending with the same request id when the write did not apply", async () => {
    const provider = new ScriptedProvider(["lost_before_apply", "ok"]);
    const op = await ops.enqueue("booking.create", payload);

    expect((await ops.runOne("worker-1", provider))?.state).toBe("unknown");
    clock.advance(1_000);
    const recovered = await ops.runOne("worker-1", provider);

    expect(recovered?.state).toBe("succeeded");
    expect(provider.lookupCalls).toEqual([op.requestId]);
    expect(provider.performCalls.map((c) => c.requestId)).toEqual([op.requestId, op.requestId]);
    expect(provider.appliedWrites()).toEqual([op.requestId]);
  });

  it("does not treat a refusal during recovery as 'nothing changed' when the earlier write applied", async () => {
    // Attempt 1 applies but the answer is lost. On attempt 2 the provider would
    // refuse a fresh request (say the slot is now taken, by our own earlier write).
    // The lookup finds the earlier write, so the operation is succeeded, not refused.
    const provider = new ScriptedProvider(["lost_after_apply", "refuse"]);
    const op = await ops.enqueue("booking.create", payload);
    await ops.runOne("worker-1", provider);
    clock.advance(1_000);
    const recovered = await ops.runOne("worker-1", provider);
    expect(recovered?.state).toBe("succeeded");
    expect(recovered?.result).toEqual({ providerRef: `ref_${op.requestId.slice(0, 8)}` });
  });

  it("gives up into 'stuck' after max attempts without an answer, never guessing", async () => {
    const provider = new ScriptedProvider(["lost_before_apply", "lost_before_apply", "lost_before_apply"]);
    const op = await ops.enqueue("booking.create", payload);
    for (let i = 0; i < 3; i++) {
      await ops.runOne("worker-1", provider);
      clock.advance(8_000);
    }
    const final = await ops.get(op.id);
    expect(final?.state).toBe("stuck");
    expect(final?.attempts).toBe(3);
    expect((await ops.stuck()).map((o) => o.id)).toEqual([op.id]);
    expect(await ops.runOne("worker-1", provider)).toBeNull();
  });

  it("hands each operation to exactly one of several concurrent workers", async () => {
    const provider = new ScriptedProvider(["ok", "ok", "ok"]);
    await Promise.all([ops.enqueue("a", payload), ops.enqueue("b", payload), ops.enqueue("c", payload)]);
    const claims = await Promise.all(
      Array.from({ length: 8 }, (_, i) => ops.claim(`worker-${i}`)),
    );
    const got = claims.filter((c) => c !== null);
    expect(got).toHaveLength(3);
    expect(new Set(got.map((c) => c!.id)).size).toBe(3);
  });

  it("lets another worker reclaim an expired lease, looks up before writing, and fences out the stale worker", async () => {
    const provider = new ScriptedProvider(["ok", "ok"]);
    const op = await ops.enqueue("booking.create", payload);

    const stale = await ops.claim("worker-1");
    expect(stale?.recovering).toBe(false);
    // worker-1 stalls past its lease without recording anything.
    clock.advance(10_001);

    const fresh = await ops.claim("worker-2");
    expect(fresh?.id).toBe(op.id);
    expect(fresh?.recovering).toBe(true); // a crashed attempt may have reached the provider

    const done = await ops.execute(fresh!, "worker-2", provider);
    expect(done.state).toBe("succeeded");
    expect(provider.lookupCalls).toEqual([op.requestId]);

    // worker-1 wakes up and tries to record its own outcome: rejected.
    await expect(ops.execute(stale!, "worker-1", provider)).rejects.toBeInstanceOf(StaleLeaseError);
    await expect(ops.renew(op.id, "worker-1")).rejects.toBeInstanceOf(StaleLeaseError);
    expect((await ops.get(op.id))?.state).toBe("succeeded");
  });

  it("renews a live lease for its owner only", async () => {
    const op = await ops.enqueue("booking.create", payload);
    await ops.claim("worker-1");
    clock.advance(9_000);
    await ops.renew(op.id, "worker-1");
    clock.advance(9_000); // past the original lease, within the renewed one
    expect(await ops.claim("worker-2")).toBeNull();
    await expect(ops.renew(op.id, "worker-2")).rejects.toBeInstanceOf(StaleLeaseError);
  });
});
