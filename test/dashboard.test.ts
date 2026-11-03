import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ManualClock, OperationStore, ScriptedProvider, type Pool } from "../src/index.js";
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

describe("operations dashboard: listDue and stats", () => {
  it("reports zeros for every state on an empty table", async () => {
    expect(await ops.stats()).toEqual({
      byState: { pending: 0, in_flight: 0, succeeded: 0, refused: 0, unknown: 0, stuck: 0 },
      due: 0,
      oldestDueAt: null,
    });
    expect(await ops.listDue()).toEqual([]);
  });

  it("counts one row in each state, and only claimable rows as due", async () => {
    const provider = new ScriptedProvider(["ok", "refuse", "lost_before_apply", "lost_before_apply"]);

    await ops.enqueue("booking.create", payload);
    expect((await ops.runOne("worker-1", provider))?.state).toBe("succeeded");
    await ops.enqueue("booking.create", payload);
    expect((await ops.runOne("worker-1", provider))?.state).toBe("refused");
    const backingOff = await ops.enqueue("booking.create", payload);
    expect((await ops.runOne("worker-1", provider))?.state).toBe("unknown"); // due again in 1s
    await ops.enqueue("booking.create", payload, { maxAttempts: 1 });
    expect((await ops.runOne("worker-1", provider))?.state).toBe("stuck");
    await ops.enqueue("booking.create", payload);
    expect((await ops.claim("worker-1"))?.state).toBe("in_flight"); // live lease for 10s
    const waiting = await ops.enqueue("booking.create", payload);

    const now = await ops.stats();
    expect(now.byState).toEqual({ pending: 1, in_flight: 1, succeeded: 1, refused: 1, unknown: 1, stuck: 1 });
    // Only the fresh pending row is claimable: the unknown one is backing off,
    // the in_flight one has a live lease, and the rest are finished.
    expect(now.due).toBe(1);
    expect(now.oldestDueAt).toEqual(waiting.nextAttemptAt);
    expect((await ops.listDue()).map((o) => o.id)).toEqual([waiting.id]);

    clock.advance(1_000); // the backoff ends; the lease is still live
    const later = await ops.stats();
    expect(later.due).toBe(2);
    expect(later.oldestDueAt).toEqual(waiting.nextAttemptAt);
    const due = await ops.listDue();
    expect(due.map((o) => o.id)).toEqual([waiting.id, backingOff.id]);
    expect(due.map((o) => o.recovering)).toEqual([false, true]); // the unknown row will look up first
  });

  it("lists due rows oldest first, honours the limit, and shows a crashed worker's row once its lease expires", async () => {
    const first = await ops.enqueue("booking.create", payload);
    clock.advance(100);
    const second = await ops.enqueue("booking.create", payload);
    clock.advance(100);
    const third = await ops.enqueue("booking.create", payload);

    expect((await ops.claim("worker-1"))?.id).toBe(first.id); // then worker-1 dies

    expect((await ops.listDue()).map((o) => o.id)).toEqual([second.id, third.id]);
    expect((await ops.listDue(1)).map((o) => o.id)).toEqual([second.id]);

    clock.advance(10_001); // the lease expires without renewal
    const due = await ops.listDue();
    expect(due.map((o) => o.id)).toEqual([first.id, second.id, third.id]);
    expect(due[0]?.recovering).toBe(true); // the dead attempt may have reached the provider
    expect((await ops.stats()).due).toBe(3);
    expect((await ops.stats()).oldestDueAt).toEqual(first.nextAttemptAt);
  });

  it("is a plain read: listing takes no lease, and the next claim gets the first listed row", async () => {
    await ops.enqueue("booking.create", payload);
    clock.advance(100);
    await ops.enqueue("booking.create", payload);

    const listed = await ops.listDue();
    expect(listed.every((o) => o.state === "pending" && o.leaseOwner === null)).toBe(true);
    expect(await ops.listDue()).toEqual(listed); // looking twice changes nothing

    const claimed = await ops.claim("worker-1");
    expect(claimed?.id).toBe(listed[0]!.id);
    expect((await ops.listDue()).map((o) => o.id)).toEqual([listed[1]!.id]);
  });
});
