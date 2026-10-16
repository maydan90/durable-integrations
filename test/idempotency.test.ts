import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  IdempotencyInProgressError,
  IdempotencyKeyReusedError,
  IdempotencyStore,
  ManualClock,
  type Pool,
} from "../src/index.js";
import { setupDb, truncateAll } from "./helpers/db.js";

let pool: Pool;
let clock: ManualClock;
let store: IdempotencyStore;

beforeAll(async () => {
  pool = await setupDb();
});
afterAll(async () => {
  await pool.end();
});
beforeEach(async () => {
  await truncateAll(pool);
  clock = new ManualClock();
  store = new IdempotencyStore(pool, { clock, ttlMs: 60_000 });
});

describe("idempotency keys", () => {
  it("runs the handler once and replays the stored response for the same key and payload", async () => {
    let calls = 0;
    const handler = async () => ({ bookingId: `b_${++calls}` });

    const first = await store.run("client_a", "key-1", { party: 2 }, handler);
    const second = await store.run("client_a", "key-1", { party: 2 }, handler);

    expect(first).toEqual({ response: { bookingId: "b_1" }, replayed: false });
    expect(second).toEqual({ response: { bookingId: "b_1" }, replayed: true });
    expect(calls).toBe(1);
  });

  it("refuses the same key with a different payload instead of replaying", async () => {
    await store.run("client_a", "key-1", { party: 2 }, async () => "ok");
    await expect(store.run("client_a", "key-1", { party: 4 }, async () => "ok")).rejects.toBeInstanceOf(
      IdempotencyKeyReusedError,
    );
  });

  it("treats payloads with the same content in a different key order as the same request", async () => {
    let calls = 0;
    await store.run("client_a", "key-1", { a: 1, b: { c: 2, d: 3 } }, async () => ++calls);
    const r = await store.run("client_a", "key-1", { b: { d: 3, c: 2 }, a: 1 }, async () => ++calls);
    expect(r.replayed).toBe(true);
    expect(calls).toBe(1);
  });

  it("scopes keys so another client cannot replay or collide", async () => {
    let calls = 0;
    await store.run("client_a", "key-1", { party: 2 }, async () => ++calls);
    const other = await store.run("client_b", "key-1", { party: 9 }, async () => ++calls);
    expect(other).toEqual({ response: 2, replayed: false });
  });

  it("lets exactly one of two concurrent callers run the handler; the other is told it is in progress", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    let calls = 0;
    const slow = async () => {
      calls++;
      await gate;
      return "done";
    };

    const a = store.run("client_a", "key-1", {}, slow);
    // Give the first call time to claim the key before the second arrives.
    await new Promise((r) => setTimeout(r, 50));
    await expect(store.run("client_a", "key-1", {}, slow)).rejects.toBeInstanceOf(IdempotencyInProgressError);

    release();
    expect(await a).toEqual({ response: "done", replayed: false });
    expect(calls).toBe(1);
  });

  it("frees the key when the handler throws so the client can retry", async () => {
    await expect(
      store.run("client_a", "key-1", {}, async () => {
        throw new Error("downstream down");
      }),
    ).rejects.toThrow("downstream down");
    const retry = await store.run("client_a", "key-1", {}, async () => "second time lucky");
    expect(retry).toEqual({ response: "second time lucky", replayed: false });
  });

  it("expires stored responses after the TTL", async () => {
    let calls = 0;
    await store.run("client_a", "key-1", {}, async () => ++calls);
    clock.advance(60_001);
    const r = await store.run("client_a", "key-1", {}, async () => ++calls);
    expect(r).toEqual({ response: 2, replayed: false });
  });
});
