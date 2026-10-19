import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  IdempotencyInProgressError,
  IdempotencyKeyReusedError,
  IdempotencyStore,
  ManualClock,
  withTransaction,
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

describe("HTTP responses keep their status code", () => {
  it("replays a 201 Created as 201 with the same body, without running the handler again", async () => {
    let calls = 0;
    const create = () =>
      store.runHttp("client_a", "key-1", { party: 2 }, async () => ({
        status: 201,
        body: { bookingId: `b_${++calls}` },
      }));

    expect(await create()).toEqual({ status: 201, body: { bookingId: "b_1" }, replayed: false });
    expect(await create()).toEqual({ status: 201, body: { bookingId: "b_1" }, replayed: true });
    expect(calls).toBe(1);
  });

  it("replays a returned 422 as 422: a validation error the handler answered with is a real response", async () => {
    let calls = 0;
    const handler = async () => {
      calls++;
      return { status: 422, body: { error: "party_too_large", max: 12 } };
    };

    await store.runHttp("client_a", "key-1", { party: 40 }, handler);
    const retry = await store.runHttp("client_a", "key-1", { party: 40 }, handler);

    expect(retry).toEqual({ status: 422, body: { error: "party_too_large", max: 12 }, replayed: true });
    expect(calls).toBe(1);
  });

  it("stores the status next to the response, and none for keys stored through run()", async () => {
    await store.runHttp("client_a", "key-1", {}, async () => ({ status: 202, body: { queued: true } }));
    await store.run("client_a", "key-2", {}, async () => ({ queued: true }));

    const rows = await pool.query<{ key: string; response_status: number | null }>(
      `select key, response_status from idempotency_keys where scope = 'client_a' order by key`,
    );
    expect(rows.rows).toEqual([
      { key: "key-1", response_status: 202 },
      { key: "key-2", response_status: null },
    ]);

    // A key first stored without a status replays as 200 through runHttp.
    const replay = await store.runHttp("client_a", "key-2", {}, async () => ({ status: 500, body: null }));
    expect(replay).toEqual({ status: 200, body: { queued: true }, replayed: true });
  });

  it("still frees the key when the handler throws, so a retry can produce a status at all", async () => {
    await expect(
      store.runHttp("client_a", "key-1", {}, async () => {
        throw new Error("downstream down");
      }),
    ).rejects.toThrow("downstream down");
    const retry = await store.runHttp("client_a", "key-1", {}, async () => ({ status: 201, body: "created" }));
    expect(retry).toEqual({ status: 201, body: "created", replayed: false });
  });
});

describe("a handler that runs inside withTransaction", () => {
  // A local table the handler writes to, standing in for the rows a real
  // request would create (a booking, an order line) before the response exists.
  beforeEach(async () => {
    await pool.query(`create table if not exists test_bookings (id text primary key, idempotency_key text not null)`);
    await pool.query(`truncate test_bookings`);
  });

  const bookingRows = async () =>
    (await pool.query<{ n: number }>(`select count(*)::int as n from test_bookings`)).rows[0]?.n;
  const keyRows = async () =>
    (
      await pool.query<{ n: number }>(
        `select count(*)::int as n from idempotency_keys where scope = 'client_a' and key = 'key-1'`,
      )
    ).rows[0]?.n;

  it("rolls the handler's writes back and frees the key when the handler throws, so the retry starts clean", async () => {
    let calls = 0;
    const book = (failAfterWrite: boolean) =>
      store.run("client_a", "key-1", { party: 2 }, () =>
        withTransaction(pool, async (tx) => {
          calls++;
          await tx.query(`insert into test_bookings (id, idempotency_key) values ($1, $2)`, ["b_1", "key-1"]);
          if (failAfterWrite) throw new Error("provider down");
          return { bookingId: "b_1" };
        }),
      );

    // The handler wrote a row and then failed: the row is gone with the
    // transaction, and the key is free, so the client is not stuck behind an
    // "in progress" claim for a request that produced nothing.
    await expect(book(true)).rejects.toThrow("provider down");
    expect(await bookingRows()).toBe(0);
    expect(await keyRows()).toBe(0);

    // The retry runs the handler again and commits exactly once.
    expect(await book(false)).toEqual({ response: { bookingId: "b_1" }, replayed: false });
    expect(await bookingRows()).toBe(1);
    expect(await keyRows()).toBe(1);

    // A further retry replays the stored response. Had the handler run again,
    // the primary key on test_bookings would have made it throw.
    expect(await book(false)).toEqual({ response: { bookingId: "b_1" }, replayed: true });
    expect(await bookingRows()).toBe(1);
    expect(calls).toBe(2);
  });

  it("keeps the handler's writes when it returns, and the stored response with them", async () => {
    const result = await store.run("client_a", "key-1", { party: 4 }, () =>
      withTransaction(pool, async (tx) => {
        await tx.query(`insert into test_bookings (id, idempotency_key) values ($1, $2)`, ["b_2", "key-1"]);
        return { bookingId: "b_2" };
      }),
    );
    expect(result).toEqual({ response: { bookingId: "b_2" }, replayed: false });
    expect(await bookingRows()).toBe(1);

    const stored = await pool.query<{ status: string; response: { bookingId: string } }>(
      `select status, response from idempotency_keys where scope = 'client_a' and key = 'key-1'`,
    );
    expect(stored.rows[0]).toEqual({ status: "completed", response: { bookingId: "b_2" } });
  });
});
