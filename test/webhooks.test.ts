import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ManualClock, SIGNATURE_HEADER, WebhookStore, sign, verify, type Pool, type Transport } from "../src/index.js";
import { setupDb, truncateAll } from "./helpers/db.js";

let pool: Pool;
let clock: ManualClock;
let hooks: WebhookStore;

/** A transport that records every POST and answers from a script of statuses. */
class ScriptedTransport implements Transport {
  readonly posts: Array<{ url: string; body: string; headers: Record<string, string> }> = [];
  constructor(private statuses: Array<number | Error>) {}
  async post(url: string, body: string, headers: Record<string, string>) {
    this.posts.push({ url, body, headers });
    const next = this.statuses.shift() ?? 200;
    if (next instanceof Error) throw next;
    return { status: next };
  }
}

beforeAll(async () => {
  pool = await setupDb();
});
afterAll(async () => {
  await pool.end();
});
beforeEach(async () => {
  await truncateAll(pool);
  clock = new ManualClock();
  hooks = new WebhookStore(pool, {
    clock,
    maxAttempts: 3,
    disableAfterFailures: 4,
    leaseMs: 10_000,
    backoff: { baseMs: 1_000, maxMs: 8_000, random: () => 1 },
  });
});

describe("signatures", () => {
  it("round-trips and rejects a tampered body, a wrong secret and a stale timestamp", () => {
    const body = JSON.stringify({ id: "evt_1", data: { amount: 1250 } });
    const header = sign("whsec_test", body, 1_700_000_000);
    expect(verify("whsec_test", body, header, { nowSeconds: 1_700_000_100 })).toBe(true);
    expect(verify("whsec_test", body.replace("1250", "9999"), header, { nowSeconds: 1_700_000_100 })).toBe(false);
    expect(verify("whsec_other", body, header, { nowSeconds: 1_700_000_100 })).toBe(false);
    expect(verify("whsec_test", body, header, { nowSeconds: 1_700_000_000 + 301 })).toBe(false);
    expect(verify("whsec_test", body, "garbage", { nowSeconds: 1_700_000_100 })).toBe(false);
  });
});

describe("webhook delivery", () => {
  it("delivers a signed event once to each enabled endpoint", async () => {
    const a = await hooks.createEndpoint("https://a.example/hooks", "secret_a");
    await hooks.createEndpoint("https://b.example/hooks", "secret_b");
    const transport = new ScriptedTransport([200, 200]);

    const { event } = await hooks.publish("booking.created", { bookingId: "b_1" });
    const delivered = await hooks.deliverDue(transport);

    expect(delivered.map((d) => d.state)).toEqual(["delivered", "delivered"]);
    expect(transport.posts.map((p) => p.url).sort()).toEqual(["https://a.example/hooks", "https://b.example/hooks"]);
    const toA = transport.posts.find((p) => p.url.startsWith("https://a"))!;
    expect(verify(a.secret, toA.body, toA.headers[SIGNATURE_HEADER]!, { nowSeconds: clock.now().getTime() / 1000 })).toBe(true);
    expect(JSON.parse(toA.body)).toMatchObject({ id: event.id, type: "booking.created", data: { bookingId: "b_1" } });
    expect(await hooks.deliverDue(transport)).toEqual([]); // nothing left
  });

  it("does not create a second event or second deliveries when a publish is retried with the same event id", async () => {
    await hooks.createEndpoint("https://a.example/hooks", "s");
    const first = await hooks.publish("booking.created", { bookingId: "b_1" }, "11111111-1111-4111-8111-111111111111");
    const again = await hooks.publish("booking.created", { bookingId: "b_1" }, "11111111-1111-4111-8111-111111111111");
    expect(first.created).toBe(true);
    expect(again.created).toBe(false);
    expect(await hooks.deliveriesFor(first.event.id)).toHaveLength(1);
  });

  it("retries a failed delivery with exponential backoff and then marks it failed", async () => {
    await hooks.createEndpoint("https://a.example/hooks", "s");
    const transport = new ScriptedTransport([500, new Error("ECONNRESET"), 503]);
    const { event } = await hooks.publish("booking.created", {});

    const [first] = await hooks.deliverDue(transport);
    expect(first).toMatchObject({ state: "pending", attempts: 1, lastStatus: 500 });
    expect(first!.nextAttemptAt.getTime()).toBe(clock.now().getTime() + 1_000);
    expect(await hooks.deliverDue(transport)).toEqual([]); // backing off

    clock.advance(1_000);
    const [second] = await hooks.deliverDue(transport);
    expect(second).toMatchObject({ state: "pending", attempts: 2, lastStatus: null, lastError: "ECONNRESET" });
    expect(second!.nextAttemptAt.getTime()).toBe(clock.now().getTime() + 2_000);

    clock.advance(2_000);
    const [third] = await hooks.deliverDue(transport);
    expect(third).toMatchObject({ state: "failed", attempts: 3, lastStatus: 503 });
    expect(transport.posts.map((p) => p.headers["webhook-attempt"])).toEqual(["1", "2", "3"]);
    expect((await hooks.deliveriesFor(event.id))[0]?.state).toBe("failed");
  });

  it("disables an endpoint after consecutive failures and resets the count on success", async () => {
    // No retries here, so each publish + deliverDue is exactly one attempt.
    const noRetry = new WebhookStore(pool, { clock, maxAttempts: 1, disableAfterFailures: 4 });
    const ep = await noRetry.createEndpoint("https://dead.example/hooks", "s");
    const transport = new ScriptedTransport([500, 500, 200, 500, 500, 500, 500]);

    for (let i = 0; i < 3; i++) {
      await noRetry.publish("ping", { i });
      await noRetry.deliverDue(transport);
    }
    expect((await noRetry.getEndpoint(ep.id))?.consecutiveFailures).toBe(0); // the 200 reset it
    expect((await noRetry.getEndpoint(ep.id))?.enabled).toBe(true);

    for (let i = 0; i < 4; i++) {
      await noRetry.publish("ping", { i });
      await noRetry.deliverDue(transport);
    }
    const after = await noRetry.getEndpoint(ep.id);
    expect(after?.enabled).toBe(false);
    expect(after?.consecutiveFailures).toBe(4);

    // A disabled endpoint receives no new deliveries until it is re-enabled.
    const { event } = await noRetry.publish("ping", { late: true });
    expect(await noRetry.deliveriesFor(event.id)).toEqual([]);
    await noRetry.enableEndpoint(ep.id);
    const { event: next } = await noRetry.publish("ping", { afterFix: true });
    expect(await noRetry.deliveriesFor(next.id)).toHaveLength(1);
  });

  it("replays delivered and failed events for an endpoint over a window", async () => {
    const ep = await hooks.createEndpoint("https://a.example/hooks", "s");
    const transport = new ScriptedTransport([200, 500, 500, 500, 200, 200]);

    const { event: e1 } = await hooks.publish("one", {});
    await hooks.deliverDue(transport); // delivered
    clock.advance(60_000);
    const { event: e2 } = await hooks.publish("two", {});
    for (let i = 0; i < 3; i++) {
      await hooks.deliverDue(transport); // 500 x3 -> failed
      clock.advance(8_000);
    }
    expect((await hooks.deliveriesFor(e2.id))[0]?.state).toBe("failed");

    const replayed = await hooks.replay(ep.id, new Date(e1.createdAt.getTime() - 1));
    expect(replayed).toBe(2);
    const redelivered = await hooks.deliverDue(transport);
    expect(redelivered.map((d) => d.state)).toEqual(["delivered", "delivered"]);
    expect(transport.posts.slice(-2).map((p) => JSON.parse(p.body).id).sort()).toEqual([e1.id, e2.id].sort());
  });

  it("claims each due delivery for exactly one of several concurrent workers", async () => {
    await hooks.createEndpoint("https://a.example/hooks", "s");
    for (let i = 0; i < 5; i++) await hooks.publish("n", { i });
    const transport = new ScriptedTransport([]);
    const batches = await Promise.all([hooks.deliverDue(transport, 10), hooks.deliverDue(transport, 10), hooks.deliverDue(transport, 10)]);
    const all = batches.flat();
    expect(all).toHaveLength(5);
    expect(new Set(all.map((d) => d.id)).size).toBe(5);
    expect(transport.posts).toHaveLength(5);
  });
});
