import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import { backoffMs, type Clock } from "./clock.js";
import type { Pool } from "./db.js";

/**
 * Webhook delivery that cannot double-notify and can be replayed.
 *
 *   publish   appends an event to the ledger. Callers that retry pass their own
 *             deterministic event id, so a retried publish is a no-op instead of
 *             a second event.
 *   fan-out   one delivery row per (endpoint, event), unique, so a retried
 *             fan-out cannot create a second delivery either.
 *   deliver   claims due deliveries (SKIP LOCKED), signs the body with the
 *             endpoint's secret, POSTs, and on failure backs off exponentially.
 *             After max attempts the delivery is failed; after N consecutive
 *             failures the endpoint is disabled so a dead URL stops burning
 *             retries.
 *   replay    re-queues deliveries for an endpoint over a time window, which is
 *             how a partner recovers after fixing their receiver.
 *
 * The signature scheme is `t=<unix seconds>,v1=<hex hmac-sha256(secret, t + "." + body)>`.
 * The timestamp is inside the signed message, so an attacker cannot replay an
 * old signed body with a fresh timestamp, and receivers reject stale ones.
 */

export interface Endpoint {
  id: string;
  url: string;
  secret: string;
  enabled: boolean;
  consecutiveFailures: number;
}

export interface WebhookEvent<T = unknown> {
  id: string;
  type: string;
  payload: T;
  createdAt: Date;
}

export type DeliveryState = "pending" | "in_flight" | "delivered" | "failed";

export interface Delivery {
  id: string;
  endpointId: string;
  eventId: string;
  state: DeliveryState;
  attempts: number;
  maxAttempts: number;
  nextAttemptAt: Date;
  lastStatus: number | null;
  lastError: string | null;
}

/** The HTTP side, injectable so tests can script responses. */
export interface Transport {
  post(url: string, body: string, headers: Record<string, string>): Promise<{ status: number }>;
}

export interface WebhookOptions {
  clock: Clock;
  maxAttempts?: number;
  /** consecutive failed deliveries before an endpoint is disabled; default 10 */
  disableAfterFailures?: number;
  leaseMs?: number;
  backoff?: { baseMs: number; maxMs: number; random?: () => number };
}

export const SIGNATURE_HEADER = "webhook-signature";

export function sign(secret: string, body: string, timestampSeconds: number): string {
  const mac = createHmac("sha256", secret).update(`${timestampSeconds}.${body}`).digest("hex");
  return `t=${timestampSeconds},v1=${mac}`;
}

/**
 * Receiver-side verification. Constant-time comparison; rejects signatures
 * whose timestamp is outside `toleranceSeconds` of `nowSeconds`.
 */
export function verify(
  secret: string,
  body: string,
  header: string,
  opts: { nowSeconds: number; toleranceSeconds?: number },
): boolean {
  const parts = Object.fromEntries(header.split(",").map((kv) => kv.split("=", 2) as [string, string]));
  const t = Number(parts["t"]);
  const v1 = parts["v1"];
  if (!Number.isFinite(t) || !v1) return false;
  if (Math.abs(opts.nowSeconds - t) > (opts.toleranceSeconds ?? 300)) return false;
  const expected = createHmac("sha256", secret).update(`${t}.${body}`).digest("hex");
  const a = Buffer.from(expected, "hex");
  const b = Buffer.from(v1, "hex");
  return a.length === b.length && timingSafeEqual(a, b);
}

export class WebhookStore {
  private readonly maxAttempts: number;
  private readonly disableAfterFailures: number;
  private readonly leaseMs: number;
  private readonly backoff: { baseMs: number; maxMs: number; random?: () => number };

  constructor(
    private readonly pool: Pool,
    private readonly opts: WebhookOptions,
  ) {
    this.maxAttempts = opts.maxAttempts ?? 8;
    this.disableAfterFailures = opts.disableAfterFailures ?? 10;
    this.leaseMs = opts.leaseMs ?? 30_000;
    this.backoff = opts.backoff ?? { baseMs: 1_000, maxMs: 15 * 60_000 };
  }

  async createEndpoint(url: string, secret: string): Promise<Endpoint> {
    const id = randomUUID();
    await this.pool.query(
      `insert into webhook_endpoints (id, url, secret, enabled, consecutive_failures, created_at) values ($1, $2, $3, true, 0, $4)`,
      [id, url, secret, this.opts.clock.now()],
    );
    return { id, url, secret, enabled: true, consecutiveFailures: 0 };
  }

  async getEndpoint(id: string): Promise<Endpoint | null> {
    const res = await this.pool.query<{
      id: string;
      url: string;
      secret: string;
      enabled: boolean;
      consecutive_failures: number;
    }>(`select id, url, secret, enabled, consecutive_failures from webhook_endpoints where id = $1`, [id]);
    const r = res.rows[0];
    return r ? { id: r.id, url: r.url, secret: r.secret, enabled: r.enabled, consecutiveFailures: r.consecutive_failures } : null;
  }

  async enableEndpoint(id: string): Promise<void> {
    await this.pool.query(`update webhook_endpoints set enabled = true, consecutive_failures = 0 where id = $1`, [id]);
  }

  /**
   * Appends an event and fans it out to every enabled endpoint. Pass a
   * deterministic `eventId` (e.g. derived from the domain change) when the
   * caller might retry; the second publish then returns the existing event
   * and creates no new deliveries.
   */
  async publish<T>(type: string, payload: T, eventId: string = randomUUID()): Promise<{ event: WebhookEvent<T>; created: boolean }> {
    const now = this.opts.clock.now();
    const inserted = await this.pool.query(
      `insert into webhook_events (id, type, payload, created_at) values ($1, $2, $3, $4)
       on conflict (id) do nothing returning id`,
      [eventId, type, JSON.stringify(payload), now],
    );
    const created = inserted.rowCount === 1;
    // Fan-out is idempotent too: the (endpoint, event) unique key absorbs retries.
    await this.pool.query(
      `insert into webhook_deliveries (id, endpoint_id, event_id, state, attempts, max_attempts, next_attempt_at, created_at, updated_at)
       select gen_random_uuid(), e.id, $1, 'pending', 0, $2, $3, $3, $3
         from webhook_endpoints e where e.enabled
       on conflict (endpoint_id, event_id) do nothing`,
      [eventId, this.maxAttempts, now],
    );
    const ev = await this.pool.query<{ id: string; type: string; payload: T; created_at: Date }>(
      `select id, type, payload, created_at from webhook_events where id = $1`,
      [eventId],
    );
    const r = ev.rows[0]!;
    return { event: { id: r.id, type: r.type, payload: r.payload, createdAt: r.created_at }, created };
  }

  async deliveriesFor(eventId: string): Promise<Delivery[]> {
    const res = await this.pool.query<DeliveryRow>(
      `select ${DELIVERY_COLUMNS} from webhook_deliveries where event_id = $1 order by created_at`,
      [eventId],
    );
    return res.rows.map(toDelivery);
  }

  /**
   * Delivers up to `limit` due deliveries. Each is claimed with a lease under
   * SKIP LOCKED so concurrent workers never pick the same row, then POSTed
   * with a signed body. Returns the deliveries it touched.
   */
  async deliverDue(transport: Transport, limit = 10): Promise<Delivery[]> {
    const now = this.opts.clock.now();
    const claimed = await this.pool.query<DeliveryRow & { url: string; secret: string; type: string; payload: unknown; enabled: boolean }>(
      `with candidate as (
         select d.id from webhook_deliveries d
          where d.state in ('pending', 'in_flight')
            and d.next_attempt_at <= $1
            and (d.lease_until is null or d.lease_until < $1)
          order by d.next_attempt_at
          for update skip locked
          limit $3
       )
       update webhook_deliveries d
          set state = 'in_flight', lease_until = $2, updated_at = $1
         from candidate, webhook_endpoints e, webhook_events ev
        where d.id = candidate.id and e.id = d.endpoint_id and ev.id = d.event_id
       returning ${DELIVERY_COLUMNS.split(", ").map((c) => `d.${c}`).join(", ")},
                 e.url, e.secret, e.enabled, ev.type, ev.payload`,
      [now, new Date(now.getTime() + this.leaseMs), limit],
    );

    const out: Delivery[] = [];
    for (const row of claimed.rows) {
      const attempt = row.attempts + 1;
      if (!row.enabled) {
        out.push(await this.finish(row.id, "failed", attempt, null, "endpoint disabled"));
        continue;
      }
      const body = JSON.stringify({ id: row.event_id, type: row.type, created_at: row.created_at, data: row.payload });
      const headers = {
        "content-type": "application/json",
        [SIGNATURE_HEADER]: sign(row.secret, body, Math.floor(this.opts.clock.now().getTime() / 1000)),
        "webhook-id": row.event_id,
        "webhook-attempt": String(attempt),
      };
      let status: number | null = null;
      let error: string | null = null;
      try {
        status = (await transport.post(row.url, body, headers)).status;
      } catch (err) {
        error = err instanceof Error ? err.message : String(err);
      }

      if (status !== null && status >= 200 && status < 300) {
        await this.pool.query(`update webhook_endpoints set consecutive_failures = 0 where id = $1`, [row.endpoint_id]);
        out.push(await this.finish(row.id, "delivered", attempt, status, null));
        continue;
      }

      const failures = await this.pool.query<{ consecutive_failures: number }>(
        `update webhook_endpoints set consecutive_failures = consecutive_failures + 1,
                enabled = case when consecutive_failures + 1 >= $2 then false else enabled end
          where id = $1 returning consecutive_failures`,
        [row.endpoint_id, this.disableAfterFailures],
      );
      const endpointDisabled = (failures.rows[0]?.consecutive_failures ?? 0) >= this.disableAfterFailures;

      if (attempt >= row.max_attempts || endpointDisabled) {
        out.push(await this.finish(row.id, "failed", attempt, status, error ?? `http ${status}`));
      } else {
        const delay = backoffMs(attempt, this.backoff);
        const res = await this.pool.query<DeliveryRow>(
          `update webhook_deliveries
              set state = 'pending', attempts = $2, last_status = $3, last_error = $4,
                  next_attempt_at = $5, lease_until = null, updated_at = $6
            where id = $1 returning ${DELIVERY_COLUMNS}`,
          [row.id, attempt, status, error ?? `http ${status}`, new Date(this.opts.clock.now().getTime() + delay), this.opts.clock.now()],
        );
        out.push(toDelivery(res.rows[0]!));
      }
    }
    return out;
  }

  /**
   * Re-queues every delivery to `endpointId` for events created in
   * [since, until). Delivered and failed rows alike go back to pending with a
   * fresh attempt budget; the receiver sees the same event ids, so an
   * idempotent receiver is unaffected by duplicates.
   */
  async replay(endpointId: string, since: Date, until: Date = this.opts.clock.now()): Promise<number> {
    const now = this.opts.clock.now();
    const res = await this.pool.query(
      `update webhook_deliveries d
          set state = 'pending', attempts = 0, max_attempts = $4, next_attempt_at = $5, lease_until = null,
              last_status = null, last_error = null, updated_at = $5
         from webhook_events ev
        where d.event_id = ev.id and d.endpoint_id = $1
          and ev.created_at >= $2 and ev.created_at < $3
          and d.state in ('delivered', 'failed')`,
      [endpointId, since, until, this.maxAttempts, now],
    );
    return res.rowCount ?? 0;
  }

  private async finish(id: string, state: DeliveryState, attempts: number, status: number | null, error: string | null) {
    const res = await this.pool.query<DeliveryRow>(
      `update webhook_deliveries
          set state = $2, attempts = $3, last_status = $4, last_error = $5, lease_until = null, updated_at = $6
        where id = $1 returning ${DELIVERY_COLUMNS}`,
      [id, state, attempts, status, error, this.opts.clock.now()],
    );
    return toDelivery(res.rows[0]!);
  }
}

interface DeliveryRow {
  id: string;
  endpoint_id: string;
  event_id: string;
  state: DeliveryState;
  attempts: number;
  max_attempts: number;
  next_attempt_at: Date;
  last_status: number | null;
  last_error: string | null;
  created_at: Date;
}

const DELIVERY_COLUMNS = "id, endpoint_id, event_id, state, attempts, max_attempts, next_attempt_at, last_status, last_error, created_at";

function toDelivery(r: DeliveryRow): Delivery {
  return {
    id: r.id,
    endpointId: r.endpoint_id,
    eventId: r.event_id,
    state: r.state,
    attempts: r.attempts,
    maxAttempts: r.max_attempts,
    nextAttemptAt: r.next_attempt_at,
    lastStatus: r.last_status,
    lastError: r.last_error,
  };
}
