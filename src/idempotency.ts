import { createHash } from "node:crypto";
import type { Clock } from "./clock.js";
import type { Pool } from "./db.js";

/**
 * Idempotency keys, Stripe-style.
 *
 * A client sends `Idempotency-Key: <key>` with a request. The first request
 * with a key runs the handler and stores its response. A retry with the same
 * key and the same payload gets the stored response back and the handler does
 * not run again. The same key with a different payload is a client bug and is
 * refused rather than replayed. A key whose first request is still running is
 * refused with "in progress" so two concurrent retries cannot both execute.
 *
 * Keys are scoped (per API client, per tenant) so one caller cannot replay or
 * collide with another caller's keys.
 */

export class IdempotencyKeyReusedError extends Error {
  readonly code = "idempotency_key_reused";
  constructor(scope: string, key: string) {
    super(`Idempotency key "${key}" in scope "${scope}" was already used with a different request payload`);
  }
}

export class IdempotencyInProgressError extends Error {
  readonly code = "idempotency_in_progress";
  constructor(scope: string, key: string) {
    super(`A request with idempotency key "${key}" in scope "${scope}" is still in progress`);
  }
}

export interface IdempotentResult<T> {
  response: T;
  /** true when the response came from the ledger and the handler did not run */
  replayed: boolean;
}

/** What an HTTP handler produces: the status code matters as much as the body. */
export interface HttpResponse<B> {
  status: number;
  body: B;
}

export interface IdempotentHttpResult<B> extends HttpResponse<B> {
  /** true when the status and body came from the ledger and the handler did not run */
  replayed: boolean;
}

export interface IdempotencyOptions {
  clock: Clock;
  /** how long a completed key keeps its stored response; default 24h */
  ttlMs?: number;
}

/** Stable digest of the request so retries with a changed payload are caught. */
export function requestDigest(request: unknown): string {
  return createHash("sha256").update(canonicalJson(request)).digest("hex");
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalJson(obj[k])}`).join(",")}}`;
}

export class IdempotencyStore {
  private readonly ttlMs: number;
  constructor(
    private readonly pool: Pool,
    private readonly opts: IdempotencyOptions,
  ) {
    this.ttlMs = opts.ttlMs ?? 24 * 60 * 60 * 1000;
  }

  /**
   * Runs `handler` at most once per (scope, key, payload).
   *
   * The claim is a single INSERT ... ON CONFLICT DO NOTHING, so under
   * concurrency exactly one caller wins the key; the others see the row and
   * either replay it or are told it is in progress.
   */
  async run<T>(scope: string, key: string, request: unknown, handler: () => Promise<T>): Promise<IdempotentResult<T>> {
    const r = await this.execute(scope, key, request, async () => ({ value: await handler(), status: null }));
    return { response: r.value, replayed: r.replayed };
  }

  /**
   * Like `run`, for an HTTP endpoint: the status code is stored with the body,
   * so a retry of a request that returned 201 Created gets 201 again, not a
   * 200 with the same body. A response the handler *returns* (a 422 for a
   * validation error, say) is a real answer and is replayed with its status;
   * only a handler that *throws* frees the key.
   *
   * A key first stored through `run` has no status and replays as 200.
   */
  async runHttp<B>(
    scope: string,
    key: string,
    request: unknown,
    handler: () => Promise<HttpResponse<B>>,
  ): Promise<IdempotentHttpResult<B>> {
    const r = await this.execute(scope, key, request, async () => {
      const res = await handler();
      return { value: res.body, status: res.status };
    });
    return { status: r.status ?? 200, body: r.value, replayed: r.replayed };
  }

  private async execute<T>(
    scope: string,
    key: string,
    request: unknown,
    handler: () => Promise<{ value: T; status: number | null }>,
  ): Promise<{ value: T; status: number | null; replayed: boolean }> {
    const hash = requestDigest(request);
    const now = this.opts.clock.now();
    const expiresAt = new Date(now.getTime() + this.ttlMs);

    // Expired rows are reaped lazily: a new claim may replace one whose TTL passed.
    await this.pool.query(`delete from idempotency_keys where scope = $1 and key = $2 and expires_at <= $3`, [
      scope,
      key,
      now,
    ]);

    const claimed = await this.pool.query(
      `insert into idempotency_keys (scope, key, request_hash, status, created_at, expires_at)
       values ($1, $2, $3, 'in_progress', $4, $5)
       on conflict (scope, key) do nothing
       returning key`,
      [scope, key, hash, now, expiresAt],
    );

    if (claimed.rowCount === 0) {
      const existing = await this.pool.query<{
        request_hash: string;
        status: string;
        response: T;
        response_status: number | null;
      }>(
        `select request_hash, status, response, response_status from idempotency_keys where scope = $1 and key = $2`,
        [scope, key],
      );
      const row = existing.rows[0];
      if (!row) {
        // The row vanished between our insert and select (a failed handler freed it). Try again.
        return this.execute(scope, key, request, handler);
      }
      if (row.request_hash !== hash) throw new IdempotencyKeyReusedError(scope, key);
      if (row.status !== "completed") throw new IdempotencyInProgressError(scope, key);
      return { value: row.response, status: row.response_status, replayed: true };
    }

    let result: { value: T; status: number | null };
    try {
      result = await handler();
    } catch (err) {
      // A handler that threw did not produce a response worth replaying: free the
      // key so the client can retry. (If the handler itself wrote to a third
      // party, that write belongs in a provider operation, see operations.ts.)
      await this.pool.query(`delete from idempotency_keys where scope = $1 and key = $2`, [scope, key]);
      throw err;
    }

    await this.pool.query(
      `update idempotency_keys set status = 'completed', response = $3, response_status = $4
       where scope = $1 and key = $2`,
      [scope, key, JSON.stringify(result.value), result.status],
    );
    return { value: result.value, status: result.status, replayed: false };
  }
}
