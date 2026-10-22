import { randomUUID } from "node:crypto";
import { backoffMs, type Clock } from "./clock.js";
import type { Pool } from "./db.js";
import { LostAnswerError, type Provider } from "./provider.js";

/**
 * Durable provider operations: every write to a third party is a row first.
 *
 * Lifecycle
 *   enqueue  -> pending
 *   claim    -> in_flight (a worker takes a lease; SKIP LOCKED so workers never
 *               block each other and a crashed worker's lease simply expires)
 *   execute  -> succeeded | refused | unknown
 *   unknown  -> the next attempt first asks the provider whether the previous
 *               write applied (lookup by request_id). If it did, we record
 *               success without writing again. If it did not, we resend with
 *               the same request_id.
 *   stuck    -> attempts exhausted while still unknown. Nothing is guessed; a
 *               human (or a reconciliation job) resolves it from the provider's
 *               records.
 *
 * Two rules that keep this honest:
 *   1. A refusal is only "nothing changed" when no earlier attempt could have
 *      applied. A refusal met while recovering an unknown attempt is resolved
 *      by lookup, never by assuming.
 *   2. Completion is fenced by the lease owner. A worker that lost its lease
 *      (expired, reclaimed) cannot overwrite the state the new owner wrote.
 */

export type OperationState = "pending" | "in_flight" | "succeeded" | "refused" | "unknown" | "stuck";

export interface Operation<P = unknown, R = unknown> {
  id: string;
  kind: string;
  requestId: string;
  payload: P;
  state: OperationState;
  attempts: number;
  maxAttempts: number;
  leaseOwner: string | null;
  leaseUntil: Date | null;
  nextAttemptAt: Date;
  result: R | null;
  lastError: string | null;
  /**
   * true when a previous attempt may have reached the provider: an earlier
   * attempt ended unknown, or a worker crashed mid-attempt and its lease
   * expired. The next attempt must look before it writes.
   */
  recovering: boolean;
}

export interface OperationsOptions {
  clock: Clock;
  leaseMs?: number;
  maxAttempts?: number;
  backoff?: { baseMs: number; maxMs: number; random?: () => number };
}

export class StaleLeaseError extends Error {
  readonly code = "stale_lease";
  constructor(id: string, owner: string) {
    super(`Operation ${id} is no longer leased by ${owner}`);
  }
}

interface Row {
  id: string;
  kind: string;
  request_id: string;
  payload: unknown;
  state: OperationState;
  attempts: number;
  max_attempts: number;
  lease_owner: string | null;
  lease_until: Date | null;
  next_attempt_at: Date;
  result: unknown;
  last_error: string | null;
  prev_state?: OperationState;
}

function toOperation<P, R>(r: Row): Operation<P, R> {
  return {
    id: r.id,
    kind: r.kind,
    requestId: r.request_id,
    payload: r.payload as P,
    state: r.state,
    attempts: r.attempts,
    maxAttempts: r.max_attempts,
    leaseOwner: r.lease_owner,
    leaseUntil: r.lease_until,
    nextAttemptAt: r.next_attempt_at,
    result: r.result as R | null,
    lastError: r.last_error,
    recovering: r.prev_state ? r.prev_state !== "pending" : r.attempts > 0,
  };
}

const COLUMNS =
  "id, kind, request_id, payload, state, attempts, max_attempts, lease_owner, lease_until, next_attempt_at, result, last_error";

export class OperationStore {
  /** Exposed so a worker loop can pick a renewal interval (see worker.ts). */
  readonly leaseMs: number;
  private readonly maxAttempts: number;
  private readonly backoff: { baseMs: number; maxMs: number; random?: () => number };

  constructor(
    private readonly pool: Pool,
    private readonly opts: OperationsOptions,
  ) {
    this.leaseMs = opts.leaseMs ?? 30_000;
    this.maxAttempts = opts.maxAttempts ?? 5;
    this.backoff = opts.backoff ?? { baseMs: 1_000, maxMs: 60_000 };
  }

  /** Records the intent to write. The request_id is minted here and never changes. */
  async enqueue<P>(kind: string, payload: P, opts: { maxAttempts?: number } = {}): Promise<Operation<P>> {
    const now = this.opts.clock.now();
    const res = await this.pool.query<Row>(
      `insert into provider_operations
         (id, kind, request_id, payload, state, attempts, max_attempts, next_attempt_at, created_at, updated_at)
       values ($1, $2, $3, $4, 'pending', 0, $5, $6, $6, $6)
       returning ${COLUMNS}`,
      [randomUUID(), kind, randomUUID(), JSON.stringify(payload), opts.maxAttempts ?? this.maxAttempts, now],
    );
    return toOperation<P, never>(res.rows[0]!);
  }

  async get<P = unknown, R = unknown>(id: string): Promise<Operation<P, R> | null> {
    const res = await this.pool.query<Row>(`select ${COLUMNS} from provider_operations where id = $1`, [id]);
    return res.rows[0] ? toOperation<P, R>(res.rows[0]) : null;
  }

  /**
   * Takes a lease on one due operation. `FOR UPDATE SKIP LOCKED` lets any
   * number of workers poll the same table without contention, and an expired
   * lease (`lease_until < now`) is claimable even if the row still says
   * in_flight, which is how a crashed worker's work gets picked up.
   */
  async claim<P = unknown, R = unknown>(owner: string): Promise<Operation<P, R> | null> {
    const now = this.opts.clock.now();
    const leaseUntil = new Date(now.getTime() + this.leaseMs);
    const res = await this.pool.query<Row>(
      `with candidate as (
         select id, state as prev_state from provider_operations
          where state in ('pending', 'unknown', 'in_flight')
            and next_attempt_at <= $1
            and (lease_until is null or lease_until < $1)
          order by next_attempt_at
          for update skip locked
          limit 1
       )
       update provider_operations o
          set state = 'in_flight', lease_owner = $2, lease_until = $3, updated_at = $1
         from candidate
        where o.id = candidate.id
       returning ${COLUMNS.split(", ").map((c) => `o.${c}`).join(", ")}, candidate.prev_state`,
      [now, owner, leaseUntil],
    );
    return res.rows[0] ? toOperation<P, R>(res.rows[0]) : null;
  }

  /** Extends the lease; only the current owner may do so. */
  async renew(id: string, owner: string): Promise<void> {
    const now = this.opts.clock.now();
    const res = await this.pool.query(
      `update provider_operations set lease_until = $3, updated_at = $4
        where id = $1 and lease_owner = $2 and state = 'in_flight' and lease_until >= $4`,
      [id, owner, new Date(now.getTime() + this.leaseMs), now],
    );
    if (res.rowCount === 0) throw new StaleLeaseError(id, owner);
  }

  /**
   * Performs one attempt of a claimed operation against the provider and
   * records the outcome. Returns the operation's new state.
   */
  async execute<P, R>(op: Operation<P, R>, owner: string, provider: Provider<P, R>): Promise<Operation<P, R>> {
    const attempt = op.attempts + 1;

    // Recovery: if an earlier attempt may have reached the provider, ask before writing.
    if (op.recovering) {
      let found: R | null;
      try {
        found = await provider.lookup(op.requestId);
      } catch (err) {
        return this.recordUnknown(op, owner, attempt, `lookup failed: ${errorMessage(err)}`);
      }
      if (found !== null) return this.recordSuccess(op, owner, attempt, found);
    }

    try {
      const outcome = await provider.perform(op.kind, op.payload, op.requestId);
      if (outcome.ok) return this.recordSuccess(op, owner, attempt, outcome.result);
      // A refusal on a fresh attempt means nothing changed. On a recovery
      // attempt the lookup above already proved the earlier write did not
      // apply (the provider dedupes by request_id), so it is safe here too.
      return this.recordRefusal(op, owner, attempt, outcome.refusal);
    } catch (err) {
      return this.recordUnknown(op, owner, attempt, errorMessage(err));
    }
  }

  private async recordSuccess<P, R>(op: Operation<P, R>, owner: string, attempt: number, result: R) {
    return this.fencedUpdate<P, R>(
      op.id,
      owner,
      `state = 'succeeded', attempts = $3, result = $4, last_error = null, lease_owner = null, lease_until = null`,
      [attempt, JSON.stringify(result)],
    );
  }

  private async recordRefusal<P, R>(
    op: Operation<P, R>,
    owner: string,
    attempt: number,
    refusal: { code: string; message: string },
  ) {
    return this.fencedUpdate<P, R>(
      op.id,
      owner,
      `state = 'refused', attempts = $3, result = $4, last_error = null, lease_owner = null, lease_until = null`,
      [attempt, JSON.stringify({ refusal })],
    );
  }

  private async recordUnknown<P, R>(op: Operation<P, R>, owner: string, attempt: number, error: string) {
    const now = this.opts.clock.now();
    if (attempt >= op.maxAttempts) {
      return this.fencedUpdate<P, R>(
        op.id,
        owner,
        `state = 'stuck', attempts = $3, last_error = $4, lease_owner = null, lease_until = null`,
        [attempt, error],
      );
    }
    const delay = backoffMs(attempt, this.backoff);
    return this.fencedUpdate<P, R>(
      op.id,
      owner,
      `state = 'unknown', attempts = $3, last_error = $4, next_attempt_at = $5, lease_owner = null, lease_until = null`,
      [attempt, error, new Date(now.getTime() + delay)],
    );
  }

  /** Every state write is conditioned on still owning the lease: the fence. */
  private async fencedUpdate<P, R>(id: string, owner: string, setClause: string, params: unknown[]) {
    const now = this.opts.clock.now();
    const res = await this.pool.query<Row>(
      `update provider_operations
          set ${setClause}, updated_at = $${params.length + 3}
        where id = $1 and lease_owner = $2 and state = 'in_flight' and lease_until >= $${params.length + 3}
       returning ${COLUMNS}`,
      [id, owner, ...params, now],
    );
    if (!res.rows[0]) throw new StaleLeaseError(id, owner);
    return toOperation<P, R>(res.rows[0]);
  }

  /** Operations needing a human: unknown outcome with attempts exhausted. */
  async stuck(): Promise<Operation[]> {
    const res = await this.pool.query<Row>(`select ${COLUMNS} from provider_operations where state = 'stuck' order by updated_at`);
    return res.rows.map((r) => toOperation(r));
  }

  /**
   * Convenience worker loop step: claim one due operation and execute it.
   * Returns null when nothing is due.
   */
  async runOne<P, R>(owner: string, provider: Provider<P, R>): Promise<Operation<P, R> | null> {
    const op = await this.claim<P, R>(owner);
    if (!op) return null;
    return this.execute(op, owner, provider);
  }
}

function errorMessage(err: unknown): string {
  if (err instanceof LostAnswerError) return err.message;
  if (err instanceof Error) return `${err.name}: ${err.message}`;
  return String(err);
}
