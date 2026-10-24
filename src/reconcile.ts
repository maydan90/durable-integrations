import type { Clock } from "./clock.js";
import type { Pool } from "./db.js";
import type { OperationState } from "./operations.js";
import type { AppliedWrite, Provider, ProviderLedger } from "./provider.js";

/**
 * Reconciliation: compare one window of the provider's records with our
 * operations, fix what is provably safe to fix, and report the rest.
 *
 * Lookup-before-retry (operations.ts) covers the operation a worker is about
 * to retry. Reconciliation covers everything else: operations that went stuck
 * and will never be retried, and writes we have no record of at all. It is
 * the job you run every night over yesterday.
 *
 * What it changes, and the only thing it changes:
 *   unknown or stuck, and the provider applied it  ->  succeeded, with the
 *   provider's result. The provider's own ledger is the proof.
 *
 * What it reports and leaves alone (a person decides):
 *   orphan_at_provider   the provider applied a write we have no row for
 *   applied_but_refused  we recorded a refusal, the provider has the write
 *   missing_at_provider  we recorded success, the provider has no such write
 *                        (confirmed by lookup, so a window edge cannot cause it)
 *
 * What it never does: mark anything failed, resend anything, or touch a row a
 * worker owns. Pending and in_flight rows belong to the workers, which look up
 * before they write. The heal is a compare-and-set on the state column, so a
 * worker that claims the row first wins and the heal is skipped; whichever
 * commits second sees the row has moved on.
 */

export type Finding =
  | { kind: "orphan_at_provider"; requestId: string; appliedAt: Date }
  | { kind: "applied_but_refused"; operationId: string; requestId: string; appliedAt: Date }
  | { kind: "missing_at_provider"; operationId: string; requestId: string };

export interface HealedOperation {
  operationId: string;
  requestId: string;
  from: "unknown" | "stuck";
}

export interface ReconcileReport {
  since: Date;
  until: Date;
  /** Writes the provider listed for the window. */
  providerWrites: number;
  /** Already succeeded on our side; nothing to do. */
  matched: number;
  healed: HealedOperation[];
  /** Pending or in_flight: a worker owns these and will look up before writing. */
  leftToWorkers: number;
  findings: Finding[];
}

export interface ReconcileOptions {
  since: Date;
  until: Date;
  clock: Clock;
}

/** What reconciliation needs from a provider: its ledger, and lookup to confirm a gap. */
export type ReconcileSource<R> = ProviderLedger<R> & Pick<Provider<unknown, R>, "lookup">;

interface LocalRow {
  id: string;
  request_id: string;
  state: OperationState;
}

export async function reconcile<R>(pool: Pool, source: ReconcileSource<R>, opts: ReconcileOptions): Promise<ReconcileReport> {
  const { since, until, clock } = opts;
  if (until <= since) throw new RangeError("reconcile: `until` must be after `since`");

  const applied = await source.listApplied(since, until);
  const report: ReconcileReport = {
    since,
    until,
    providerWrites: applied.length,
    matched: 0,
    healed: [],
    leftToWorkers: 0,
    findings: [],
  };

  const local = await pool.query<LocalRow>(
    `select id, request_id, state from provider_operations where request_id = any($1::text[])`,
    [applied.map((w) => w.requestId)],
  );
  const byRequestId = new Map(local.rows.map((r) => [r.request_id, r]));

  for (const write of applied) {
    const row = byRequestId.get(write.requestId);
    if (!row) {
      report.findings.push({ kind: "orphan_at_provider", requestId: write.requestId, appliedAt: write.appliedAt });
      continue;
    }
    switch (row.state) {
      case "succeeded":
        report.matched++;
        break;
      case "refused":
        report.findings.push({
          kind: "applied_but_refused",
          operationId: row.id,
          requestId: row.request_id,
          appliedAt: write.appliedAt,
        });
        break;
      case "pending":
      case "in_flight":
        report.leftToWorkers++;
        break;
      case "unknown":
      case "stuck":
        await heal(pool, clock, row, write, report);
        break;
    }
  }

  // The other direction: successes we recorded in the window that the
  // provider did not list. The provider applies a write before we can record
  // its success, so an operation whose success was recorded in the window but
  // whose write applied just before `since` would look missing. Lookup settles
  // it, and costs one call per gap rather than one per operation.
  const listed = new Set(applied.map((w) => w.requestId));
  const succeeded = await pool.query<{ id: string; request_id: string }>(
    `select id, request_id from provider_operations
      where state = 'succeeded' and updated_at >= $1 and updated_at < $2
      order by updated_at`,
    [since, until],
  );
  for (const row of succeeded.rows) {
    if (listed.has(row.request_id)) continue;
    if ((await source.lookup(row.request_id)) === null) {
      report.findings.push({ kind: "missing_at_provider", operationId: row.id, requestId: row.request_id });
    }
  }

  return report;
}

async function heal<R>(pool: Pool, clock: Clock, row: LocalRow, write: AppliedWrite<R>, report: ReconcileReport) {
  // Compare-and-set: only from the state we read. If a worker claimed the row
  // in the meantime it is in_flight now, this updates nothing, and the worker
  // finishes the job through its own lookup.
  const res = await pool.query(
    `update provider_operations
        set state = 'succeeded', result = $3, last_error = null, updated_at = $4
      where id = $1 and state = $2`,
    [row.id, row.state, JSON.stringify(write.result), clock.now()],
  );
  if (res.rowCount === 1) {
    report.healed.push({ operationId: row.id, requestId: row.request_id, from: row.state as "unknown" | "stuck" });
    return;
  }
  const now = await pool.query<{ state: OperationState }>(`select state from provider_operations where id = $1`, [row.id]);
  if (now.rows[0]?.state === "succeeded") report.matched++;
  else report.leftToWorkers++;
}
