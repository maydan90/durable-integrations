import { OperationStore, StaleLeaseError, type Operation } from "./operations.js";
import type { Provider } from "./provider.js";

/**
 * A long-running worker loop over an OperationStore.
 *
 *   claim one due operation
 *   execute it, extending the lease on a timer while the provider is slow
 *   record the outcome (the store fences this on still owning the lease)
 *   repeat; sleep briefly when nothing is due
 *
 * Why the timer matters: a lease is a promise to finish or disappear within
 * `leaseMs`. A provider that takes longer than that would otherwise expire
 * the lease mid-attempt, another worker would claim the same row, and this
 * worker's completion would be rejected with StaleLeaseError. Renewing every
 * `leaseMs / 3` keeps a live worker's lease live and still lets a dead one
 * expire within one lease period.
 *
 * Stopping is graceful: `stop()` finishes the attempt in progress and then
 * returns. Nothing is ever abandoned mid-write, because an abandoned write is
 * exactly the unknown outcome this whole repository exists to avoid.
 */

export interface WorkerOptions {
  /** Identifies this worker in lease_owner. Use something stable per process, e.g. host + pid. */
  owner: string;
  /** How often to extend the lease during an attempt. Default: a third of the store's leaseMs. */
  renewEveryMs?: number;
  /** How long to sleep when nothing is due. Default 1000 ms. stop() cuts the sleep short. */
  idleMs?: number;
  /** Every finished attempt: succeeded, refused, unknown or stuck. */
  onResult?: (op: Operation) => void;
  /** Claim or execute threw. StaleLeaseError here means another worker now owns the row. */
  onError?: (err: unknown, op: Operation | null) => void;
}

export interface WorkerHandle {
  /** Resolves when the loop has exited, whether by stop() or by an unrecoverable error. */
  readonly done: Promise<void>;
  /** Ask the loop to finish its current attempt and exit. Safe to call more than once. */
  stop(): Promise<void>;
}

export function runWorker<P, R>(ops: OperationStore, provider: Provider<P, R>, opts: WorkerOptions): WorkerHandle {
  const renewEveryMs = opts.renewEveryMs ?? Math.max(50, Math.floor(ops.leaseMs / 3));
  const idleMs = opts.idleMs ?? 1_000;
  let stopping = false;
  let wake: (() => void) | null = null;

  const idle = () =>
    new Promise<void>((resolve) => {
      const timer = setTimeout(finish, idleMs);
      wake = finish;
      function finish() {
        clearTimeout(timer);
        wake = null;
        resolve();
      }
    });

  const executeWithRenewal = async (op: Operation<P, R>): Promise<Operation<P, R>> => {
    let finished = false;
    let renewal: Promise<void> | null = null;
    const timer = setInterval(() => {
      if (renewal) return; // a renewal is already in flight; never stack them
      renewal = ops
        .renew(op.id, opts.owner)
        .catch((err) => {
          // After completion the row is no longer in_flight, so a late renew
          // is refused. That is expected, not a lost lease.
          if (!(err instanceof StaleLeaseError) || !finished) opts.onError?.(err, op);
        })
        .finally(() => {
          renewal = null;
        });
    }, renewEveryMs);
    try {
      return await ops.execute(op, opts.owner, provider);
    } finally {
      finished = true;
      clearInterval(timer);
      if (renewal) await renewal;
    }
  };

  const done = (async () => {
    while (!stopping) {
      let op: Operation<P, R> | null = null;
      try {
        op = await ops.claim<P, R>(opts.owner);
      } catch (err) {
        opts.onError?.(err, null);
      }
      if (!op) {
        if (!stopping) await idle(); // stop() may have arrived during the claim
        continue;
      }
      // A row claimed after stop() was requested is still ours to finish:
      // completing it is cheaper and safer than letting the lease expire.
      try {
        const result = await executeWithRenewal(op);
        opts.onResult?.(result);
      } catch (err) {
        opts.onError?.(err, op);
      }
    }
  })();

  return {
    done,
    stop() {
      stopping = true;
      wake?.();
      return done;
    },
  };
}
