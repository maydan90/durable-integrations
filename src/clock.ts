/**
 * Time is an input. Every store takes a Clock so tests can move time forward
 * (lease expiry, backoff, replay windows) without sleeping.
 */
export interface Clock {
  now(): Date;
}

export const systemClock: Clock = { now: () => new Date() };

export class ManualClock implements Clock {
  private current: Date;
  constructor(start: Date = new Date("2026-01-01T00:00:00Z")) {
    this.current = start;
  }
  now(): Date {
    return new Date(this.current);
  }
  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
  set(date: Date): void {
    this.current = new Date(date);
  }
}

/** Exponential backoff with full jitter, capped. `random` is injectable for tests. */
export function backoffMs(attempt: number, opts: { baseMs: number; maxMs: number; random?: () => number }): number {
  const exp = Math.min(opts.maxMs, opts.baseMs * 2 ** Math.max(0, attempt - 1));
  const r = opts.random ?? Math.random;
  return Math.floor(exp / 2 + r() * (exp / 2));
}
