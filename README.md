# durable-integrations

Patterns for writing to third parties whose answers can be lost, in TypeScript on PostgreSQL, tested against a real database.

This repo demonstrates four mechanisms I reach for whenever a system has to call a payment processor, a booking provider, a carrier or any API where a duplicate write costs money and a lost answer is not a failure:

1. **Idempotency keys** (`src/idempotency.ts`): a retried request gets the stored response, the same key with a different payload is refused, and two concurrent retries cannot both run.
2. **Leased operations** (`src/operations.ts`): every outbound write is a row first. Workers claim rows under `FOR UPDATE SKIP LOCKED`, hold a lease, and every state write is fenced on still owning it.
3. **Unknown-outcome recovery** (`src/operations.ts`): a timeout is recorded as *unknown*, not failed. The next attempt asks the provider whether the write applied before sending again, with the same request id. After the attempt budget it becomes *stuck* for a human; nothing is guessed.
4. **Webhook delivery with replay** (`src/webhooks.ts`): an append-only event ledger, one delivery row per (endpoint, event) so retries cannot fan out twice, HMAC signatures with the timestamp inside the signed message, exponential backoff, auto-disable of dead endpoints, and replay over a time window.

Everything is deliberately small: `pg` and plain SQL, no framework, under 900 lines of source. The tests are the specification.

```
npm install
npm test          # starts PostgreSQL 16 in Docker via Testcontainers
DATABASE_URL=postgresql://user:pass@localhost:5432/db npm test   # or use your own server
```

## The problem

A call to a third party has three outcomes, not two:

| What happened | What we know | Wrong reaction | Right reaction |
|---|---|---|---|
| 2xx | The write applied | | Record success |
| 4xx with a reason | Nothing changed | Retry | Record the refusal and tell the caller why |
| Timeout, reset, 502 from a proxy | **The write may have applied** | Treat as failure and retry blindly, or treat as success | Record *unknown*, then look before writing again |

Most integration bugs I have fixed in production were the third row being handled as the first or the second: a reservation retried every minute for a day because a refusal was read as "still confirming", and a double booking because a timeout was read as "not booked". The design below makes the third outcome a first-class state.

## Idempotency keys

```ts
const store = new IdempotencyStore(pool, { clock: systemClock, ttlMs: 24 * 3600 * 1000 });

const { response, replayed } = await store.run(clientId, idempotencyKey, requestBody, async () => {
  return createBooking(requestBody);
});
```

- The claim is one `INSERT ... ON CONFLICT DO NOTHING`. Under concurrency exactly one caller wins; the others either replay the stored response or get `IdempotencyInProgressError` (409) while the first is still running.
- The request is hashed canonically (sorted keys), so `{a:1,b:2}` and `{b:2,a:1}` are the same request, and a changed payload under the same key is `IdempotencyKeyReusedError` (422) rather than a silent replay of something else.
- Keys are scoped per caller. One client's key cannot replay or collide with another's.
- A handler that throws frees the key so the client can retry. If the handler itself wrote to a third party, that write belongs in an operation (below), not behind the key.

### The handler inside a transaction

A request usually writes local rows before it has a response to store: the booking, the order lines, the audit entry. Run the handler inside `withTransaction` so those rows and the response stand or fall together:

```ts
const { response } = await store.run(clientId, idempotencyKey, requestBody, () =>
  withTransaction(pool, async (tx) => {
    const booking = await insertBooking(tx, requestBody);
    await enqueueProviderWrite(tx, booking); // a provider_operations row, see below
    return { bookingId: booking.id };
  }),
);
```

If the handler throws, the transaction rolls back and the store frees the key, so the retry starts from nothing: no half-written booking, no "in progress" refusal for a request that produced nothing. `test/idempotency.test.ts` proves both halves.

Two things are deliberately outside the transaction. The claim is committed before the handler starts, because it has to be visible to a concurrent retry on another connection. The `completed` write happens after the commit. A crash between the two leaves the key `in_progress` until its TTL, which is the conservative failure: the client is told to wait, never handed a duplicate.

## Leased operations and unknown-outcome recovery

```ts
const ops = new OperationStore(pool, { clock: systemClock, leaseMs: 30_000, maxAttempts: 5 });

await ops.enqueue("booking.create", { table: 12, party: 4, at });   // intent, recorded first

// in a worker loop, any number of workers:
await ops.runOne(workerId, provider);
```

State machine:

```
enqueue ──▶ pending ──claim──▶ in_flight ──▶ succeeded
                                    │
                                    ├──▶ refused        (provider said no; nothing changed)
                                    │
                                    └──▶ unknown ──backoff──▶ claim again ──lookup──▶ succeeded (it had applied)
                                            │                                   └──perform──▶ ...
                                            └── attempts exhausted ──▶ stuck   (a human resolves it)
```

Rules the tests pin down:

- **One request id for the life of the operation.** Minted at enqueue, sent on every attempt. A provider that dedupes on it cannot double-apply; a provider that does not still gets exactly one send per attempt, and the lookup before each retry keeps that safe.
- **Look before you write again.** If a previous attempt may have reached the provider (an unknown outcome, or a reclaimed lease after a worker crash), the next attempt calls `provider.lookup(requestId)` first. Found: record success, no second write. Not found: resend with the same id.
- **A refusal is only "nothing changed" when nothing could have changed.** On a recovery attempt the lookup runs first, so a refusal cannot mask an earlier write that applied. The test `does not treat a refusal during recovery as 'nothing changed'` is the one I would show an interviewer.
- **Leases fence writes.** Claiming sets `lease_owner` and `lease_until`. Every state update is `WHERE lease_owner = $me AND lease_until >= now()`. A worker that stalled past its lease and wakes up later gets `StaleLeaseError` instead of overwriting what the new owner recorded.
- **Skip, don't block.** `FOR UPDATE SKIP LOCKED` lets eight workers poll one table; the test hands three operations to eight concurrent claimers and checks that each goes to exactly one.
- **Bounded, honest retries.** Backoff is exponential with jitter. After `maxAttempts` the row is *stuck* and listed by `ops.stuck()`. It is never marked failed, because we do not know that it failed.

## Webhook delivery with replay

```ts
const hooks = new WebhookStore(pool, { clock: systemClock, maxAttempts: 8, disableAfterFailures: 10 });

await hooks.createEndpoint("https://partner.example/hooks", secret);
await hooks.publish("booking.created", { bookingId }, deterministicEventId);   // ledger + fan-out
await hooks.deliverDue(transport);                                             // worker loop
await hooks.replay(endpointId, since);                                         // partner fixed their receiver
```

- **Deterministic event ids.** A caller that might retry passes an id derived from the domain change. The second publish is a no-op: no second event, no second deliveries. The (endpoint, event) unique key makes fan-out idempotent as well.
- **Signatures.** Header `webhook-signature: t=<unix>,v1=<hex hmac-sha256(secret, t + "." + body)>`. The timestamp is inside the signed message, so a captured body cannot be replayed with a fresh timestamp. `verify()` is constant-time and rejects stale timestamps.
- **Backoff and auto-disable.** 5xx or a transport error schedules a retry with exponential backoff; after `maxAttempts` the delivery is failed. An endpoint that fails `disableAfterFailures` times in a row is disabled so a dead URL stops consuming retries; a success resets the counter; `enableEndpoint` turns it back on.
- **Replay.** `replay(endpointId, since, until)` returns delivered and failed rows to pending with a fresh attempt budget. Receivers see the same event ids, so an idempotent receiver is unaffected by the duplicates.

## How the tests work

- `@testcontainers/postgresql` starts PostgreSQL 16 for the run; set `DATABASE_URL` to use an existing server instead.
- Time is injected (`ManualClock`), so lease expiry, backoff and replay windows are tested by moving the clock, never by sleeping.
- The provider and the HTTP transport are scripted fakes that record every call (`ScriptedProvider`, `ScriptedTransport`). The provider fake can apply a write and then lose the answer, which is the case the recovery logic exists for.
- Concurrency tests use real concurrent queries against the real database, because `SKIP LOCKED` semantics are the point.

## Layout

```
src/
  schema.sql       tables and the state constraints
  db.ts            pool, migrate, withTransaction
  clock.ts         Clock, ManualClock, backoff with jitter
  idempotency.ts   IdempotencyStore
  provider.ts      Provider interface, LostAnswerError, ScriptedProvider
  operations.ts    OperationStore: enqueue, claim, execute, renew, stuck
  webhooks.ts      WebhookStore: endpoints, publish, deliverDue, replay; sign/verify
test/
  idempotency.test.ts
  operations.test.ts
  webhooks.test.ts
  helpers/         Testcontainers global setup, truncate helper
```

## What this is not

It is a reference implementation, not a library: no HTTP layer, no scheduler, no metrics. In a real service each store sits behind a small module boundary, the worker loop is a cron or a long-running process with lease renewal on a timer, and every transition emits a span. The domain (bookings) is generic and nothing here is taken from an employer's codebase.

MIT licensed. Written by [Yurii Maidaniuk](https://github.com/maydan90).
