-- durable-integrations schema. Applied by migrate() in db.ts; idempotent.

-- 1. Idempotency keys: one row per (scope, key). The request digest detects
--    "same key, different payload", which is a client bug and must not replay.
create table if not exists idempotency_keys (
  scope         text        not null,
  key           text        not null,
  request_hash  text        not null,
  status        text        not null check (status in ('in_progress', 'completed')),
  response      jsonb,
  response_status int,
  created_at    timestamptz not null,
  expires_at    timestamptz not null,
  primary key (scope, key)
);

--    response_status is the HTTP status of the first response (null for keys
--    stored through run(), which has no status). Added after the first release,
--    so existing databases get it here; migrate() stays safe to run twice.
alter table idempotency_keys add column if not exists response_status int;

-- 2. Provider operations: a ledger of writes to a third party. The request_id
--    is generated once and sent on every attempt so the provider can dedupe.
--    States:
--      pending    never attempted (or backing off after an unknown outcome)
--      in_flight  a worker holds the lease and is talking to the provider
--      succeeded  the provider confirmed the write
--      refused    the provider definitively rejected it; nothing changed
--      unknown    an attempt ended without an answer; the write MAY have applied
--      stuck      attempts exhausted while still unknown; needs a human
create table if not exists provider_operations (
  id               uuid        primary key,
  kind             text        not null,
  request_id       text        not null unique,
  payload          jsonb       not null,
  state            text        not null check (state in ('pending', 'in_flight', 'succeeded', 'refused', 'unknown', 'stuck')),
  attempts         int         not null default 0,
  max_attempts     int         not null,
  lease_owner      text,
  lease_until      timestamptz,
  next_attempt_at  timestamptz not null,
  result           jsonb,
  last_error       text,
  created_at       timestamptz not null,
  updated_at       timestamptz not null
);

create index if not exists provider_operations_claimable
  on provider_operations (next_attempt_at)
  where state in ('pending', 'unknown', 'in_flight');

-- 3. Webhooks: an append-only event ledger, endpoints with a signing secret,
--    and one delivery row per (endpoint, event) so a retry can never fan out twice.
create table if not exists webhook_endpoints (
  id                    uuid        primary key,
  url                   text        not null,
  secret                text        not null,
  enabled               boolean     not null default true,
  consecutive_failures  int         not null default 0,
  created_at            timestamptz not null
);

create table if not exists webhook_events (
  id          uuid        primary key,
  type        text        not null,
  payload     jsonb       not null,
  created_at  timestamptz not null
);

create table if not exists webhook_deliveries (
  id               uuid        primary key,
  endpoint_id      uuid        not null references webhook_endpoints (id),
  event_id         uuid        not null references webhook_events (id),
  state            text        not null check (state in ('pending', 'in_flight', 'delivered', 'failed')),
  attempts         int         not null default 0,
  max_attempts     int         not null,
  next_attempt_at  timestamptz not null,
  lease_until      timestamptz,
  last_status      int,
  last_error       text,
  created_at       timestamptz not null,
  updated_at       timestamptz not null,
  unique (endpoint_id, event_id)
);

create index if not exists webhook_deliveries_due
  on webhook_deliveries (next_attempt_at)
  where state in ('pending', 'in_flight');
