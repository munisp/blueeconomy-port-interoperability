# Performance Notes (Phase 11 audit)

Scope: index coverage vs. actual query code, unbounded queries, N+1 patterns,
connection-pool sizing, Kafka producer batching. No behavior changes; tenant
RLS and fail-closed invariants preserved.

## Indexes added (db/migrations/0021_perf_indexes.sql)

| Index | Justifying query |
|---|---|
| `customs_declarations_tenant_created_idx` `(tenant_id, created_at DESC, declaration_id)` | `declarations.Store.List`: `ORDER BY created_at DESC, declaration_id LIMIT/OFFSET` (existing status/trader indexes do not serve the newest-first tenant-wide sort) |
| `truck_queue_tenant_status_idx` (partial, active statuses) | `queue.Store.ListActiveCallups`: `WHERE tenant_id AND status IN ('CALLED_UP','EN_ROUTE') ORDER BY terminal_id, grace_deadline` |
| `passenger_manifest_rejections_tenant_seq_idx` `(tenant_id, rejection_seq)` | `manifests.Store.ListRejectionsPage` tenant-wide rejection queue (existing index leads with manifest_id) |

Verified already-covered (not duplicated): nsw_delivery due-claim partial,
outbox partials, queue head-of-line partials, secure_chain link/audit,
tariff schedule/rule, bookings, port_call documents/outbox tenant partials.

## Query caps / pagination

- `GET manifest rejections` was unbounded when `manifest_id` is empty (whole
  tenant queue). Added `manifests.Store.ListRejectionsPage` with a fail-closed
  cap (default 500, max 5000, out-of-range → default), wired to a new `limit`
  query parameter. `ListRejections` keeps its old signature and now returns
  the capped first page (deterministic `rejection_seq` order).
- `declarations.Store.List` was already paginated with a fail-closed limit;
  NSW delivery claim and queue head reads use `LIMIT ... FOR UPDATE
  (SKIP LOCKED)`. No other unbounded list endpoints found.

## N+1

None found: booking/queue/manifest flows use single set-based statements or
transactional multi-statement writes, not per-row loops.

## Connection pool sizing (env, opt-in)

`telemetry.ApplyPoolEnv` is applied in `telemetry.NewPGXPool` and in every
store `Open` (booking, cruise, declarations, manifests, offshore, portcall,
pushtokens, queue, securechain, tariff). Unset variables keep pgx defaults:

- `PORTIO_DB_POOL_MAX_CONNS` (default: pgx default = max(4, NumCPU))
- `PORTIO_DB_POOL_MIN_CONNS` (default: 0)
- `PORTIO_DB_POOL_MAX_CONN_IDLE_SEC` (default: 1800)
- `PORTIO_DB_POOL_MAX_CONN_LIFE_SEC` (default: 3600)

Invalid values fail closed at startup.

## Kafka producer batching (env, opt-in)

`outbox-publisher` (franz-go) honors `PORTIO_KAFKA_LINGER_MS` and
`PORTIO_KAFKA_MAX_BATCH_BYTES` (both default to franz-go defaults = unchanged).
`RequiredAcks=AllISR` is untouched.

## Remaining recommendations (not implemented)

- `nswadapter.drain` iterates all active tenants each tick; fine while tenant
  count is small, shard the runner by tenant hash if it grows.
- `tariff.Store.List` history scans per schedule are bounded by
  schedule_id index; consider keyset pagination if schedule history deepens.
