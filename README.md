# Blue Economy Port Interoperability

This repository implements the S1 port-call workflow and the eCallUp 2.0
per-truck port access booking service for the Blue Economy Platform. It is a Go
HTTP service backed by PostgreSQL with transactional outbox events to Kafka,
Temporal workflow orchestration, a Mojaloop NGN payment boundary and a
TigerBeetle double-entry settlement ledger. It does not claim IMO Maritime
Single Window or partner conformance until the Ministry and port agencies
provide approved interface profiles and non-production endpoints.

## Wiring diagram

```
                      HS256 gateway token (tenantctx.Verifier)
 trucker / operator ──► /v1/bookings, /v1/slots, /v1/gate/scans, /v1/port-calls ─┐
                      (loopback trusted proxy + tenant middleware)                │
 NSW authority ──────► POST /v1/nsw/port-calls                                    │
   RS256 JWS (X-NSW-Signature, pinned JWKS, replay store)                         ▼
                                              internal/server ──► portcall.Store ─┐
                                                     │          booking.Store    │
                                                     │  (all SQL runs inside     │
                                                     │   WithTenantTx + RLS)     │
                                                     ▼                            ▼
 USSD (Africa's Talking POST) ──► cmd/ussd-gateway ──► booking.Directory    PostgreSQL
        sessions in Redis (TTL, fail-closed)             │                  (migrations
                                                        ▼                   0001–0009)
 payments.Gateway (Mojaloop, HTTPS + bearer, idempotent request_id)
 booking.Orchestrator (Temporal): ECallUpBookingWorkflow
        receipt-check ─► gate-scan approval ─► TigerBeetle commit ─► audit commit
                                                     │
 platform_outbox ──► cmd/outbox-publisher ──► Kafka ports.booking.v1 / ports.gate.v1
        (transactional, at-least-once, event-id key, all-ISR acks,
         FHIR-aligned envelope v1.0 with provenance + sha256 signature)
```

## Implemented controls

Port calls (existing, now tenant-wired):

- PostgreSQL-backed port-call creation with required `Idempotency-Key`, atomic
  exact replay and fail-closed conflicting-key rejection.
- Version-checked `DRAFT -> SUBMITTED -> ACCEPTED|REJECTED` transitions,
  document declarations/review/supersession and clearance decisions/amendments.
- **Tenant middleware is mounted on every `/v1/` route**: a validated HS256
  gateway token (`TENANT_GATEWAY_*`) supplies the tenant claims; a
  caller-supplied `X-Tenant-ID` header is rejected outright.
- **Every store operation runs inside `WithTenantTx`** (`tenantdb.WithTx`),
  setting transaction-local `app.tenant_id` so the migration-0008 row-level
  security policies isolate tenants; inserts stamp `tenant_id` from claims.
- **NSW JWS ingress is mounted at `POST /v1/nsw/port-calls`**: RS256-only
  (shared-secret algorithms are prohibited), pinned HTTPS JWKS with digest pin,
  validated `iss`/`aud`/`exp`/`jti`/`tenant_id` claims, PostgreSQL replay
  protection, and the verified claims become the tenant context.

eCallUp 2.0 truck booking (new):

- Fail-closed state machine `DRAFTED -> SLOT_RESERVED -> PAID -> GATE_APPROVED
  -> COMPLETED` with `CANCELLED` / `EXPIRED` / `RECONCILIATION_REQUIRED`
  terminal-style branches and `PENDING_SYNC` for offline bookings. Any unlisted
  transition is rejected in code and by the database CHECK constraint.
- Time-based terminal slots with capacity enforced twice: row-lock counting in
  the reserving transaction and the `enforce_slot_capacity` trigger — no
  overbooking is possible even under races.
- Offline mode: OFFLINE bookings enter `PENDING_SYNC`; on reconnect
  `POST /v1/bookings/{id}/reconcile` either reserves the slot or moves the
  booking to `RECONCILIATION_REQUIRED` with a recorded reason — never silent.
- Gate scan controller (`POST /v1/gate/scans`) approves only when the booking
  is PAID, has a payment receipt and the scan is inside the slot window; every
  scan (approved or denied) is persisted for audit and emits `ports.gate.v1`.
- Mojaloop NGN payment intents (`POST /v1/bookings/{id}/payment-intents`) are
  idempotent on `request_id` at both the switch (transactionRequestId +
  Idempotency-Key header) and the database (`UNIQUE(tenant_id, request_id)`).
- `ECallUpBookingWorkflow` (Temporal): waits for the `payment-confirmed`
  signal, receipt-checks the persisted payment, waits for the `gate-scan`
  signal, commits the TigerBeetle double entry (trucker-payable →
  terminal-operator + FGN share with deterministic, idempotent transfer ids)
  and audit-commits the booking with the ledger commit hash. Observers query
  progress via the `observer` query handler.
- All booking and gate mutations write FHIR-aligned envelopes
  (`envelopeVersion` 1.0, FHIR R4 `Bundle` of type `message`, provenance with
  principal id/role, SHA-256 bundle signature, ledger commit hash,
  classification `INTERNAL`) to the transactional `platform_outbox`;
  `cmd/outbox-publisher` drains it to Kafka at-least-once with all-ISR acks.
- USSD fallback (`cmd/ussd-gateway`): Africa's Talking-style POST callback
  (`sessionId`, `phoneNumber`, `text`) with menus for booking status and slot
  booking by ID; sessions live in Redis with a TTL and the gateway refuses to
  start or serve without it.

## Processes

| Command | Purpose | Required env (fail-closed) |
| --- | --- | --- |
| `cmd/port-interoperability` | HTTP API (port calls, bookings, gate scans, NSW ingress) | `DATABASE_URL`, `MIGRATION_PATH`, `PORT`, `AUTH_MODE=loopback_trusted_proxy`, `TENANT_GATEWAY_KEY` (≥32 bytes) / `_ISS` / `_AUD`, `NSW_JWKS_URL` (HTTPS) / `NSW_JWKS_PIN_SHA256` / `NSW_ALLOWED_KIDS` / `NSW_ISSUER` / `NSW_AUDIENCE`, `MOJALOOP_BASE_URL` (HTTPS) / `MOJALOOP_BEARER_TOKEN`, `TEMPORAL_ADDRESS` / `_NAMESPACE` / `_TASK_QUEUE`, `FGN_SHARE_BASIS_POINTS` |
| `cmd/ussd-gateway` | USSD callback handler | `REDIS_URL`, `DATABASE_URL`, `MIGRATION_PATH`, `PORT`, `USSD_TENANT_ID` (optional `USSD_SESSION_TTL_SECONDS`, default 300) |
| `cmd/booking-worker` | Temporal worker for `ECallUpBookingWorkflow` | `TEMPORAL_ADDRESS` / `_NAMESPACE` / `_TASK_QUEUE`, `DATABASE_URL`, `TIGERBEETLE_CLUSTER_ID`, `TIGERBEETLE_ADDRESSES` |
| `cmd/outbox-publisher` | Kafka publisher for `platform_outbox` | `DATABASE_URL`, `KAFKA_BROKERS` (optional `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL`) |

Optional: `NSW_REPLAY_TTL_MINUTES` (default 1440).

## Local verification

Run:

```bash
scripts/verify-local.sh
```

The script starts a real PostgreSQL 16.4 container, applies migrations
0001–0009, mints local gateway tenant tokens, and verifies the tenant-wired
port-call flow (create, replay, conflicts, documents, clearance), cross-tenant
invisibility, the booking state machine over HTTP, slot capacity enforcement,
offline reconciliation surfacing, gate denial for unpaid bookings and outbox
atomicity. Docker access through passwordless `sudo` is supported.

PostgreSQL-backed booking store tests (including the slot-capacity race) run
when the database is reachable:

```bash
docker compose -f docker-compose.integration.yml up -d --wait postgres
BOOKING_TEST_DATABASE_URL='postgres://blueeconomy:local-only-integration-password@127.0.0.1:55433/blueeconomy_port?sslmode=disable' \
  go test ./internal/booking/ -run 'TestSlot|TestOffline|TestGate|TestBooking' -v
```

Unit tests (state machine, JWS signature suite, USSD session flow, Temporal
testsuite, envelope signatures, ledger determinism) need no services:

```bash
go build ./... && go vet ./... && go test ./...
```

## Runbook notes

- **Startup failures are intentional**: every process exits when its security
  or integration configuration is missing (see the table above). Check the
  process log for the first failing variable.
- **Payment confirmations**: `POST /v1/bookings/{id}/payment-confirmations`
  with `{receipt_ref, expected_version}` records the switch receipt and signals
  the workflow. A `502` means the database committed but the Temporal signal
  failed; retry the same request after the worker is healthy (the workflow id
  is `ecallup-booking-<booking_id>`).
- **Offline reconciliation queue**: find stranded bookings with
  `SELECT booking_id, reconciliation_reason FROM truck_bookings WHERE status =
  'RECONCILIATION_REQUIRED'` (with `SET app.tenant_id = ...`). Resolve by
  reserving a different slot via `POST /v1/bookings/{id}/reserve`.
- **Outbox lag**: unpublished events are
  `SELECT count(*) FROM platform_outbox WHERE published_at IS NULL`. The
  publisher is idempotent — restarting it never duplicates business effects,
  and Kafka record keys are the event ids.
- **Tenant isolation**: direct psql sessions see no rows until
  `SET app.tenant_id = '<tenant>'` because RLS is FORCE-enabled.

## Current boundary

This is an implemented local S1 + eCallUp foundation, not a complete Maritime
Single Window. Remaining work includes the approved IMO/NSW message profile,
port-agency adapters, the Ministry OIDC edge replacing
`loopback_trusted_proxy`, production Mojaloop/TigerBeetle/Temporal cluster
credentials, lakehouse consumers of the published topics, conformance tests and
Ministry acceptance.
