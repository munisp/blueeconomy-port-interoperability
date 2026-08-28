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
                                                     │          queue.Store      │
                                                     │  (all SQL runs inside     │
                                                     │   WithTenantTx + RLS)     │
                                                     ▼                            ▼
 USSD (Africa's Talking POST) ──► cmd/ussd-gateway ──► booking.Directory    PostgreSQL
        sessions in Redis (TTL, fail-closed)             │                  (migrations
                                                        ▼                   0001–0011)
 payments.Gateway (Mojaloop, HTTPS + bearer, idempotent request_id)
 booking.Orchestrator (Temporal): ECallUpBookingWorkflow
        receipt-check ─► gate-scan approval ─► TigerBeetle commit ─► audit commit
 queue.CallUpOrchestrator (Temporal): ECallUpCallUpWorkflow
        grace-window timer ─► forfeit ─► next-in-queue promotion (chain)
                                                     │
 platform_outbox ──► cmd/outbox-publisher ──► Kafka ports.booking.v1 / ports.gate.v1 / ports.queue.v1
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
  (`sessionId`, `phoneNumber`, `text`) with menus for booking status, slot
  booking by ID, queue entry and queue position; sessions live in Redis with a
  TTL and the gateway refuses to start or serve without it.

eCallUp 2.0 truck call-up queue (new):

- Fail-closed state machine `REQUESTED -> QUEUED -> CALLED_UP -> EN_ROUTE ->
  ARRIVED` with `EXPIRED` / `FORFEITED` / `CANCELLED` /
  `RECONCILIATION_REQUIRED` terminal-style branches. Any unlisted transition is
  rejected in code and by the database CHECK constraints; a called-up request
  cannot exist without a grace deadline and an arrived one without an arrival
  timestamp.
- Per-terminal FIFO with priority classes (`PERISHABLE`, `PRIORITY`,
  `STANDARD`): priority cargo jumps the standard queue but never reorders
  within its class. Positions are assigned atomically under the terminal row
  lock and pinned by `UNIQUE (terminal_id, position)` — exactly one winner per
  position even under concurrent creators.
- Queue requests are idempotent on `(tenant_id, idempotency_key)` and either
  reference an existing booking or create a PENDING (DRAFTED) booking priced at
  the terminal fee atomically in the same transaction.
- Call-up engine: when terminal call-up capacity frees (booking slot release
  via the in-transaction capacity listener on cancel/expire/complete, queue
  arrival/cancellation/forfeiture, or the worker sweeper), the head-of-queue is
  promoted to `CALLED_UP` with a grace window (`CALLUP_GRACE_MINUTES`, default
  90). The `enforce_terminal_callup_capacity` trigger makes promotion beyond
  `port_terminals.queue_capacity` impossible at the database level.
- Grace expiry moves the call-up to `FORFEITED` with an audit event and chains
  the next-in-queue promotion — enforced both by `ECallUpCallUpWorkflow`
  (Temporal grace-window timer → forfeit, `arrival-confirmed` signal → arrival
  check) and by the booking-worker sweeper, which also idempotently starts a
  call-up workflow for every active call-up (workflow id =
  `ecallup-callup-<queue_request_id>`).
- Arrival after the grace deadline fails closed: the request is forfeited, the
  audit event emitted and the chain promoted before `422` is returned.
- REST: `POST /v1/queue-requests` (Idempotency-Key header), `GET
  /v1/queue-requests/{id}`, `POST /v1/queue-requests/{id}/arrive|depart|cancel`
  and the operator/npa-officer view `GET /v1/terminals/{id}/queue`; all behind
  the tenant middleware. USSD menu options 3 and 4 request queue entry
  (terminal code + plate, idempotent per session) and check the queue position
  with the call-up state.
- All queue mutations write FHIR-aligned envelopes to `ports.queue.v1` through
  the same transactional `platform_outbox` (classification `INTERNAL`,
  provenance principal + SHA-256 bundle signature).

Nigeria Customs cross-validation (new, migration 0011):

- A booking may bind a cargo declaration (`cargo_declaration_ref`,
  `declared_weight_kg`, `consignee_id`, `operator_id` — all-or-none at create,
  enforced by DB CHECK). Declaration-carrying bookings must clear the Nigeria
  Customs declaration surface before gate approval.
- State machine gate: `PAID -> VALIDATION_PENDING -> PAID` on MATCH, or
  `VALIDATION_PENDING -> REJECTED` with a stable reason code
  (`DECLARATION_NOT_FOUND`, `DECLARATION_STATUS_INVALID`,
  `WEIGHT_TOLERANCE_EXCEEDED`, `CONSIGNEE_MISMATCH`, `OPERATOR_MISMATCH`).
  `VALIDATION_PENDING` still occupies slot capacity; gate scans of
  declaration-carrying bookings re-check for a MATCH row, fail-closed.
- Rules: declaration exists, status `VALID`/`RELEASED`, declared cargo weight
  within `CUSTOMS_WEIGHT_TOLERANCE_BPS` (inclusive boundary) of the booking
  declaration, consignee and operator match.
- The `CustomsValidator` HTTP client is fail-closed: HTTPS only, bearer or
  mTLS per config, bounded body, explicit timeout. Validator unreachable → the
  booking stays `VALIDATION_PENDING` (activity retries); mismatch → `REJECTED`.
- Workflow ordering is receipt-check → customs-validation → gate-scan →
  ledger → audit-commit. Every decision is persisted to
  `customs_validations` (append-only, tenant RLS) and emitted as
  `booking.customs_validated` on `ports.booking.v1` (envelope v1.0,
  classification `INTERNAL`) with decision + reason code.

NSW outbound adapter (`cmd/nsw-adapter`, new):

- Drains NSW-relevant outbox rows — `booking.drafted`/`booking.paid`, gate
  scan decisions, `port_call.clearance_decided`, `queue.called_up` — and posts
  them to the NSW operator endpoint as signed messages, mirroring the inbound
  security posture: RS256 compact JWS (`X-NSW-Signature`) with `kid` header
  and `iss`/`aud`/`sub`/`tenant_id`/`jti`/`exp` claims plus a
  `payload_sha256` claim binding the signature to the exact body bytes.
- HTTPS-only with a pinned CA (`NSW_CA_CERT_FILE`), redirects never followed,
  bounded response body, per-attempt timeout. A 409 replay dedup counts as
  delivered (the `jti` is stable per delivery). Fail-closed: no signing key,
  no pinned CA or a non-HTTPS endpoint refuses startup.
- At-least-once with per-event state in `nsw_delivery` (tenant RLS):
  `PENDING -> DELIVERED`, or `PENDING -> FAILED_PERMANENT` after
  `NSW_MAX_ATTEMPTS` with exponential backoff (`NSW_BACKOFF_BASE` doubling,
  capped at `NSW_BACKOFF_MAX`). Nothing is silently dropped.
- Content negotiation by config: `application/json` (default, raw envelope)
  or `application/xml` — the documented `NSWPortCallEvent` v1.0 schema
  (`EventID`, `CallReference`, `EventType`, `OccurredAt`, `TenantID`,
  `PayloadSHA256`, `Payload`).

## Processes

| Command | Purpose | Required env (fail-closed) |
| --- | --- | --- |
| `cmd/port-interoperability` | HTTP API (port calls, bookings, call-up queue, gate scans, NSW ingress) | `DATABASE_URL`, `MIGRATION_PATH`, `PORT`, `AUTH_MODE=loopback_trusted_proxy`, `TENANT_GATEWAY_KEY` (≥32 bytes) / `_ISS` / `_AUD`, `NSW_JWKS_URL` (HTTPS) / `NSW_JWKS_PIN_SHA256` / `NSW_ALLOWED_KIDS` / `NSW_ISSUER` / `NSW_AUDIENCE`, `MOJALOOP_BASE_URL` (HTTPS) / `MOJALOOP_BEARER_TOKEN`, `TEMPORAL_ADDRESS` / `_NAMESPACE` / `_TASK_QUEUE`, `FGN_SHARE_BASIS_POINTS` (optional `CALLUP_GRACE_MINUTES`, default 90) |
| `cmd/ussd-gateway` | USSD callback handler | `REDIS_URL`, `DATABASE_URL`, `MIGRATION_PATH`, `PORT`, `USSD_TENANT_ID` (optional `USSD_SESSION_TTL_SECONDS`, default 300) |
| `cmd/booking-worker` | Temporal worker for `ECallUpBookingWorkflow` and `ECallUpCallUpWorkflow`, plus the call-up sweeper | `TEMPORAL_ADDRESS` / `_NAMESPACE` / `_TASK_QUEUE`, `DATABASE_URL`, `TIGERBEETLE_CLUSTER_ID`, `TIGERBEETLE_ADDRESSES`, `WORKER_TENANT_ID`, `CUSTOMS_BASE_URL` (HTTPS) + exactly one of `CUSTOMS_BEARER_TOKEN` or `CUSTOMS_CLIENT_CERT_FILE` + `CUSTOMS_CLIENT_KEY_FILE` (optional `CUSTOMS_CA_CERT_FILE` pinned CA, `CUSTOMS_TIMEOUT` default 10s, `CUSTOMS_WEIGHT_TOLERANCE_BPS` default 500, `CALLUP_GRACE_MINUTES` default 90, `QUEUE_SWEEP_INTERVAL_SECONDS` default 60) |
| `cmd/outbox-publisher` | Kafka publisher for `platform_outbox` | `DATABASE_URL`, `KAFKA_BROKERS` (optional `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL`) |
| `cmd/nsw-adapter` | Signed NSW outbound delivery of NSW-relevant outbox events | `DATABASE_URL`, `NSW_ENDPOINT_URL` (HTTPS), `NSW_CA_CERT_FILE` (pinned CA), `NSW_SIGNING_KEY_FILE` (PEM RSA ≥2048), `NSW_SIGNING_KID`, `NSW_OUTBOUND_AUDIENCE` (optional `NSW_OUTBOUND_ISSUER` default `s1-port-interoperability`, `NSW_OUTBOUND_SUBJECT` default `nsw-adapter`, `NSW_TOKEN_TTL` default 5m, `NSW_CONTENT_TYPE` `application/json` default or `application/xml`, `NSW_TIMEOUT` default 10s, `NSW_MAX_ATTEMPTS` default 8, `NSW_BACKOFF_BASE` default 5s, `NSW_BACKOFF_MAX` default 10m, `NSW_BATCH_SIZE` default 100, `NSW_POLL_INTERVAL` default 5s, `NSW_MAX_BODY_BYTES` default 1 MiB) |

Optional: `NSW_REPLAY_TTL_MINUTES` (default 1440).

## Local verification

Run:

```bash
scripts/verify-local.sh
```

The script starts a real PostgreSQL 16.4 container, applies migrations
0001–0011, mints local gateway tenant tokens, and verifies the tenant-wired
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

PostgreSQL-backed queue store tests (position race, priority ordering, call-up
chain, grace forfeiture, booking-release hook) run the same way; a dedicated
database keeps the package race-safe against the booking schema reset:

```bash
QUEUE_TEST_DATABASE_URL='postgres://blueeconomy:local-only-integration-password@127.0.0.1:55433/blueeconomy_port?sslmode=disable' \
  go test ./internal/queue/ -v
```

PostgreSQL-backed customs-gate and NSW delivery tests run against the same
database (`NSW_TEST_DATABASE_URL` falls back to `BOOKING_TEST_DATABASE_URL`):

```bash
BOOKING_TEST_DATABASE_URL='postgres://blueeconomy:local-only-integration-password@127.0.0.1:55433/blueeconomy_port?sslmode=disable' \
  go test ./internal/booking/ -run 'TestCustoms' -v
NSW_TEST_DATABASE_URL='postgres://blueeconomy:local-only-integration-password@127.0.0.1:55433/blueeconomy_port?sslmode=disable' \
  go test ./internal/nswadapter/ -run 'TestDrain' -v
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
- **Call-up reconciliation**: queue requests stuck in
  `RECONCILIATION_REQUIRED` are listed with
  `SELECT queue_request_id, reconciliation_reason FROM truck_queue_requests
  WHERE status = 'RECONCILIATION_REQUIRED'`; resolve with the requeue store
  path (tail position, class preserved) or cancel. Forfeited call-ups carry
  `forfeit_reason`; the sweeper and the Temporal grace timer both enforce the
  deadline, so a stuck worker never silently extends a grace window.
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
