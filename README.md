# Blue Economy Port Interoperability

This repository implements the first real S1 port-call workflow increment for the Blue Economy Platform. It is a Go HTTP service backed by PostgreSQL and is deliberately limited to the implemented port-call state machine; it does not claim IMO Maritime Single Window or partner conformance until the Ministry and port agencies provide approved interface profiles and non-production endpoints.

## Implemented controls

The service provides:

- PostgreSQL-backed port-call creation with required `Idempotency-Key`.
- Atomic exact replay and fail-closed conflicting-key rejection.
- Strict seven-digit vessel IMO identifier and uppercase port-code validation.
- Version-checked `DRAFT -> SUBMITTED -> ACCEPTED|REJECTED` transitions.
- Database-enforced status and version constraints.
- Transactional append-only outbox events for creation and status changes.
- HTTP read, create, submit, accept and reject endpoints.
- Bounded request bodies, JSON unknown-field rejection and hardened HTTP server timeouts.

## Local verification

Run:

```bash
scripts/verify-local.sh
```

The script starts a real PostgreSQL 16.4 container, applies `db/migrations/0001_port_calls.sql`, starts the service with an explicit database URL, and verifies creation, exact replay, conflicting replay rejection, optimistic transitions and three persisted outbox events. Docker access through passwordless `sudo` is supported for the sandbox environment.

The service requires `DATABASE_URL`, `MIGRATION_PATH` and `PORT`; it does not create a database, invent partner routes or use an in-memory fallback.

## Current boundary

This is an implemented local S1 foundation, not a complete Maritime Single Window. Remaining work includes the approved IMO/NSW message profile, port-agency adapters, document declarations, clearance decisions, authentication enforcement, external acknowledgements, workflow orchestration, conformance tests and Ministry acceptance.
