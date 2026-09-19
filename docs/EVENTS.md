# Published event topics

All events use the signed FHIR-aligned envelope v1.0 defined in
`internal/events/envelope.go` (producer `s1-port-interoperability`). Envelopes
are staged transactionally in the per-module outbox tables and drained by the
NSW adapter / outbox drain (`internal/nswadapter/drain.go`). The full set of
topics is logged at startup (`port-interoperability published topics: ...`).

| Topic | Producer module | Consumer | Schema ref |
|---|---|---|---|
| `ports.booking.v1` | internal/booking | singlewindow `pcsProjection.ts` (started in `server/_core/index.ts`) | internal/events/envelope.go |
| `ports.gate.v1` | internal/booking (gate moves) | reserved — no consumer yet | internal/events/envelope.go |
| `ports.queue.v1` | internal/queue | reserved — no consumer yet | internal/events/envelope.go |
| `trade.declarations.v1` | internal/declarations (event types `trade.declaration.{submitted,risk-assessed,scoring-unavailable,cleared,amended}.v1`; `cleared` also drained by nswadapter) | reserved — no consumer yet | internal/declarations/model.go |
| `ports.offshore.v1` | internal/offshore | reserved — no consumer yet | internal/events/envelope.go |
| `ports.manifests.v1` | internal/manifests | reserved — no consumer yet | internal/events/envelope.go |
| `ports.cruise.v1` | internal/cruise | reserved — no consumer yet | internal/events/envelope.go |
| `finance.revenue-assessments.v1` | internal/tariff (deterministic fee/dues assessments) | blueeconomy-financial-controls `internal/revenueintake` | internal/events/envelope.go |
| `ports.securechain.v1` | internal/securechain (WP-7) | reserved — no consumer yet | internal/events/envelope.go |
| `registry.vessel.v1` | internal/registry | reserved — no consumer yet | internal/events/envelope.go |
| `registry.seafarer.v1` | internal/registry | reserved — no consumer yet | internal/events/envelope.go |
| `registry.cabotage.v1` | internal/registry | reserved — no consumer yet | internal/events/envelope.go |
| `registry.fisheries.v1` | internal/registry (Phase 19 fisheries licensing) | reserved — no consumer yet | internal/events/envelope.go |
| `ports.waste-receipts.v1` | internal/waste (MARPOL PRF receipts) | reserved — no consumer yet | internal/events/envelope.go |

"Reserved" topics are published deliberately: the signed-envelope production
path is live and the topics exist for downstream reconciliation/audit
consumers scheduled in later phases. Do not remove producers; wire consumers
or update this table when one appears.

## Reserved tables

`portcall_tenant_backfill_mappings` and `portcall_tenant_quarantine`
(migration `0007_tenant_expand.sql`) are intentionally EXPAND-ONLY
scaffolding for the pending tenant backfill release (see the migration
header); they must not be written until the mapping release is approved, and
must not be dropped while that release is pending. Note: the audit's third
"dead table", `port_agency_profiles` (migration 0005), no longer exists — it
was renamed to `port_agency_profile_versions` by migration 0006 and is live
(`internal/portcall/store.go`).

## Removed surface (Phase 20)

- `POST /v1/push-tokens` and `POST /v1/push-tokens/revoke` REST handlers and
  the `internal/pushtokens` store were removed: they duplicated the live
  singlewindow `pushTokens` tRPC router (the path the Flutter app actually
  calls) and had zero callers. Historical migrations are untouched; the
  `push_tokens` table remains for audit history but is no longer written.
- The unpublished event-type constant `trade.declaration.rejected.v1` was
  removed from `internal/declarations/model.go` (rejection is modelled as a
  status transition, not an emitted event).
