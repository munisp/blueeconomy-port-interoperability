#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
docker_prefix=()
if ! docker info >/dev/null 2>&1; then
  docker_prefix=(sudo docker)
fi
compose=("${docker_prefix[@]}" compose -f docker-compose.integration.yml)
server_pid=''
server_binary=''
cleanup() {
  [[ -z "$server_pid" ]] || { kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; }
  [[ -z "$server_binary" ]] || rm -f "$server_binary"
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" up -d --wait postgres
server_binary=$(mktemp)
go build -o "$server_binary" ./cmd/port-interoperability
DATABASE_URL='postgres://blueeconomy:local-only-integration-password@127.0.0.1:55433/blueeconomy_port?sslmode=disable' \
MIGRATION_PATH="$repo_root/db/migrations/0001_port_calls.sql,$repo_root/db/migrations/0002_documents_clearance.sql,$repo_root/db/migrations/0003_document_review.sql,$repo_root/db/migrations/0004_document_supersession_clearance_amendment.sql,$repo_root/db/migrations/0005_agency_profiles.sql,$repo_root/db/migrations/0006_profile_binding_and_append_only_ledger.sql" \
PORT=18081 AUTH_MODE=loopback_trusted_proxy "$server_binary" >"$repo_root/.ledger-contract-server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 30); do curl -fsS http://127.0.0.1:18081/healthz >/dev/null && break; sleep 1; done
curl -fsS http://127.0.0.1:18081/healthz >/dev/null

auth=(-H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: ledger-contract')
profile='{"profile_id":"npa-ledger","version":"2026-08-17","agency_code":"NPA","profile_sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","registered_by":"ledger-contract","active":true}'
curl -fsS -X POST http://127.0.0.1:18081/v1/agency-profiles "${auth[@]}" --data "$profile" >/dev/null
call='{"call_id":"ledger-call-001","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"ledger-decl-001","submitted_by":"ledger-contract","agency_profile_id":"npa-ledger","agency_profile_version":"2026-08-17"}'
curl -fsS -X POST http://127.0.0.1:18081/v1/port-calls "${auth[@]}" -H 'Idempotency-Key: ledger-idem-001' --data "$call" >/dev/null

container_id=$("${docker_prefix[@]}" ps --filter name=port-interoperability-postgres -q | head -n1)
psql() { "${docker_prefix[@]}" exec "$container_id" psql -p 55433 -U blueeconomy -d blueeconomy_port -v ON_ERROR_STOP=1 -Atc "$1"; }

[[ "$(psql "SELECT is_nullable FROM information_schema.columns WHERE table_name='port_calls' AND column_name='agency_profile_id'")" == 'NO' ]]
[[ "$(psql "SELECT is_nullable FROM information_schema.columns WHERE table_name='port_calls' AND column_name='agency_profile_version'")" == 'NO' ]]
[[ "$(psql "SELECT count(*) FROM port_agency_profile_events WHERE profile_id='npa-ledger' AND version='2026-08-17' AND event_type='REGISTERED'")" == '1' ]]
[[ "$(psql "SELECT active FROM port_agency_profile_events WHERE profile_id='npa-ledger' ORDER BY event_sequence DESC LIMIT 1")" == 't' ]]

if psql "UPDATE port_agency_profile_events SET actor='tamper' WHERE profile_id='npa-ledger'" >/dev/null 2>&1; then echo 'event update unexpectedly succeeded' >&2; exit 1; fi
if psql "DELETE FROM port_agency_profile_events WHERE profile_id='npa-ledger'" >/dev/null 2>&1; then echo 'event delete unexpectedly succeeded' >&2; exit 1; fi
if psql "UPDATE port_agency_profile_versions SET agency_code='NIMASA' WHERE profile_id='npa-ledger'" >/dev/null 2>&1; then echo 'version mutation unexpectedly succeeded' >&2; exit 1; fi
if psql "DELETE FROM port_agency_profile_versions WHERE profile_id='npa-ledger'" >/dev/null 2>&1; then echo 'version delete unexpectedly succeeded' >&2; exit 1; fi

inactive='{"profile_id":"npa-ledger","version":"2026-08-17","agency_code":"NPA","profile_sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","registered_by":"ledger-contract","active":false}'
curl -fsS -X POST http://127.0.0.1:18081/v1/agency-profiles "${auth[@]}" --data "$inactive" >/dev/null
[[ "$(psql "SELECT active FROM port_agency_profile_events WHERE profile_id='npa-ledger' ORDER BY event_sequence DESC LIMIT 1")" == 'f' ]]
if curl -sS -o /tmp/ledger-inactive-create.json -w '%{http_code}' -X POST http://127.0.0.1:18081/v1/port-calls "${auth[@]}" -H 'Idempotency-Key: ledger-idem-002' --data '{"call_id":"ledger-call-002","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"ledger-decl-002","submitted_by":"ledger-contract","agency_profile_id":"npa-ledger","agency_profile_version":"2026-08-17"}' | grep -q '^422$'; then :; else echo 'inactive event did not block create' >&2; exit 1; fi

echo 'S1 append-only agency-profile ledger contract verification passed.'
