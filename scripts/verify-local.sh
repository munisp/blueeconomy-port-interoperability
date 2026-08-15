#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
docker_prefix=()
if ! docker info >/dev/null 2>&1; then
  if sudo -n docker info >/dev/null 2>&1; then
    docker_prefix=(sudo docker)
  else
    echo 'Docker daemon is unavailable to the current user' >&2
    exit 1
  fi
fi
compose=("${docker_prefix[@]}" compose -f docker-compose.integration.yml)
server_pid=''
server_binary=''
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_binary" ]]; then
    rm -f "$server_binary"
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" up -d --wait postgres
server_binary=$(mktemp)
GOFLAGS='' go build -o "$server_binary" ./cmd/port-interoperability
DATABASE_URL='postgres://blueeconomy:local-only-integration-password@127.0.0.1:55433/blueeconomy_port?sslmode=disable' \
MIGRATION_PATH="$repo_root/db/migrations/0001_port_calls.sql" \
PORT=18080 \
"$server_binary" >"$repo_root/.integration-server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 30); do
  if curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null; then break; fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null

payload='{"call_id":"call-001","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"decl-001","submitted_by":"agent-001"}'
create=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: idem-001' --data "$payload")
printf '%s' "$create" | grep -q '"status":"DRAFT"'
printf '%s' "$create" | grep -q '"version":1'
replay=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: idem-001' --data "$payload")
test "$create" = "$replay"

if curl --silent --show-error -o /tmp/port-call-conflict.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: idem-001' \
  --data '{"call_id":"call-001","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"changed","submitted_by":"agent-001"}' | grep -q '^409$'; then :; else
  echo 'conflicting idempotency replay was not rejected' >&2
  exit 1
fi

submitted=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls/call-001/submit \
  -H 'Content-Type: application/json' --data '{"expected_version":1}')
printf '%s' "$submitted" | grep -q '"status":"SUBMITTED"'
printf '%s' "$submitted" | grep -q '"version":2'
accepted=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls/call-001/accept \
  -H 'Content-Type: application/json' --data '{"expected_version":2}')
printf '%s' "$accepted" | grep -q '"status":"ACCEPTED"'
printf '%s' "$accepted" | grep -q '"version":3'

container_id=$("${docker_prefix[@]}" ps --filter name=port-interoperability-postgres -q | head -n1)
outbox_count=$("${docker_prefix[@]}" exec "$container_id" psql -p 55433 -U blueeconomy -d blueeconomy_port -Atc 'select count(*) from port_call_outbox where call_id = '\''call-001'\'';')
test "$outbox_count" = 3
printf '%s\n' 'S1 real PostgreSQL integration passed: create, exact replay, conflicting replay rejection, transitions and outbox atomicity.'
