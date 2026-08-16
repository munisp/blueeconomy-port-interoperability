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
MIGRATION_PATH="$repo_root/db/migrations/0001_port_calls.sql,$repo_root/db/migrations/0002_documents_clearance.sql,$repo_root/db/migrations/0003_document_review.sql,$repo_root/db/migrations/0004_document_supersession_clearance_amendment.sql" \
PORT=18080 \
AUTH_MODE=loopback_trusted_proxy \
"$server_binary" >"$repo_root/.integration-server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 30); do
  if curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null; then break; fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null
if curl --silent --show-error -o /tmp/port-call-unauthenticated.json -w '%{http_code}' \
  -X GET http://127.0.0.1:18080/v1/port-calls/call-001 | grep -q '^401$'; then :; else
  echo 'unauthenticated API request was not rejected' >&2
  exit 1
fi

payload='{"call_id":"call-001","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"decl-001","submitted_by":"agent-001"}'
create=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' -H 'Idempotency-Key: idem-001' --data "$payload")
printf '%s' "$create" | grep -q '"status":"DRAFT"'
printf '%s' "$create" | grep -q '"version":1'
replay=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' -H 'Idempotency-Key: idem-001' --data "$payload")
test "$create" = "$replay"

if curl --silent --show-error -o /tmp/port-call-conflict.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' -H 'Idempotency-Key: idem-001' \
  --data '{"call_id":"call-001","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"changed","submitted_by":"agent-001"}' | grep -q '^409$'; then :; else
  echo 'conflicting idempotency replay was not rejected' >&2
  exit 1
fi

submitted=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls/call-001/submit \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' --data '{"expected_version":1}')
printf '%s' "$submitted" | grep -q '"status":"SUBMITTED"'
printf '%s' "$submitted" | grep -q '"version":2'
accepted=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls/call-001/accept \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' --data '{"expected_version":2}')
printf '%s' "$accepted" | grep -q '"status":"ACCEPTED"'
printf '%s' "$accepted" | grep -q '"version":3'

document_payload='{"document_type":"cargo_manifest","media_type":"application/pdf","size_bytes":4096,"sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","declared_by":"integration-agent"}'
document=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls/call-001/documents \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' --data "$document_payload")
printf '%s' "$document" | grep -q '"status":"DECLARED"'
document_replay=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls/call-001/documents \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' --data "$document_payload")
test "$document" = "$document_replay"
if curl --silent --show-error -o /tmp/document-conflict.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls/call-001/documents \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' --data "${document_payload/4096/4097}" | grep -q '^409$'; then :; else
  echo 'conflicting document declaration was not rejected' >&2
  exit 1
fi
document_id=$(printf '%s' "$document" | python3 -c 'import json,sys; print(json.load(sys.stdin)["document_id"])')
if curl --silent --show-error -o /tmp/clearance-before-document-review.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls/call-001/clearance \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-checker' \
  --data '{"expected_version":3,"decision":"APPROVED","reason":"attempt before document verification","decided_by":"integration-checker"}' | grep -q '^422$'; then :; else
  echo 'clearance approval before document verification was not rejected' >&2
  exit 1
fi
review=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls/call-001/documents/review \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-checker' \
  --data "{\"document_id\":\"$document_id\",\"expected_version\":1,\"status\":\"VERIFIED\",\"reviewed_by\":\"integration-checker\",\"reason\":\"digest and metadata verified\"}")
printf '%s' "$review" | grep -q '"status":"VERIFIED"'
printf '%s' "$review" | grep -q '"version":2'
clearance=$(curl --fail --silent -X POST http://127.0.0.1:18080/v1/port-calls/call-001/clearance \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-checker' \
  --data '{"expected_version":3,"decision":"APPROVED","reason":"all required declaration evidence verified","decided_by":"integration-checker"}')
printf '%s' "$clearance" | grep -q '"decision":"APPROVED"'
printf '%s' "$clearance" | grep -q '"call_version":4'

container_id=$("${docker_prefix[@]}" ps --filter name=port-interoperability-postgres -q | head -n1)
outbox_count=$("${docker_prefix[@]}" exec "$container_id" psql -p 55433 -U blueeconomy -d blueeconomy_port -Atc 'select count(*) from port_call_outbox where call_id = '\''call-001'\'';')
test "$outbox_count" = 6
printf '%s\n' 'S1 real PostgreSQL integration passed: create, exact replay, conflicting replay rejection, document declaration replay/conflict, approval-before-document-verification rejection, document review/version control, clearance decision/version control and outbox atomicity.'
