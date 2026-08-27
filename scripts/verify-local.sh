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

# The local run exercises the port-call, booking and tenant-isolation paths.
# Payment intents and workflow starts need the real Mojaloop/Temporal edges;
# they are configured with fail-closed values here and are not invoked by this
# script (the booking paths verified below need only PostgreSQL).
TENANT_GATEWAY_KEY='local-integration-gateway-key-32b'
DATABASE_URL='postgres://blueeconomy:local-only-integration-password@127.0.0.1:55433/blueeconomy_port?sslmode=disable'
export DATABASE_URL \
  MIGRATION_PATH="$repo_root/db/migrations/0001_port_calls.sql,$repo_root/db/migrations/0002_documents_clearance.sql,$repo_root/db/migrations/0003_document_review.sql,$repo_root/db/migrations/0004_document_supersession_clearance_amendment.sql,$repo_root/db/migrations/0005_agency_profiles.sql,$repo_root/db/migrations/0006_profile_binding_and_append_only_ledger.sql,$repo_root/db/migrations/0007_tenant_expand.sql,$repo_root/db/migrations/0008_tenant_rls_enforce.sql,$repo_root/db/migrations/0009_ecallup_booking.sql,$repo_root/db/migrations/0010_queue_callup.sql" \
  PORT=18080 \
  AUTH_MODE=loopback_trusted_proxy \
  TENANT_GATEWAY_KEY \
  TENANT_GATEWAY_ISS='gateway.local' \
  TENANT_GATEWAY_AUD='s1-port-interoperability' \
  NSW_JWKS_URL='https://nsw.local.invalid/jwks.json' \
  NSW_JWKS_PIN_SHA256='sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' \
  NSW_ALLOWED_KIDS='local-nsw-key' \
  NSW_ISSUER='nsw.authority.ng' \
  NSW_AUDIENCE='s1-port-interoperability' \
  MOJALOOP_BASE_URL='https://mojaloop.local.invalid' \
  MOJALOOP_BEARER_TOKEN='local-integration-token' \
  TEMPORAL_ADDRESS='127.0.0.1:7233' \
  TEMPORAL_NAMESPACE='default' \
  TEMPORAL_TASK_QUEUE='ecallup-booking' \
  FGN_SHARE_BASIS_POINTS='250'

"$server_binary" >"$repo_root/.integration-server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 30); do
  if curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null; then break; fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:18080/healthz >/dev/null

container_id=$("${docker_prefix[@]}" ps --filter name=port-interoperability-postgres -q | head -n1)
"${docker_prefix[@]}" exec "$container_id" psql -p 55433 -U blueeconomy -d blueeconomy_port -c \
  "INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES ('tenant-integration', 'local-integration-authority')"

# Mint a gateway tenant token (HS256, the shared-secret edge credential).
mint_token() {
  local subject=$1
  python3 - "$TENANT_GATEWAY_KEY" "$subject" <<'PY'
import base64, hashlib, hmac, json, sys, time
key, subject = sys.argv[1].encode(), sys.argv[2]
b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b'=').decode()
head = b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(',', ':')).encode())
claims = {"iss": "gateway.local", "aud": "s1-port-interoperability",
          "tenant_id": "tenant-integration", "sub": subject,
          "exp": int(time.time()) + 3600}
payload = b64(json.dumps(claims, separators=(',', ':')).encode())
sig = b64(hmac.new(key, f"{head}.{payload}".encode(), hashlib.sha256).digest())
print(f"{head}.{payload}.{sig}")
PY
}
token_admin=$(mint_token integration-admin)
token_agent=$(mint_token integration-agent)
token_checker=$(mint_token integration-checker)
token_supervisor=$(mint_token integration-supervisor)
token_amender=$(mint_token integration-amender)

# api METHOD PATH TOKEN [curl args...]
api() {
  local method=$1 path=$2 token=$3
  shift 3
  curl --fail --silent -X "$method" "http://127.0.0.1:18080$path" \
    -H 'Content-Type: application/json' \
    -H 'X-Trusted-Proxy: loopback' \
    -H "X-Authenticated-Principal: $token" \
    -H "Authorization: Bearer $token" "$@"
}

if curl --silent --show-error -o /tmp/port-call-unauthenticated.json -w '%{http_code}' \
  -X GET http://127.0.0.1:18080/v1/port-calls/call-001 | grep -q '^401$'; then :; else
  echo 'unauthenticated API request was not rejected' >&2
  exit 1
fi
# Authenticated at the edge but without a tenant token must also fail closed.
if curl --silent --show-error -o /tmp/port-call-no-tenant-token.json -w '%{http_code}' \
  -X GET http://127.0.0.1:18080/v1/port-calls/call-001 \
  -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' | grep -q '^401$'; then :; else
  echo 'request without tenant token was not rejected' >&2
  exit 1
fi
# A caller-supplied tenant header is prohibited outright.
if curl --silent --show-error -o /tmp/port-call-forged-tenant.json -w '%{http_code}' \
  -X GET http://127.0.0.1:18080/v1/port-calls/call-001 \
  -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' \
  -H "Authorization: Bearer $token_agent" -H 'X-Tenant-ID: tenant-forged' | grep -q '^400$'; then :; else
  echo 'caller-supplied tenant header was not rejected' >&2
  exit 1
fi

profile='{"profile_id":"npa-lagos","version":"2026-08-16","agency_code":"NPA","profile_sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","registered_by":"integration-admin","active":true}'
api POST /v1/agency-profiles "$token_admin" --data "$profile" | grep -q '"profile_id":"npa-lagos"'
payload='{"call_id":"call-001","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"decl-001","submitted_by":"agent-001","agency_profile_id":"npa-lagos","agency_profile_version":"2026-08-16"}'
create=$(api POST /v1/port-calls "$token_agent" -H 'Idempotency-Key: idem-001' --data "$payload")
printf '%s' "$create" | grep -q '"status":"DRAFT"'
printf '%s' "$create" | grep -q '"version":1'
replay=$(api POST /v1/port-calls "$token_agent" -H 'Idempotency-Key: idem-001' --data "$payload")
test "$create" = "$replay"

if curl --silent --show-error -o /tmp/port-call-unknown-profile.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' -H "Authorization: Bearer $token_agent" -H 'Idempotency-Key: idem-unknown-profile' --data '{"call_id":"call-unknown","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"decl-unknown","submitted_by":"agent-001","agency_profile_id":"unknown","agency_profile_version":"1"}' | grep -q '^404$'; then :; else
  echo 'unknown agency profile was not rejected' >&2
  exit 1
fi
inactive_profile='{"profile_id":"npa-inactive","version":"1","agency_code":"NPA","profile_sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","registered_by":"integration-admin","active":false}'
api POST /v1/agency-profiles "$token_admin" --data "$inactive_profile" >/dev/null
if curl --silent --show-error -o /tmp/port-call-inactive-profile.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' -H "Authorization: Bearer $token_agent" -H 'Idempotency-Key: idem-inactive-profile' --data '{"call_id":"call-inactive","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"decl-inactive","submitted_by":"agent-001","agency_profile_id":"npa-inactive","agency_profile_version":"1"}' | grep -q '^422$'; then :; else
  echo 'inactive agency profile was not rejected' >&2
  exit 1
fi

if curl --silent --show-error -o /tmp/port-call-conflict.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' -H "Authorization: Bearer $token_agent" -H 'Idempotency-Key: idem-001' \
  --data '{"call_id":"call-001","vessel_imo":"1234567","port_code":"LAGOS","declaration_reference":"changed","submitted_by":"agent-001","agency_profile_id":"npa-lagos","agency_profile_version":"2026-08-16"}' | grep -q '^409$'; then :; else
  echo 'conflicting idempotency replay was not rejected' >&2
  exit 1
fi

submitted=$(api POST /v1/port-calls/call-001/submit "$token_agent" --data '{"expected_version":1}')
printf '%s' "$submitted" | grep -q '"status":"SUBMITTED"'
printf '%s' "$submitted" | grep -q '"version":2'
accepted=$(api POST /v1/port-calls/call-001/accept "$token_agent" --data '{"expected_version":2}')
printf '%s' "$accepted" | grep -q '"status":"ACCEPTED"'
printf '%s' "$accepted" | grep -q '"version":3'

document_payload='{"document_type":"cargo_manifest","media_type":"application/pdf","size_bytes":4096,"sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","declared_by":"integration-agent"}'
document=$(api POST /v1/port-calls/call-001/documents "$token_agent" --data "$document_payload")
printf '%s' "$document" | grep -q '"status":"DECLARED"'
document_replay=$(api POST /v1/port-calls/call-001/documents "$token_agent" --data "$document_payload")
test "$document" = "$document_replay"
if curl --silent --show-error -o /tmp/document-conflict.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls/call-001/documents \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' -H "Authorization: Bearer $token_agent" --data "${document_payload/4096/4097}" | grep -q '^409$'; then :; else
  echo 'conflicting document declaration was not rejected' >&2
  exit 1
fi
document_id=$(printf '%s' "$document" | python3 -c 'import json,sys; print(json.load(sys.stdin)["document_id"])')
if curl --silent --show-error -o /tmp/clearance-before-document-review.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls/call-001/clearance \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-checker' -H "Authorization: Bearer $token_checker" \
  --data '{"expected_version":3,"decision":"APPROVED","reason":"attempt before document verification","decided_by":"integration-checker"}' | grep -q '^422$'; then :; else
  echo 'clearance approval before document verification was not rejected' >&2
  exit 1
fi
review=$(api POST /v1/port-calls/call-001/documents/review "$token_checker" \
  --data "{\"document_id\":\"$document_id\",\"expected_version\":1,\"status\":\"VERIFIED\",\"reviewed_by\":\"integration-checker\",\"reason\":\"digest and metadata verified\"}")
printf '%s' "$review" | grep -q '"status":"VERIFIED"'
printf '%s' "$review" | grep -q '"version":2'
replacement_payload='{"document_type":"cargo_manifest","media_type":"application/pdf","size_bytes":4097,"sha256":"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","declared_by":"integration-agent"}'
replacement=$(api POST /v1/port-calls/call-001/documents "$token_agent" --data "$replacement_payload")
replacement_id=$(printf '%s' "$replacement" | python3 -c 'import json,sys; print(json.load(sys.stdin)["document_id"])')
replacement_review=$(api POST /v1/port-calls/call-001/documents/review "$token_checker" --data "{\"document_id\":\"$replacement_id\",\"expected_version\":1,\"status\":\"VERIFIED\",\"reviewed_by\":\"integration-checker\",\"reason\":\"replacement digest verified\"}")
printf '%s' "$replacement_review" | grep -q '"status":"VERIFIED"'
supersession=$(api POST /v1/port-calls/call-001/documents/supersede "$token_supervisor" --data "{\"original_document_id\":\"$document_id\",\"replacement_document_id\":\"$replacement_id\",\"reason\":\"corrected manifest\",\"superseded_by\":\"integration-supervisor\"}")
printf '%s' "$supersession" | grep -q '"status":"superseded"'
inactive_existing='{"profile_id":"npa-lagos","version":"2026-08-16","agency_code":"NPA","profile_sha256":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","registered_by":"integration-admin","active":false}'
api POST /v1/agency-profiles "$token_admin" --data "$inactive_existing" >/dev/null
if curl --silent --show-error -o /tmp/clearance-inactive-profile.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/port-calls/call-001/clearance -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-checker' -H "Authorization: Bearer $token_checker" --data '{"expected_version":3,"decision":"APPROVED","reason":"profile inactive enforcement","decided_by":"integration-checker"}' | grep -q '^422$'; then :; else
  echo 'clearance did not reject inactive agency profile' >&2
  exit 1
fi
api POST /v1/agency-profiles "$token_admin" --data "$profile" >/dev/null
clearance=$(api POST /v1/port-calls/call-001/clearance "$token_checker" \
  --data '{"expected_version":3,"decision":"APPROVED","reason":"all required declaration evidence verified","decided_by":"integration-checker"}')
printf '%s' "$clearance" | grep -q '"decision":"APPROVED"'
printf '%s' "$clearance" | grep -q '"call_version":4'
amendment=$(api POST /v1/port-calls/call-001/clearance/amend "$token_amender" --data '{"expected_version":4,"decision":"REJECTED","reason":"new authority instruction","amended_by":"integration-amender"}')
printf '%s' "$amendment" | grep -q '"decision":"REJECTED"'

# eCallUp booking smoke: terminal, slot, booking, reservation, cancellation.
api POST /v1/terminals "$token_admin" --data '{"terminal_id":"APAPA-T1","port_code":"LAGOS","name":"Apapa Terminal 1","booking_fee_kobo":250000}' | grep -q '"terminal_id":"APAPA-T1"'
slot_window_start=$(python3 -c 'import datetime; print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=1)).isoformat().replace("+00:00","Z"))')
slot_window_end=$(python3 -c 'import datetime; print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=3)).isoformat().replace("+00:00","Z"))')
slot=$(api POST /v1/slots "$token_admin" --data "{\"terminal_id\":\"APAPA-T1\",\"starts_at\":\"$slot_window_start\",\"ends_at\":\"$slot_window_end\",\"capacity\":1}")
slot_id=$(printf '%s' "$slot" | python3 -c 'import json,sys; print(json.load(sys.stdin)["slot_id"])')
booking_expiry=$(python3 -c 'import datetime; print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=4)).isoformat().replace("+00:00","Z"))')
booking_one=$(api POST /v1/bookings "$token_agent" --data "{\"request_id\":\"req-verify-0001\",\"truck_plate\":\"LAG-123-XY\",\"trucker_msisdn\":\"+2348012345678\",\"terminal_id\":\"APAPA-T1\",\"channel\":\"WEB\",\"amount_kobo\":250000,\"expires_at\":\"$booking_expiry\"}")
printf '%s' "$booking_one" | grep -q '"status":"DRAFTED"'
booking_one_id=$(printf '%s' "$booking_one" | python3 -c 'import json,sys; print(json.load(sys.stdin)["booking_id"])')
reserved=$(api POST "/v1/bookings/$booking_one_id/reserve" "$token_agent" --data "{\"slot_id\":\"$slot_id\",\"expected_version\":1}")
printf '%s' "$reserved" | grep -q '"status":"SLOT_RESERVED"'
# Second truck cannot overbook the capacity-1 slot.
booking_two=$(api POST /v1/bookings "$token_agent" --data "{\"request_id\":\"req-verify-0002\",\"truck_plate\":\"LAG-456-ZZ\",\"trucker_msisdn\":\"+2348098765432\",\"terminal_id\":\"APAPA-T1\",\"channel\":\"WEB\",\"amount_kobo\":250000,\"expires_at\":\"$booking_expiry\"}")
booking_two_id=$(printf '%s' "$booking_two" | python3 -c 'import json,sys; print(json.load(sys.stdin)["booking_id"])')
if curl --silent --show-error -o /tmp/booking-overbook.json -w '%{http_code}' -X POST "http://127.0.0.1:18080/v1/bookings/$booking_two_id/reserve" \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-agent' -H "Authorization: Bearer $token_agent" \
  --data "{\"slot_id\":\"$slot_id\",\"expected_version\":1}" | grep -q '^409$'; then :; else
  echo 'slot overbooking was not rejected' >&2
  exit 1
fi
# Offline booking is accepted into PENDING_SYNC and reconciliation against the
# full slot must surface RECONCILIATION_REQUIRED — never a silent drop.
offline_booking=$(api POST /v1/bookings "$token_agent" --data "{\"request_id\":\"req-verify-0003\",\"truck_plate\":\"LAG-789-QA\",\"trucker_msisdn\":\"+2348011122233\",\"terminal_id\":\"APAPA-T1\",\"channel\":\"OFFLINE\",\"amount_kobo\":250000,\"expires_at\":\"$booking_expiry\"}")
printf '%s' "$offline_booking" | grep -q '"status":"PENDING_SYNC"'
offline_booking_id=$(printf '%s' "$offline_booking" | python3 -c 'import json,sys; print(json.load(sys.stdin)["booking_id"])')
reconciled=$(api POST "/v1/bookings/$offline_booking_id/reconcile" "$token_agent" --data "{\"slot_id\":\"$slot_id\",\"expected_version\":1}")
printf '%s' "$reconciled" | grep -q '"status":"RECONCILIATION_REQUIRED"'
# Gate scan of an unpaid booking is denied.
if curl --silent --show-error -o /tmp/gate-unpaid.json -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/gate/scans \
  -H 'Content-Type: application/json' -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-gate' -H "Authorization: Bearer $token_agent" \
  --data "{\"booking_id\":\"$booking_one_id\",\"gate_id\":\"GATE-A\",\"scanned_by\":\"integration-gate\"}" | grep -q '^403$'; then :; else
  echo 'gate scan of unpaid booking was not denied' >&2
  exit 1
fi
cancelled=$(api POST "/v1/bookings/$booking_one_id/cancel" "$token_agent" --data '{"expected_version":2,"reason":"verification cleanup"}')
printf '%s' "$cancelled" | grep -q '"status":"CANCELLED"'

# Cross-tenant reads must be invisible: a token for another tenant sees nothing.
token_other=$(python3 - "$TENANT_GATEWAY_KEY" <<'PY'
import base64, hashlib, hmac, json, sys, time
key = sys.argv[1].encode()
b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b'=').decode()
head = b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(',', ':')).encode())
claims = {"iss": "gateway.local", "aud": "s1-port-interoperability",
          "tenant_id": "tenant-other", "sub": "integration-outsider",
          "exp": int(time.time()) + 3600}
payload = b64(json.dumps(claims, separators=(',', ':')).encode())
sig = b64(hmac.new(key, f"{head}.{payload}".encode(), hashlib.sha256).digest())
print(f"{head}.{payload}.{sig}")
PY
)
if curl --silent --show-error -o /tmp/port-call-cross-tenant.json -w '%{http_code}' -X GET http://127.0.0.1:18080/v1/port-calls/call-001 \
  -H 'X-Trusted-Proxy: loopback' -H 'X-Authenticated-Principal: integration-outsider' -H "Authorization: Bearer $token_other" | grep -q '^404$'; then :; else
  echo 'cross-tenant read was not hidden' >&2
  exit 1
fi

outbox_count=$("${docker_prefix[@]}" exec "$container_id" psql -p 55433 -U blueeconomy -d blueeconomy_port -Atc \
  "SET app.tenant_id = 'tenant-integration'; select count(*) from port_call_outbox where call_id = 'call-001';")
test "$outbox_count" = 10
platform_outbox_count=$("${docker_prefix[@]}" exec "$container_id" psql -p 55433 -U blueeconomy -d blueeconomy_port -Atc \
  "select count(*) from platform_outbox;")
test "$platform_outbox_count" -ge 6
printf '%s\n' 'S1 real PostgreSQL integration passed: tenant-wired port-call flow, replay/conflict rejection, document and clearance controls, cross-tenant invisibility, eCallUp booking state machine, slot capacity enforcement, offline reconciliation surfacing, gate denial and outbox atomicity.'
