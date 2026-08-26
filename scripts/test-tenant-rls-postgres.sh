#!/usr/bin/env bash
set -euo pipefail

# This test never contacts a managed database. It creates and destroys a local
# PostgreSQL cluster, applies the repository migrations, and exercises RLS as a
# non-superuser runtime role.
for command in sudo psql pg_config; do
  command -v "$command" >/dev/null || { echo "missing required command: $command" >&2; exit 2; }
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pg_bin="$(pg_config --bindir)"
tmp_dir="$(mktemp -d /tmp/port-tenant-rls.XXXXXX)"
data_dir="$tmp_dir/data"
socket_dir="$tmp_dir/socket"
port="$((55432 + RANDOM % 1000))"
db="port_tenant_isolation"

cleanup() {
  if [[ -f "$data_dir/postmaster.pid" ]]; then
    sudo -u postgres "$pg_bin/pg_ctl" -D "$data_dir" -m immediate stop >/dev/null 2>&1 || true
  fi
  sudo rm -rf "$tmp_dir"
}
trap cleanup EXIT

mkdir -p "$data_dir" "$socket_dir"
sudo chown -R postgres:postgres "$tmp_dir"
sudo -u postgres "$pg_bin/initdb" --no-locale --encoding=UTF8 -D "$data_dir" --auth=trust >/dev/null
sudo -u postgres "$pg_bin/pg_ctl" -D "$data_dir" -o "-k $socket_dir -p $port -c listen_addresses=''" -w start >/dev/null

psql_admin() {
  sudo -u postgres psql -X -v ON_ERROR_STOP=1 -h "$socket_dir" -p "$port" -U postgres -d "$db" "$@"
}

sudo -u postgres createdb -h "$socket_dir" -p "$port" -U postgres "$db"
for migration in "$repo_root"/db/migrations/*.sql; do
  psql_admin < "$migration" >/dev/null
done

psql_admin <<'SQL' >/dev/null
CREATE ROLE port_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
GRANT USAGE ON SCHEMA public TO port_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON port_calls, port_call_documents, port_call_clearance_decisions, port_call_outbox TO port_runtime;
INSERT INTO platform_tenants (tenant_id, authority_reference) VALUES
  ('tenant-alpha', 'authority-alpha'),
  ('tenant-beta', 'authority-beta');
INSERT INTO port_agency_profile_versions (profile_id, version, agency_code, active, profile_sha256, registered_by, registered_at, tenant_id)
VALUES ('profile-alpha', 'v1', 'ALPHA', true, 'sha256:' || repeat('a', 64), 'integration-test', now(), 'tenant-alpha');
SQL

runtime_sql() {
  psql_admin -At -c "SET ROLE port_runtime; $1"
}

tenant_scalar() {
  local tenant="$1" query="$2"
  runtime_sql "BEGIN; SELECT set_config('app.tenant_id', '$tenant', true); $query; COMMIT;" | grep -E '^[0-9]+$|^[tf]$' | tail -1
}

insert_call() {
  local tenant="$1" call_id="$2" idempotency="$3"
  runtime_sql "BEGIN; SELECT set_config('app.tenant_id', '$tenant', true); INSERT INTO port_calls (call_id, vessel_imo, port_code, declaration_reference, submitted_by, status, idempotency_key, created_at, updated_at, version, agency_profile_id, agency_profile_version, tenant_id) VALUES ('$call_id', '1234567', 'ABCD', 'decl-$call_id', 'integration-test', 'DRAFT', '$idempotency', now(), now(), 1, 'profile-alpha', 'v1', '$tenant'); COMMIT;" >/dev/null
}

insert_call tenant-alpha call-alpha idem-alpha
insert_call tenant-beta call-beta idem-beta

alpha_visible="$(tenant_scalar tenant-alpha "SELECT count(*) FROM port_calls WHERE call_id = 'call-alpha'")"
beta_visible_to_alpha="$(tenant_scalar tenant-alpha "SELECT count(*) FROM port_calls WHERE call_id = 'call-beta'")"
alpha_visible_to_beta="$(tenant_scalar tenant-beta "SELECT count(*) FROM port_calls WHERE call_id = 'call-alpha'")"
anonymous_visible="$(runtime_sql "SELECT count(*) FROM port_calls;" | grep -E '^[0-9]+$' | tail -1)"

[[ "$alpha_visible" == "1" ]] || { echo "tenant-alpha cannot read its own port call" >&2; exit 1; }
[[ "$beta_visible_to_alpha" == "0" ]] || { echo "tenant-alpha can read tenant-beta data" >&2; exit 1; }
[[ "$alpha_visible_to_beta" == "0" ]] || { echo "tenant-beta can read tenant-alpha data" >&2; exit 1; }
[[ "$anonymous_visible" == "0" ]] || { echo "unscoped runtime role can read tenant data" >&2; exit 1; }

if runtime_sql "BEGIN; SELECT set_config('app.tenant_id', 'tenant-alpha', true); INSERT INTO port_calls (call_id, vessel_imo, port_code, declaration_reference, submitted_by, status, idempotency_key, created_at, updated_at, version, agency_profile_id, agency_profile_version, tenant_id) VALUES ('cross-tenant-write', '1234567', 'ABCD', 'decl-cross', 'integration-test', 'DRAFT', 'idem-cross', now(), now(), 1, 'profile-alpha', 'v1', 'tenant-beta'); COMMIT;" >/dev/null 2>&1; then
  echo "tenant-alpha can write a tenant-beta row" >&2
  exit 1
fi

cross_update="$(tenant_scalar tenant-beta "WITH changed AS (UPDATE port_calls SET declaration_reference = 'changed' WHERE call_id = 'call-alpha' RETURNING 1) SELECT count(*) FROM changed")"
cross_delete="$(tenant_scalar tenant-beta "WITH deleted AS (DELETE FROM port_calls WHERE call_id = 'call-alpha' RETURNING 1) SELECT count(*) FROM deleted")"
[[ "$cross_update" == "0" ]] || { echo "tenant-beta updated tenant-alpha data: $cross_update" >&2; exit 1; }
[[ "$cross_delete" == "0" ]] || { echo "tenant-beta deleted tenant-alpha data: $cross_delete" >&2; exit 1; }

context_cleared="$(runtime_sql "BEGIN; SELECT set_config('app.tenant_id', 'tenant-alpha', true); COMMIT; SELECT NULLIF(current_setting('app.tenant_id', true), '') IS NULL;" | grep -E '^[tf]$' | tail -1)"
[[ "$context_cleared" == "t" ]] || { echo "transaction-local tenant context leaked after commit" >&2; exit 1; }

cat <<RESULT
POSTGRES_TENANT_RLS_INTEGRATION_PASS
port=$port
migrations=0001-0008
own_read=$alpha_visible
cross_reads=0
unscoped_reads=0
cross_insert=denied
cross_update_rows=$cross_update
cross_delete_rows=$cross_delete
tenant_context_cleared=$context_cleared
RESULT
