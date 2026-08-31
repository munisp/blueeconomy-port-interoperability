-- Phase 11 performance audit: missing indexes justified by actual query code.
-- Idempotent; does not duplicate any index declared in migrations 0001-0020.

-- declarations.Store.List (GET /v1/customs/declarations):
--   WHERE ($1='' OR trader_id=$1) AND ($2='' OR status=$2)
--   ORDER BY created_at DESC, declaration_id LIMIT/OFFSET
-- Existing customs_declarations_status_idx / _trader_idx cover the filters but
-- not the tenant-wide newest-first ordering; this serves the unfiltered and
-- filtered list paths' sort.
CREATE INDEX IF NOT EXISTS customs_declarations_tenant_created_idx
    ON customs_declarations (tenant_id, created_at DESC, declaration_id);

-- queue.Store.ListActiveCallups (GET active call-ups):
--   WHERE tenant_id=$1 AND status IN ('CALLED_UP','EN_ROUTE')
--   ORDER BY terminal_id, grace_deadline
-- Existing truck_queue_grace_idx leads with grace_deadline (no tenant), and
-- truck_queue_terminal_idx leads with terminal_id; neither serves a
-- tenant-first probe.
CREATE INDEX IF NOT EXISTS truck_queue_tenant_status_idx
    ON truck_queue_requests (tenant_id, status)
    WHERE status IN ('CALLED_UP', 'EN_ROUTE');

-- manifests.Store.ListRejections without manifest_id (tenant rejection queue):
--   ORDER BY rejection_seq LIMIT n
-- Existing passenger_manifest_rejections_manifest_idx leads with manifest_id.
CREATE INDEX IF NOT EXISTS passenger_manifest_rejections_tenant_seq_idx
    ON passenger_manifest_rejections (tenant_id, rejection_seq);
