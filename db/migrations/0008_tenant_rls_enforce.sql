-- ENFORCE ONLY after approved backfill reconciliation. Fails closed on any unassigned core record.
DO $$
DECLARE missing_count bigint;
BEGIN
  SELECT count(*) INTO missing_count FROM port_calls WHERE tenant_id IS NULL;
  IF missing_count <> 0 THEN RAISE EXCEPTION 'cannot enable tenant RLS: % unassigned port_calls', missing_count; END IF;
  SELECT count(*) INTO missing_count FROM port_call_documents WHERE tenant_id IS NULL;
  IF missing_count <> 0 THEN RAISE EXCEPTION 'cannot enable tenant RLS: % unassigned documents', missing_count; END IF;
  SELECT count(*) INTO missing_count FROM port_call_clearance_decisions WHERE tenant_id IS NULL;
  IF missing_count <> 0 THEN RAISE EXCEPTION 'cannot enable tenant RLS: % unassigned clearance decisions', missing_count; END IF;
  SELECT count(*) INTO missing_count FROM port_call_outbox WHERE tenant_id IS NULL;
  IF missing_count <> 0 THEN RAISE EXCEPTION 'cannot enable tenant RLS: % unassigned outbox rows', missing_count; END IF;
END $$;
ALTER TABLE port_calls ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE port_call_documents ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE port_call_clearance_decisions ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE port_call_outbox ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE port_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE port_calls FORCE ROW LEVEL SECURITY;
CREATE POLICY port_calls_tenant_policy ON port_calls USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE port_call_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE port_call_documents FORCE ROW LEVEL SECURITY;
CREATE POLICY port_call_documents_tenant_policy ON port_call_documents USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE port_call_clearance_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE port_call_clearance_decisions FORCE ROW LEVEL SECURITY;
CREATE POLICY port_call_clearance_tenant_policy ON port_call_clearance_decisions USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE port_call_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE port_call_outbox FORCE ROW LEVEL SECURITY;
CREATE POLICY port_call_outbox_tenant_policy ON port_call_outbox USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
