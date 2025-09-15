-- Is RLS enabled/forced?
SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity
FROM pg_class c
WHERE c.oid = 'acme.audit_log'::regclass;

-- Any policies?
SELECT * FROM pg_policies
WHERE schemaname='acme' AND tablename='audit_log';

SHOW session_replication_role;  -- must be 'origin'
SET SESSION session_replication_role = origin;
