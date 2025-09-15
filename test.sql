\d acme.audit_log  -- psql
-- or
SELECT table_schema, table_name, privilege_type
FROM information_schema.table_privileges
WHERE table_schema='acme' AND table_name='audit_log';

