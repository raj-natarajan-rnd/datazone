-- Make sure triggers are allowed to fire
SHOW session_replication_role;           -- must be 'origin'
-- if not origin, run:
SET SESSION session_replication_role = origin;

-- Make sure NOTICEs are visible (optional)
SHOW client_min_messages;                -- set to 'notice' or lower
SET client_min_messages = notice;

-- Confirm triggers exist & are enabled ('O')
SELECT c.relname AS table_name, t.tgname, t.tgenabled, p.proname AS function_name
FROM pg_trigger t
JOIN pg_class  c ON c.oid = t.tgrelid
JOIN pg_proc   p ON p.oid = t.tgfoid
WHERE c.oid IN ('acme.hu_inet_housing'::regclass,
                'acme.hu_inet_population'::regclass,
                'acme.hu_inet_roster'::regclass)
  AND NOT t.tgisinternal
ORDER BY table_name, tgname;
