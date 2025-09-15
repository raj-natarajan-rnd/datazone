SELECT c.relname AS table_name, t.tgname, t.tgenabled, p.proname AS function_name
FROM pg_trigger t
JOIN pg_class  c ON c.oid = t.tgrelid
JOIN pg_proc   p ON p.oid = t.tgfoid
WHERE c.oid IN ('acme.hu_inet_housing'::regclass,
                'acme.hu_inet_population'::regclass,
                'acme.hu_inet_roster'::regclass)
  AND NOT t.tgisinternal
ORDER BY table_name, tgname;
