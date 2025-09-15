-- make sure we’re in the intended step (any of the 3 is fine)
SELECT set_config('acme.process_step', '1-Initial', true);

-- flip values to guarantee diffs
UPDATE acme.hu_inet_housing
SET broadbnd = CASE broadbnd WHEN 'Y' THEN 'N' ELSE 'Y' END,
    rnt      = LPAD((COALESCE(NULLIF(rnt,''),'000000')::int + 11)::text, 6, '0')
WHERE cmid = '000000001';

UPDATE acme.hu_inet_population
SET sex = CASE sex WHEN 'M' THEN 'F' ELSE 'M' END,
    wrk = CASE wrk WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001' AND pnum = '001';

UPDATE acme.hu_inet_roster
SET rostam01   = CASE rostam01 WHEN 'Y' THEN 'N' ELSE 'Y' END,
    roststay01 = CASE roststay01 WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001';

-- 4.1: Did the audit trigger run at all?
SELECT * FROM acme._audit_debug ORDER BY ts DESC LIMIT 20;

-- 4.2: What landed in the audit table?
SELECT audit_log_id, response_table, cmid, pnum, var_name,
       original_value, modified_value, process_step, created_dt
FROM acme.audit_log
WHERE cmid = '000000001'
ORDER BY audit_log_id DESC
LIMIT 50;
