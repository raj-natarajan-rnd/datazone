BEGIN;
SELECT set_config('acme.process_step', '1-Initial', true);

-- HU HOUSING: flip 3 audited columns → expect 3 audit rows
UPDATE acme.hu_inet_housing
SET broadbnd = CASE broadbnd WHEN 'Y' THEN 'N' ELSE 'Y' END,
    rnt      = LPAD((COALESCE(NULLIF(rnt,''),'000000')::int + 5)::text, 6, '0'),
    tax      = LPAD((COALESCE(NULLIF(tax,''),'000000')::int + 7)::text, 6, '0')
WHERE cmid = '000000001';

-- HU POPULATION: flip 4 audited columns → expect 4 audit rows
UPDATE acme.hu_inet_population
SET sex = CASE sex WHEN 'M' THEN 'F' ELSE 'M' END,
    mar = CASE mar WHEN 'M' THEN 'S' ELSE 'M' END,
    wrk = CASE wrk WHEN 'Y' THEN 'N' ELSE 'Y' END,
    sch = CASE sch WHEN 'C' THEN 'G' ELSE 'C' END
WHERE cmid = '000000001' AND pnum = '001';

-- HU ROSTER: change 3 audited columns → expect 3 audit rows
UPDATE acme.hu_inet_roster
SET rostal01   = rostal01 || '_T',
    rostam01   = CASE rostam01 WHEN 'Y' THEN 'N' ELSE 'Y' END,
    roststay01 = CASE roststay01 WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001';

COMMIT;

-- Inspect newest audit rows for this cmid
SELECT audit_log_id, response_table, cmid, pnum, var_name,
       original_value, modified_value, process_step, created_dt
FROM acme.audit_log
WHERE cmid = '000000001'
ORDER BY audit_log_id DESC
LIMIT 20;
