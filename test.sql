BEGIN;

-- Choose a known household & person from your sample data
-- (E.g., we inserted cmid '000000001' and pnum '001' earlier)
-- Set process step for this session
SELECT set_config('acme.process_step', '2-ARC', true);

-------------------------
-- 1) HU HOUSING update
-------------------------
-- Change 3 columns: broadbnd, rnt, tax  → expect 3 audit rows
UPDATE acme.hu_inet_housing
   SET broadbnd = CASE WHEN broadbnd = 'Y' THEN 'N' ELSE 'Y' END,
       rnt      = LPAD((CAST(SUBSTRING(rnt FROM 1 FOR 6) AS int) + 50)::text, 6, '0'),
       tax      = LPAD((CAST(SUBSTRING(tax FROM 1 FOR 6) AS int) + 75)::text, 6, '0')
 WHERE cmid = '000000001';

-- Verify BEFORE UPDATE stamped modified_* and AFTER UPDATE added rows
SELECT 'housing_row_after_update' AS check,
       cmid, broadbnd, rnt, tax, modified_dt, modified_by
  FROM acme.hu_inet_housing
 WHERE cmid = '000000001';

SELECT 'housing_audit_rows' AS check,
       audit_log_id, response_table, cmid, pnum, action_code, var_name,
       original_value, modified_value, process_step, created_dt, created_by
  FROM acme.audit_log
 WHERE cmid = '000000001'
   AND response_table = 'hu_inet_housing'
 ORDER BY audit_log_id DESC
 LIMIT 5;

----------------------------
-- 2) HU POPULATION update
----------------------------
-- Change 4 columns for one person: sex, mar, wrk, sch → expect 4 audit rows
UPDATE acme.hu_inet_population
   SET sex = CASE sex WHEN 'M' THEN 'F' ELSE 'M' END,
       mar = CASE mar WHEN 'M' THEN 'S' ELSE 'M' END,
       wrk = CASE wrk WHEN 'Y' THEN 'N' ELSE 'Y' END,
       sch = CASE sch WHEN 'C' THEN 'G' ELSE 'C' END
 WHERE cmid = '000000001' AND pnum = '001';

-- Verify row + audit (should include pnum)
SELECT 'population_row_after_update' AS check,
       cmid, pnum, sex, mar, wrk, sch, modified_dt, modified_by
  FROM acme.hu_inet_population
 WHERE cmid = '000000001' AND pnum = '001';

SELECT 'population_audit_rows' AS check,
       audit_log_id, response_table, cmid, pnum, var_name,
       original_value, modified_value, process_step
  FROM acme.audit_log
 WHERE cmid = '000000001'
   AND pnum = '001'
   AND response_table = 'hu_inet_population'
 ORDER BY audit_log_id DESC
 LIMIT 10;

------------------------
-- 3) HU ROSTER update
------------------------
-- Change 3 columns: rostal01, rostam01, roststay01 → expect 3 audit rows
UPDATE acme.hu_inet_roster
   SET rostal01  = rostal01 || '_X',
       rostam01  = CASE rostam01 WHEN 'Y' THEN 'N' ELSE 'Y' END,
       roststay01= CASE roststay01 WHEN 'Y' THEN 'N' ELSE 'Y' END
 WHERE cmid = '000000001';

-- Verify row + audit (no pnum for roster)
SELECT 'roster_row_after_update' AS check,
       cmid, rostal01, rostam01, roststay01, modified_dt, modified_by
  FROM acme.hu_inet_roster
 WHERE cmid = '000000001';

SELECT 'roster_audit_rows' AS check,
       audit_log_id, response_table, cmid, pnum, var_name,
       original_value, modified_value, process_step
  FROM acme.audit_log
 WHERE cmid = '000000001'
   AND response_table = 'hu_inet_roster'
 ORDER BY audit_log_id DESC
 LIMIT 10;

COMMIT;
