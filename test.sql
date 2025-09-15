-- =========================
-- 0) Sanity checks
-- =========================
-- Does the audit table exist?
SELECT 'audit_table_exists' AS check, to_regclass('acme.audit_log') AS regclass;

-- Show audit_log_id default & a row count
SELECT 'audit_table_def' AS check,
       column_name, column_default, is_nullable
FROM information_schema.columns
WHERE table_schema = 'acme' AND table_name = 'audit_log'
ORDER BY ordinal_position;

SELECT 'audit_rowcount_before' AS check, count(*) FROM acme.audit_log;

-- =========================
-- 1) Ensure meta columns exist on target tables
-- =========================
ALTER TABLE acme.hu_inet_housing
  ADD COLUMN IF NOT EXISTS created_dt  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by  text       NOT NULL DEFAULT SESSION_USER,
  ADD COLUMN IF NOT EXISTS modified_dt timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS modified_by text       NOT NULL DEFAULT SESSION_USER;

ALTER TABLE acme.hu_inet_population
  ADD COLUMN IF NOT EXISTS created_dt  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by  text       NOT NULL DEFAULT SESSION_USER,
  ADD COLUMN IF NOT EXISTS modified_dt timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS modified_by text       NOT NULL DEFAULT SESSION_USER;

ALTER TABLE acme.hu_inet_roster
  ADD COLUMN IF NOT EXISTS created_dt  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by  text       NOT NULL DEFAULT SESSION_USER,
  ADD COLUMN IF NOT EXISTS modified_dt timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS modified_by text       NOT NULL DEFAULT SESSION_USER;

-- =========================
-- 2) BEFORE UPDATE trigger: auto-stamp modified_*
-- =========================
CREATE OR REPLACE FUNCTION acme.trg_set_modified()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.modified_dt := now();
  NEW.modified_by := SESSION_USER;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_set_modified_hu_inet_housing    ON acme.hu_inet_housing;
DROP TRIGGER IF EXISTS trg_set_modified_hu_inet_population ON acme.hu_inet_population;
DROP TRIGGER IF EXISTS trg_set_modified_hu_inet_roster     ON acme.hu_inet_roster;

CREATE TRIGGER trg_set_modified_hu_inet_housing
BEFORE UPDATE ON acme.hu_inet_housing
FOR EACH ROW
EXECUTE FUNCTION acme.trg_set_modified();

CREATE TRIGGER trg_set_modified_hu_inet_population
BEFORE UPDATE ON acme.hu_inet_population
FOR EACH ROW
EXECUTE FUNCTION acme.trg_set_modified();

CREATE TRIGGER trg_set_modified_hu_inet_roster
BEFORE UPDATE ON acme.hu_inet_roster
FOR EACH ROW
EXECUTE FUNCTION acme.trg_set_modified();

-- =========================
-- 3) AFTER UPDATE audit trigger: with debug NOTICE
--    (remove the RAISE NOTICE once you confirm inserts)
-- =========================
CREATE OR REPLACE FUNCTION acme.trg_audit_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_step text;
  v_cmid text;
  v_pnum text;
  r      record;
BEGIN
  IF NEW IS NOT DISTINCT FROM OLD THEN
    RETURN NEW;
  END IF;

  -- pipeline step: trigger arg -> session GUC -> default
  BEGIN
    v_step := NULLIF(TG_ARGV[0], '');
  EXCEPTION WHEN OTHERS THEN
    v_step := NULL;
  END;
  IF v_step IS NULL THEN
    v_step := NULLIF(current_setting('acme.process_step', true), '');
  END IF;
  IF v_step IS NULL THEN
    v_step := '3-Normalize';
  END IF;

  -- keys
  v_cmid := NEW.cmid::text;
  v_pnum := CASE WHEN to_jsonb(NEW) ? 'pnum' THEN (to_jsonb(NEW)->>'pnum') ELSE NULL END;

  FOR r IN
    WITH oldj AS (SELECT to_jsonb(OLD) AS j),
         newj AS (SELECT to_jsonb(NEW) AS j)
    SELECT n.key,
           (o.j ->> n.key) AS oldval,
           (n.j ->> n.key) AS newval
    FROM jsonb_each((SELECT j FROM newj)) AS n(key, j)
    JOIN jsonb_each((SELECT j FROM oldj)) AS o(key, j) USING (key)
    WHERE (o.j ->> n.key) IS DISTINCT FROM (n.j ->> n.key)
      AND n.key NOT IN ('cmid','pnum','created_dt','created_by','modified_dt','modified_by')
  LOOP
    RAISE NOTICE 'AUDIT %: % -> % (table=%, cmid=%, pnum=%, step=%)',
                 r.key, r.oldval, r.newval, TG_TABLE_NAME, v_cmid, COALESCE(v_pnum,'∅'), v_step;

    INSERT INTO acme.audit_log
      (cmid, pnum, response_table, action_code, var_name,
       original_value, modified_value, process_step)
    VALUES
      (v_cmid, v_pnum, TG_TABLE_NAME, 'UPDATE', r.key,
       r.oldval, r.newval, v_step);
  END LOOP;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_audit_hu_inet_housing    ON acme.hu_inet_housing;
DROP TRIGGER IF EXISTS trg_audit_hu_inet_population ON acme.hu_inet_population;
DROP TRIGGER IF EXISTS trg_audit_hu_inet_roster     ON acme.hu_inet_roster;

CREATE TRIGGER trg_audit_hu_inet_housing
AFTER UPDATE ON acme.hu_inet_housing
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION acme.trg_audit_update();

CREATE TRIGGER trg_audit_hu_inet_population
AFTER UPDATE ON acme.hu_inet_population
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION acme.trg_audit_update();

CREATE TRIGGER trg_audit_hu_inet_roster
AFTER UPDATE ON acme.hu_inet_roster
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION acme.trg_audit_update();

-- =========================
-- 4) Prove it works: deterministic updates (must produce rows)
-- =========================
SELECT set_config('acme.process_step', '1-Initial', true);

-- Housing: flip 3 audited columns
UPDATE acme.hu_inet_housing
SET broadbnd = CASE broadbnd WHEN 'Y' THEN 'N' ELSE 'Y' END,
    rnt      = LPAD((COALESCE(NULLIF(rnt,''),'000000')::int + 5)::text, 6, '0'),
    tax      = LPAD((COALESCE(NULLIF(tax,''),'000000')::int + 7)::text, 6, '0')
WHERE cmid = '000000001';  -- exists per your seed data 

-- Population: flip 4 audited columns (row exists per seed)
UPDATE acme.hu_inet_population
SET sex = CASE sex WHEN 'M' THEN 'F' ELSE 'M' END,
    mar = CASE mar WHEN 'M' THEN 'S' ELSE 'M' END,
    wrk = CASE wrk WHEN 'Y' THEN 'N' ELSE 'Y' END,
    sch = CASE sch WHEN 'C' THEN 'G' ELSE 'C' END
WHERE cmid = '000000001' AND pnum = '001';  -- pk shape (cmid,pnum) 

-- Roster: tweak 3 audited columns
UPDATE acme.hu_inet_roster
SET rostal01   = rostal01 || '_T',
    rostam01   = CASE rostam01 WHEN 'Y' THEN 'N' ELSE 'Y' END,
    roststay01 = CASE roststay01 WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001';

-- =========================
-- 5) Inspect the results
-- =========================
SELECT 'audit_rowcount_after' AS check, count(*) FROM acme.audit_log;

SELECT audit_log_id, response_table, cmid, pnum, var_name,
       original_value, modified_value, process_step, created_dt
FROM acme.audit_log
WHERE cmid = '000000001'
ORDER BY audit_log_id DESC
LIMIT 20;
