-- Guaranteed-audit version: always logs something per UPDATE
CREATE OR REPLACE FUNCTION acme.trg_audit_update()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = acme, pg_temp
AS $$
DECLARE
  v_step   text;
  v_cmid   text;
  v_pnum   text;
  r        record;
  v_count  integer := 0;  -- how many per-column rows we wrote
BEGIN
  -- If nothing at all changed (rare in our setup), skip
  IF NEW IS NOT DISTINCT FROM OLD THEN
    RETURN NEW;
  END IF;

  -- Resolve pipeline step
  v_step := COALESCE(NULLIF(TG_ARGV[0], ''),
                     NULLIF(current_setting('acme.process_step', true), ''),
                     '3-Normalize');

  -- Keys (NULL-safe)
  v_cmid := CASE WHEN to_jsonb(NEW) ? 'cmid' THEN to_jsonb(NEW)->>'cmid' ELSE NULL END;
  v_pnum := CASE WHEN to_jsonb(NEW) ? 'pnum' THEN to_jsonb(NEW)->>'pnum' ELSE NULL END;

  -- Write one row per changed, non-meta column
  FOR r IN
    WITH oldj AS (SELECT to_jsonb(OLD) AS j),
         newj AS (SELECT to_jsonb(NEW) AS j)
    SELECT n.key, (o.j ->> n.key) AS oldval, (n.j ->> n.key) AS newval
    FROM jsonb_each((SELECT j FROM newj)) AS n(key, j)
    JOIN jsonb_each((SELECT j FROM oldj)) AS o(key, j) USING (key)
    WHERE (o.j ->> n.key) IS DISTINCT FROM (n.j ->> n.key)
      AND n.key NOT IN ('cmid','pnum','created_dt','created_by','modified_dt','modified_by')
  LOOP
    INSERT INTO acme._audit_debug
      (cmid, pnum, response_table, action_code, var_name,
       original_value, modified_value, process_step,
       table_name, note)
    VALUES
      (v_cmid, v_pnum, TG_TABLE_NAME, 'UPDATE', r.key,
       r.oldval, r.newval, v_step,
       TG_TABLE_NAME, NULL);
    v_count := v_count + 1;
  END LOOP;

  -- Fallback: if no non-meta diffs were found, still record the touch
  IF v_count = 0 THEN
    INSERT INTO acme._audit_debug
      (cmid, pnum, response_table, action_code, var_name,
       original_value, modified_value, process_step,
       table_name, note)
    VALUES
      (v_cmid, v_pnum, TG_TABLE_NAME, 'UPDATE', '_ROW_TOUCH',
       NULL, NULL, v_step,
       TG_TABLE_NAME, 'no non-meta changes (meta-only update)');
  END IF;

  RETURN NEW;
END;
$$;

-- Reattach triggers (safe no-ops if already attached)
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



-- Set a step (optional)
SELECT set_config('acme.process_step', '2-ARC', true);

-- A) Real changes (should produce per-column rows)
UPDATE acme.hu_inet_housing
SET broadbnd = CASE broadbnd WHEN 'Y' THEN 'N' ELSE 'Y' END,
    rnt      = LPAD((COALESCE(NULLIF(rnt,''),'000000')::int + 3)::text, 6, '0')
WHERE cmid = '000000001';

-- B) Meta-only update (no real data diffs we audit)
--     This causes BEFORE trigger to bump modified_* but no non-meta change,
--     so you should get one '_ROW_TOUCH' row
UPDATE acme.hu_inet_population
SET pnum = pnum
WHERE cmid = '000000001' AND pnum = '001';

-- Inspect the latest audit rows
SELECT audit_log_id, response_table, cmid, pnum, var_name,
       original_value, modified_value, process_step, created_dt, note
FROM acme._audit_debug
ORDER BY audit_log_id DESC
LIMIT 20;
