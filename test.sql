-- A.1 Ensure audit/meta columns exist on all three tables
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

-- A.2 BEFORE UPDATE: auto-stamp modified_*
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

-- Recreate BEFORE UPDATE triggers
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

-- A.3 AFTER UPDATE: audit changed columns into acme.audit_log
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

  -- process step from trigger arg → session GUC → default
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

  v_cmid := NEW.cmid::text;
  v_pnum := CASE WHEN to_jsonb(NEW) ? 'pnum' THEN (to_jsonb(NEW)->>'pnum') ELSE NULL END;

  FOR r IN
    WITH oldj AS (SELECT to_jsonb(OLD) AS j),
         newj AS (SELECT to_jsonb(NEW) AS j),
         pairs AS (
           SELECT n.key,
                  o.j ->> n.key AS oldval,
                  n.j ->> n.key AS newval
           FROM jsonb_each((SELECT j FROM newj)) AS n(key, j)
           JOIN jsonb_each((SELECT j FROM oldj)) AS o(key, j) USING (key)
         )
    SELECT key, oldval, newval
    FROM pairs
    WHERE (oldval IS DISTINCT FROM newval)
      AND key NOT IN ('cmid','pnum','created_dt','created_by','modified_dt','modified_by')
  LOOP
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

-- Recreate AFTER UPDATE triggers
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
