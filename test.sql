-- 3.1: BEFORE UPDATE stamper (ok to rerun)
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

-- Attach/re-attach BEFORE triggers
DO $$
BEGIN
  IF to_regclass('acme.hu_inet_housing') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS trg_set_modified_hu_inet_housing ON acme.hu_inet_housing;
    CREATE TRIGGER trg_set_modified_hu_inet_housing
    BEFORE UPDATE ON acme.hu_inet_housing FOR EACH ROW
    EXECUTE FUNCTION acme.trg_set_modified();
  END IF;

  IF to_regclass('acme.hu_inet_population') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS trg_set_modified_hu_inet_population ON acme.hu_inet_population;
    CREATE TRIGGER trg_set_modified_hu_inet_population
    BEFORE UPDATE ON acme.hu_inet_population FOR EACH ROW
    EXECUTE FUNCTION acme.trg_set_modified();
  END IF;

  IF to_regclass('acme.hu_inet_roster') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS trg_set_modified_hu_inet_roster ON acme.hu_inet_roster;
    CREATE TRIGGER trg_set_modified_hu_inet_roster
    BEFORE UPDATE ON acme.hu_inet_roster FOR EACH ROW
    EXECUTE FUNCTION acme.trg_set_modified();
  END IF;
END$$;

-- 3.2: AFTER UPDATE audit trigger with debug sink insert
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

  -- Ensure we log that we *entered* the trigger
  INSERT INTO acme._audit_debug(table_name, cmid, note)
  VALUES (TG_TABLE_NAME, COALESCE(NEW.cmid::text, '<no cmid>'), 'audit trigger entered');

  -- Determine process step
  v_step := COALESCE(NULLIF(TG_ARGV[0], ''),
                     NULLIF(current_setting('acme.process_step', true), ''),
                     '3-Normalize');

  v_cmid := NEW.cmid::text;
  v_pnum := CASE WHEN to_jsonb(NEW) ? 'pnum' THEN (to_jsonb(NEW)->>'pnum') ELSE NULL END;

  FOR r IN
    WITH oldj AS (SELECT to_jsonb(OLD) AS j),
         newj AS (SELECT to_jsonb(NEW) AS j)
    SELECT n.key, (o.j ->> n.key) AS oldval, (n.j ->> n.key) AS newval
    FROM jsonb_each((SELECT j FROM newj)) AS n(key, j)
    JOIN jsonb_each((SELECT j FROM oldj)) AS o(key, j) USING (key)
    WHERE (o.j ->> n.key) IS DISTINCT FROM (n.j ->> n.key)
      AND n.key NOT IN ('cmid','pnum','created_dt','created_by','modified_dt','modified_by')
  LOOP
    -- also log each changed column to the debug sink
    INSERT INTO acme._audit_debug(table_name, cmid, note)
    VALUES (TG_TABLE_NAME, v_cmid, 'diff:'||r.key||' '||COALESCE(r.oldval,'<NULL>')||'→'||COALESCE(r.newval,'<NULL>'));

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

-- Attach/re-attach AFTER triggers
DO $$
BEGIN
  IF to_regclass('acme.hu_inet_housing') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS trg_audit_hu_inet_housing ON acme.hu_inet_housing;
    CREATE TRIGGER trg_audit_hu_inet_housing
    AFTER UPDATE ON acme.hu_inet_housing
    FOR EACH ROW
    WHEN (OLD IS DISTINCT FROM NEW)
    EXECUTE FUNCTION acme.trg_audit_update();
  END IF;

  IF to_regclass('acme.hu_inet_population') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS trg_audit_hu_inet_population ON acme.hu_inet_population;
    CREATE TRIGGER trg_audit_hu_inet_population
    AFTER UPDATE ON acme.hu_inet_population
    FOR EACH ROW
    WHEN (OLD IS DISTINCT FROM NEW)
    EXECUTE FUNCTION acme.trg_audit_update();
  END IF;

  IF to_regclass('acme.hu_inet_roster') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS trg_audit_hu_inet_roster ON acme.hu_inet_roster;
    CREATE TRIGGER trg_audit_hu_inet_roster
    AFTER UPDATE ON acme.hu_inet_roster
    FOR EACH ROW
    WHEN (OLD IS DISTINCT FROM NEW)
    EXECUTE FUNCTION acme.trg_audit_update();
  END IF;
END$$;
