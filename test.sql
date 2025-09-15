-- ============================================================
-- PROMOTE acme._audit_debug TO FINAL AUDIT TABLE (robust setup)
-- ============================================================

-- 0) Make sure your session allows user triggers to fire
--    (if this shows 'replica', set it to origin for your session)
SHOW session_replication_role;

-- 1) Target tables: ensure meta columns exist (idempotent)
ALTER TABLE acme.hu_inet_housing
  ADD COLUMN IF NOT EXISTS created_dt  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by  text       NOT NULL DEFAULT SESSION_USER,
  ADD COLUMN IF NOT EXISTS modified_dt timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS modified_by text       NOT NULL DEFAULT SESSION_USER;

ALTER TABLE acme.hu_inet_population
  ADD COLUMN IF NOT EXISTS created_dt  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by  text       NOT NULL DEFAULT SESSION_USER,
  ADD COLUMN IF NOT EXISTS modified_dt timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS modified_by text       NOT NULL DEFAULT SESSION_USER;

ALTER TABLE acme.hu_inet_roster
  ADD COLUMN IF NOT EXISTS created_dt  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by  text       NOT NULL DEFAULT SESSION_USER,
  ADD COLUMN IF NOT EXISTS modified_dt timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS modified_by text       NOT NULL DEFAULT SESSION_USER;

-- 2) Final audit table: create/align schema
CREATE TABLE IF NOT EXISTS acme._audit_debug (
  ts              timestamptz DEFAULT now(),
  table_name      text,
  cmid            text,
  note            text
);

-- Add full audit columns (idempotent)
CREATE SEQUENCE IF NOT EXISTS acme._audit_debug_id_seq;

ALTER TABLE acme._audit_debug
  ADD COLUMN IF NOT EXISTS audit_log_id     bigint,
  ADD COLUMN IF NOT EXISTS pnum             varchar(9),
  ADD COLUMN IF NOT EXISTS response_table   varchar(50),
  ADD COLUMN IF NOT EXISTS action_code      varchar(10),
  ADD COLUMN IF NOT EXISTS var_name         varchar(32),  -- widened from 10 for safety
  ADD COLUMN IF NOT EXISTS original_value   varchar(200),
  ADD COLUMN IF NOT EXISTS modified_value   varchar(200),
  ADD COLUMN IF NOT EXISTS process_step     varchar(20),
  ADD COLUMN IF NOT EXISTS created_dt       timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by       text       NOT NULL DEFAULT SESSION_USER;

-- Backfill ids for any existing rows and enforce default + PK
UPDATE acme._audit_debug
SET audit_log_id = nextval('acme._audit_debug_id_seq')
WHERE audit_log_id IS NULL;

ALTER TABLE acme._audit_debug
  ALTER COLUMN audit_log_id SET DEFAULT nextval('acme._audit_debug_id_seq');

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'acme._audit_debug'::regclass AND contype = 'p'
  ) THEN
    ALTER TABLE acme._audit_debug
      ADD CONSTRAINT _audit_debug_pkey PRIMARY KEY (audit_log_id);
  END IF;
END$$;

ALTER SEQUENCE acme._audit_debug_id_seq OWNED BY acme._audit_debug.audit_log_id;

-- 3) Tiny error sink to capture any exceptions from the trigger
CREATE TABLE IF NOT EXISTS acme._audit_err (
  ts     timestamptz DEFAULT now(),
  where_ text,
  cmid   text,
  code   text,
  msg    text
);

-- 4) BEFORE UPDATE stamper (unchanged)
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

-- 5) AFTER UPDATE AUDIT TRIGGER — SECURITY DEFINER for reliable inserts
--    Also logs any exception to acme._audit_err
CREATE OR REPLACE FUNCTION acme.trg_audit_update()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = acme, pg_temp
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

  v_step := COALESCE(NULLIF(TG_ARGV[0], ''),
                     NULLIF(current_setting('acme.process_step', true), ''),
                     '3-Normalize');

  v_cmid := CASE WHEN to_jsonb(NEW) ? 'cmid' THEN to_jsonb(NEW)->>'cmid' ELSE NULL END;
  v_pnum := CASE WHEN to_jsonb(NEW) ? 'pnum' THEN to_jsonb(NEW)->>'pnum' ELSE NULL END;

  -- (Optional breadcrumb: mark we entered the trigger)
  INSERT INTO acme._audit_debug(table_name, cmid, note)
  VALUES (TG_TABLE_NAME, v_cmid, 'entered audit trigger');

  FOR r IN
    WITH oldj AS (SELECT to_jsonb(OLD) AS j),
         newj AS (SELECT to_jsonb(NEW) AS j)
    SELECT n.key, (o.j ->> n.key) AS oldval, (n.j ->> n.key) AS newval
    FROM jsonb_each((SELECT j FROM newj)) AS n(key, j)
    JOIN jsonb_each((SELECT j FROM oldj)) AS o(key, j) USING (key)
    WHERE (o.j ->> n.key) IS DISTINCT FROM (n.j ->> n.key)
      AND n.key NOT IN ('cmid','pnum','created_dt','created_by','modified_dt','modified_by')
  LOOP
    BEGIN
      INSERT INTO acme._audit_debug
        (cmid, pnum, response_table, action_code, var_name,
         original_value, modified_value, process_step,
         table_name, note)
      VALUES
        (v_cmid, v_pnum, TG_TABLE_NAME, 'UPDATE', r.key,
         r.oldval, r.newval, v_step,
         TG_TABLE_NAME, NULL);
    EXCEPTION WHEN OTHERS THEN
      INSERT INTO acme._audit_err(where_, cmid, code, msg)
      VALUES ('INSERT _audit_debug', v_cmid, SQLSTATE, SQLERRM);
    END;
  END LOOP;

  RETURN NEW;
END;
$$;

-- Reattach AFTER UPDATE triggers
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

-- 6) Deterministic test (forces real diffs)
SELECT set_config('acme.process_step', '1-Initial', true);

UPDATE acme.hu_inet_housing
SET broadbnd = CASE broadbnd WHEN 'Y' THEN 'N' ELSE 'Y' END,
    rnt      = LPAD((COALESCE(NULLIF(rnt,''),'000000')::int + 13)::text, 6, '0')
WHERE cmid = '000000001';

UPDATE acme.hu_inet_population
SET sex = CASE sex WHEN 'M' THEN 'F' ELSE 'M' END,
    wrk = CASE wrk WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001' AND pnum = '001';

UPDATE acme.hu_inet_roster
SET rostam01   = CASE rostam01 WHEN 'Y' THEN 'N' ELSE 'Y' END,
    roststay01 = CASE roststay01 WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001';

-- 7) Inspect results
SELECT audit_log_id, response_table, cmid, pnum, var_name,
       original_value, modified_value, process_step, created_dt, created_by
FROM acme._audit_debug
ORDER BY audit_log_id DESC
LIMIT 25;

SELECT * FROM acme._audit_err ORDER BY ts DESC LIMIT 10;
