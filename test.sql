-- =========================================================
-- Promote acme._audit_debug as the final audit table
-- - Ensure it exists
-- - Add all columns from audit_log
-- - Add a PK with an auto-incrementing sequence
-- - Point the audit trigger to insert here
-- =========================================================

-- 0) Make sure the target tables have meta cols (idempotent)
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

-- 1) Create acme._audit_debug if it doesn't exist (initial minimal shape)
CREATE TABLE IF NOT EXISTS acme._audit_debug (
  ts          timestamptz DEFAULT now(),
  table_name  text,
  cmid        text,
  note        text
);

-- 2) Add/align all audit columns that existed in audit_log (idempotent)
--    Choose a dedicated sequence for the new PK on _audit_debug
CREATE SEQUENCE IF NOT EXISTS acme._audit_debug_id_seq;

ALTER TABLE acme._audit_debug
  ADD COLUMN IF NOT EXISTS audit_log_id     bigint,
  ADD COLUMN IF NOT EXISTS pnum             varchar(9),
  ADD COLUMN IF NOT EXISTS response_table   varchar(50),
  ADD COLUMN IF NOT EXISTS action_code      varchar(10),
  ADD COLUMN IF NOT EXISTS var_name         varchar(10),
  ADD COLUMN IF NOT EXISTS original_value   varchar(200),
  ADD COLUMN IF NOT EXISTS modified_value   varchar(200),
  ADD COLUMN IF NOT EXISTS process_step     varchar(20),
  ADD COLUMN IF NOT EXISTS created_dt       timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by       text       NOT NULL DEFAULT SESSION_USER;

-- 3) Backfill audit_log_id for any existing rows and set default
UPDATE acme._audit_debug
SET audit_log_id = nextval('acme._audit_debug_id_seq')
WHERE audit_log_id IS NULL;

ALTER TABLE acme._audit_debug
  ALTER COLUMN audit_log_id SET DEFAULT nextval('acme._audit_debug_id_seq');

-- 4) Add PK if not already present, and own the sequence
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'acme._audit_debug'::regclass
      AND contype  = 'p'
  ) THEN
    ALTER TABLE acme._audit_debug
      ADD CONSTRAINT _audit_debug_pkey PRIMARY KEY (audit_log_id);
  END IF;
END$$;

ALTER SEQUENCE acme._audit_debug_id_seq OWNED BY acme._audit_debug.audit_log_id;

-- 5) BEFORE UPDATE trigger to auto-stamp modified_* (recreate safely)
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

-- 6) AFTER UPDATE audit trigger now writes to acme._audit_debug
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
  -- no-op if row didn't change
  IF NEW IS NOT DISTINCT FROM OLD THEN
    RETURN NEW;
  END IF;

  -- Resolve process step: trigger arg -> session GUC -> default
  v_step := COALESCE(NULLIF(TG_ARGV[0], ''),
                     NULLIF(current_setting('acme.process_step', true), ''),
                     '3-Normalize');

  -- Safely extract keys (allow NULL)
  v_cmid := CASE WHEN to_jsonb(NEW) ? 'cmid' THEN to_jsonb(NEW)->>'cmid' ELSE NULL END;
  v_pnum := CASE WHEN to_jsonb(NEW) ? 'pnum' THEN to_jsonb(NEW)->>'pnum' ELSE NULL END;

  -- For every changed, non-meta column, write one audit row
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
       -- leave debugging columns null by default; table_name/note optional
       table_name, note)
    VALUES
      (v_cmid, v_pnum, TG_TABLE_NAME, 'UPDATE', r.key,
       r.oldval, r.newval, v_step,
       TG_TABLE_NAME, NULL);
  END LOOP;

  RETURN NEW;
END;
$$;

-- Attach AFTER UPDATE triggers
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

-- (Optional) If you had an earlier one-off probe trigger, drop it:
DROP TRIGGER IF EXISTS _trg_probe_housing ON acme.hu_inet_housing;
