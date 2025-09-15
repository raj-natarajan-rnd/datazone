-- Make sure user triggers fire
SET SESSION session_replication_role = origin;

-- 0) Ensure meta columns exist on target tables (idempotent)
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

-- 1) Final audit table: add required columns (idempotent)
CREATE SEQUENCE IF NOT EXISTS acme._audit_debug_id_seq;

CREATE TABLE IF NOT EXISTS acme._audit_debug (
  audit_log_id   bigint PRIMARY KEY DEFAULT nextval('acme._audit_debug_id_seq'),
  response_table varchar(50),
  action_code    varchar(10),
  process_step   varchar(20),
  cmid           text,
  pnum           text,
  changed_keys   text[],
  old_row        jsonb,
  new_row        jsonb,
  created_dt     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by     text       NOT NULL DEFAULT SESSION_USER,
  -- keep the debug helper columns if you already had them:
  ts             timestamptz DEFAULT now(),
  table_name     text,
  note           text
);

-- Align existing table if it already existed
ALTER TABLE acme._audit_debug
  ALTER COLUMN audit_log_id SET DEFAULT nextval('acme._audit_debug_id_seq');

ALTER SEQUENCE acme._audit_debug_id_seq OWNED BY acme._audit_debug.audit_log_id;

-- Add any missing columns (safe if already present)
ALTER TABLE acme._audit_debug
  ADD COLUMN IF NOT EXISTS response_table varchar(50),
  ADD COLUMN IF NOT EXISTS action_code    varchar(10),
  ADD COLUMN IF NOT EXISTS process_step   varchar(20),
  ADD COLUMN IF NOT EXISTS cmid           text,
  ADD COLUMN IF NOT EXISTS pnum           text,
  ADD COLUMN IF NOT EXISTS changed_keys   text[],
  ADD COLUMN IF NOT EXISTS old_row        jsonb,
  ADD COLUMN IF NOT EXISTS new_row        jsonb,
  ADD COLUMN IF NOT EXISTS created_dt     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by     text       NOT NULL DEFAULT SESSION_USER;

-- 2) BEFORE UPDATE stamper (unconditional so meta diffs also get logged)
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

-- 3) AFTER UPDATE: ROW SNAPSHOT (logs ANY changed column)
CREATE OR REPLACE FUNCTION acme.trg_audit_row_snapshot()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = acme, pg_temp
AS $$
DECLARE
  v_step    text;
  v_cmid    text;
  v_pnum    text;
  v_changed text[];
BEGIN
  -- If row bytes are identical, skip (rare)
  IF NEW IS NOT DISTINCT FROM OLD THEN
    RETURN NEW;
  END IF;

  v_step := COALESCE(NULLIF(TG_ARGV[0], ''),
                     NULLIF(current_setting('acme.process_step', true), ''),
                     '3-Normalize');

  -- Extract keys when present
  IF to_jsonb(NEW) ? 'cmid' THEN v_cmid := to_jsonb(NEW)->>'cmid'; END IF;
  IF to_jsonb(NEW) ? 'pnum' THEN v_pnum := to_jsonb(NEW)->>'pnum'; END IF;

  -- Compute changed column names (no exclusions)
  SELECT COALESCE(array_agg(n.key ORDER BY n.key), ARRAY[]::text[])
    INTO v_changed
  FROM jsonb_each(to_jsonb(NEW)) AS n(key, val_n)
  JOIN jsonb_each(to_jsonb(OLD)) AS o(key, val_o) USING (key)
  WHERE val_n IS DISTINCT FROM val_o;

  -- Insert one snapshot row per UPDATE
  INSERT INTO acme._audit_debug
    (response_table, action_code, process_step,
     cmid, pnum, changed_keys, old_row, new_row,
     table_name, note)
  VALUES
    (TG_TABLE_NAME, 'UPDATE', v_step,
     v_cmid, v_pnum, v_changed, to_jsonb(OLD), to_jsonb(NEW),
     TG_TABLE_NAME, NULL);

  RETURN NEW;
END;
$$;

-- Attach AFTER UPDATE triggers (replace any older ones)
DROP TRIGGER IF EXISTS trg_audit_hu_inet_housing    ON acme.hu_inet_housing;
DROP TRIGGER IF EXISTS trg_audit_hu_inet_population ON acme.hu_inet_population;
DROP TRIGGER IF EXISTS trg_audit_hu_inet_roster     ON acme.hu_inet_roster;

CREATE TRIGGER trg_audit_hu_inet_housing
AFTER UPDATE ON acme.hu_inet_housing
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION acme.trg_audit_row_snapshot();

CREATE TRIGGER trg_audit_hu_inet_population
AFTER UPDATE ON acme.hu_inet_population
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION acme.trg_audit_row_snapshot();

CREATE TRIGGER trg_audit_hu_inet_roster
AFTER UPDATE ON acme.hu_inet_roster
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION acme.trg_audit_row_snapshot();

-- 4) Deterministic smoke test
SELECT set_config('acme.process_step', '1-Initial', true);

UPDATE acme.hu_inet_housing
SET broadbnd = CASE broadbnd WHEN 'Y' THEN 'N' ELSE 'Y' END,
    rnt      = LPAD((COALESCE(NULLIF(rnt,''),'000000')::int + 9)::text, 6, '0')
WHERE cmid = '000000001';

UPDATE acme.hu_inet_population
SET wrk = CASE wrk WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001' AND pnum = '001';

UPDATE acme.hu_inet_roster
SET rostam01 = CASE rostam01 WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001';

-- Inspect the last few audit rows
SELECT audit_log_id, response_table, action_code, process_step,
       cmid, pnum, changed_keys, created_dt, created_by
FROM acme._audit_debug
ORDER BY audit_log_id DESC
LIMIT 10;

-- See full before/after JSON for the most recent row
SELECT response_table, cmid, pnum, changed_keys, old_row, new_row, created_dt
FROM acme._audit_debug
ORDER BY audit_log_id DESC
LIMIT 1;
