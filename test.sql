-- Make sure triggers fire in this session
SET SESSION session_replication_role = origin;

-- 0) Meta columns on target tables (safe to re-run)
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

-- 1) Final audit table (keep using _audit_debug) with ALL needed columns
CREATE SEQUENCE IF NOT EXISTS acme._audit_debug_id_seq;

CREATE TABLE IF NOT EXISTS acme._audit_debug (
  audit_log_id   bigint PRIMARY KEY DEFAULT nextval('acme._audit_debug_id_seq'),
  response_table varchar(50),
  action_code    varchar(10),
  process_step   varchar(20),
  cmid           text,
  pnum           text,
  var_name       varchar(63),     -- per-field record
  original_value varchar(200),
  modified_value varchar(200),
  changed_keys   text[],          -- full list of changed columns (for context)
  old_row        jsonb,           -- full OLD snapshot
  new_row        jsonb,           -- full NEW snapshot
  created_dt     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by     text       NOT NULL DEFAULT SESSION_USER,
  -- keep these helper columns if you like
  ts             timestamptz DEFAULT now(),
  table_name     text,
  note           text
);

-- Align default/ownership if table already existed
ALTER TABLE acme._audit_debug
  ALTER COLUMN audit_log_id SET DEFAULT nextval('acme._audit_debug_id_seq');
ALTER SEQUENCE acme._audit_debug_id_seq OWNED BY acme._audit_debug.audit_log_id;

-- Add any missing columns (idempotent)
ALTER TABLE acme._audit_debug
  ADD COLUMN IF NOT EXISTS response_table varchar(50),
  ADD COLUMN IF NOT EXISTS action_code    varchar(10),
  ADD COLUMN IF NOT EXISTS process_step   varchar(20),
  ADD COLUMN IF NOT EXISTS cmid           text,
  ADD COLUMN IF NOT EXISTS pnum           text,
  ADD COLUMN IF NOT EXISTS var_name       varchar(63),
  ADD COLUMN IF NOT EXISTS original_value varchar(200),
  ADD COLUMN IF NOT EXISTS modified_value varchar(200),
  ADD COLUMN IF NOT EXISTS changed_keys   text[],
  ADD COLUMN IF NOT EXISTS old_row        jsonb,
  ADD COLUMN IF NOT EXISTS new_row        jsonb,
  ADD COLUMN IF NOT EXISTS created_dt     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS created_by     text       NOT NULL DEFAULT SESSION_USER;

-- 2) BEFORE UPDATE stamper (unconditional so meta changes are included too)
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
FOR EACH ROW EXECUTE FUNCTION acme.trg_set_modified();

CREATE TRIGGER trg_set_modified_hu_inet_population
BEFORE UPDATE ON acme.hu_inet_population
FOR EACH ROW EXECUTE FUNCTION acme.trg_set_modified();

CREATE TRIGGER trg_set_modified_hu_inet_roster
BEFORE UPDATE ON acme.hu_inet_roster
FOR EACH ROW EXECUTE FUNCTION acme.trg_set_modified();

-- 3) AFTER UPDATE: per-field audit rows + full-row snapshots (NO exclusions)
CREATE OR REPLACE FUNCTION acme.trg_audit_row_detail()
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
  r         record;
BEGIN
  -- If the row bytes are identical, skip (rare)
  IF NEW IS NOT DISTINCT FROM OLD THEN
    RETURN NEW;
  END IF;

  v_step := COALESCE(NULLIF(TG_ARGV[0], ''),
                     NULLIF(current_setting('acme.process_step', true), ''),
                     '3-Normalize');

  -- Keys if present
  IF to_jsonb(NEW) ? 'cmid' THEN v_cmid := to_jsonb(NEW)->>'cmid'; END IF;
  IF to_jsonb(NEW) ? 'pnum' THEN v_pnum := to_jsonb(NEW)->>'pnum'; END IF;

  -- Compute changed columns (all of them)
  SELECT COALESCE(array_agg(n.key ORDER BY n.key), ARRAY[]::text[])
    INTO v_changed
  FROM jsonb_each(to_jsonb(NEW)) AS n(key, val_n)
  JOIN jsonb_each(to_jsonb(OLD)) AS o(key, val_o) USING (key)
  WHERE val_n IS DISTINCT FROM val_o;

  -- Insert ONE ROW PER CHANGED COLUMN, each including full OLD/NEW snapshots
  FOR r IN
    SELECT n.key AS col,
           o.j ->> n.key AS oldval,
           n.j ->> n.key AS newval
    FROM jsonb_each(to_jsonb(NEW)) AS n(key, j)
    JOIN jsonb_each(to_jsonb(OLD)) AS o(key, j) USING (key)
    WHERE (o.j ->> n.key) IS DISTINCT FROM (n.j ->> n.key)
  LOOP
    INSERT INTO acme._audit_debug
      (response_table, action_code, process_step,
       cmid, pnum, var_name, original_value, modified_value,
       changed_keys, old_row, new_row,
       table_name, note)
    VALUES
      (TG_TABLE_NAME, 'UPDATE', v_step,
       v_cmid, v_pnum, r.col, r.oldval, r.newval,
       v_changed, to_jsonb(OLD), to_jsonb(NEW),
       TG_TABLE_NAME, NULL);
  END LOOP;

  RETURN NEW;
END;
$$;

-- Attach AFTER UPDATE triggers (call the new function)
DROP TRIGGER IF EXISTS trg_audit_hu_inet_housing    ON acme.hu_inet_housing;
DROP TRIGGER IF EXISTS trg_audit_hu_inet_population ON acme.hu_inet_population;
DROP TRIGGER IF EXISTS trg_audit_hu_inet_roster     ON acme.hu_inet_roster;

CREATE TRIGGER trg_audit_hu_inet_housing
AFTER UPDATE ON acme.hu_inet_housing
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION acme.trg_audit_row_detail();

CREATE TRIGGER trg_audit_hu_inet_population
AFTER UPDATE ON acme.hu_inet_population
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION acme.trg_audit_row_detail();

CREATE TRIGGER trg_audit_hu_inet_roster
AFTER UPDATE ON acme.hu_inet_roster
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE FUNCTION acme.trg_audit_row_detail();
