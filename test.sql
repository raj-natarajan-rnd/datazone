-- 2.1: debug sink
CREATE TABLE IF NOT EXISTS acme._audit_debug (
  ts timestamptz DEFAULT now(),
  table_name text,
  cmid text,
  note text
);

-- 2.2: simplest possible trigger func that *must* run if triggers are enabled
CREATE OR REPLACE FUNCTION acme._trg_probe()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO acme._audit_debug(table_name, cmid, note)
  VALUES (TG_TABLE_NAME, NEW.cmid::text, 'probe fired');
  RETURN NEW;
END;
$$;

-- 2.3: attach ONLY to housing (no WHEN clause)
DROP TRIGGER IF EXISTS _trg_probe_housing ON acme.hu_inet_housing;
CREATE TRIGGER _trg_probe_housing
BEFORE UPDATE ON acme.hu_inet_housing
FOR EACH ROW
EXECUTE FUNCTION acme._trg_probe();

-- 2.4: force an update that changes *something*
UPDATE acme.hu_inet_housing
SET broadbnd = CASE broadbnd WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001';

-- 2.5: did the probe fire?
SELECT * FROM acme._audit_debug ORDER BY ts DESC LIMIT 5;
