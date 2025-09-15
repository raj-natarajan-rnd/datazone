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

  -- entered
  INSERT INTO acme._audit_debug(table_name, cmid, note)
  VALUES (TG_TABLE_NAME, COALESCE(NEW.cmid::text,'<NULL>'), 'audit trigger entered');

  v_step := COALESCE(NULLIF(TG_ARGV[0], ''),
                     NULLIF(current_setting('acme.process_step', true), ''),
                     '3-Normalize');

  v_cmid := CASE WHEN to_jsonb(NEW) ? 'cmid' THEN to_jsonb(NEW)->>'cmid' ELSE NULL END;
  v_pnum := CASE WHEN to_jsonb(NEW) ? 'pnum' THEN to_jsonb(NEW)->>'pnum' ELSE NULL END;

  FOR r IN
    WITH oldj AS (SELECT to_jsonb(OLD) AS j),
         newj AS (SELECT to_jsonb(NEW) AS j)
    SELECT n.key, (o.j ->> n.key) AS oldval, (n.j ->> n.key) AS newval
    FROM jsonb_each((SELECT j FROM newj)) AS n(key, j)
    JOIN jsonb_each((SELECT j FROM oldj)) AS o(key, j) USING (key)
    WHERE (o.j ->> n.key) IS DISTINCT FROM (n.j ->> n.key)
      AND n.key NOT IN ('cmid','pnum','created_dt','created_by','modified_dt','modified_by')
  LOOP
    -- proof of a diff detected
    INSERT INTO acme._audit_debug(table_name, cmid, note)
    VALUES (TG_TABLE_NAME, COALESCE(v_cmid,'<NULL>'),
            'diff:'||r.key||' '||COALESCE(r.oldval,'<NULL>')||'→'||COALESCE(r.newval,'<NULL>'));

    BEGIN
      INSERT INTO acme.audit_log
        (cmid, pnum, response_table, action_code, var_name,
         original_value, modified_value, process_step)
      VALUES
        (v_cmid, v_pnum, TG_TABLE_NAME, 'UPDATE', r.key,
         r.oldval, r.newval, v_step);
    EXCEPTION WHEN OTHERS THEN
      INSERT INTO acme._audit_debug(table_name, cmid, note)
      VALUES (TG_TABLE_NAME, COALESCE(v_cmid,'<NULL>'),
              'AUDIT INSERT FAILED: '||SQLSTATE||' '||SQLERRM);
    END;
  END LOOP;

  RETURN NEW;
END;
$$;



SELECT set_config('acme.process_step', '1-Initial', true);

UPDATE acme.hu_inet_housing
SET broadbnd = CASE broadbnd WHEN 'Y' THEN 'N' ELSE 'Y' END
WHERE cmid = '000000001';

SELECT * FROM acme._audit_debug ORDER BY ts DESC LIMIT 20;

SELECT audit_log_id, response_table, cmid, pnum, var_name,
       original_value, modified_value, process_step, created_dt
FROM acme.audit_log
ORDER BY audit_log_id DESC
LIMIT 20;
