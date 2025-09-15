-- Create sequence if not present
CREATE SEQUENCE IF NOT EXISTS acme.audit_log_id_seq;

-- Ensure audit_log_id pulls from the sequence
ALTER TABLE acme.audit_log
    ALTER COLUMN audit_log_id
    SET DEFAULT nextval('acme.audit_log_id_seq');

-- Re-own sequence so it drops with table if dropped
ALTER SEQUENCE acme.audit_log_id_seq OWNED BY acme.audit_log.audit_log_id;
