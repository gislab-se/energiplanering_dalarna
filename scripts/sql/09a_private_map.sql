CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS private;
CREATE SCHEMA IF NOT EXISTS interim;

CREATE TABLE IF NOT EXISTS private.resp_id_map (
  resp_id TEXT PRIMARY KEY,
  pid UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO private.resp_id_map (resp_id)
SELECT DISTINCT n.respid::text
FROM novus.novus_full_dataframe n
WHERE n.respid IS NOT NULL
ON CONFLICT (resp_id) DO NOTHING;

REVOKE ALL ON SCHEMA private FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA private FROM PUBLIC;
