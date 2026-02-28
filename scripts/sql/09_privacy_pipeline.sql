-- Privacy-first point pipeline
-- Source: novus.novus_full_dataframe
-- Target: private + interim schemas
-- Important: respid must not appear in downstream interim artifacts.

BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS private;
CREATE SCHEMA IF NOT EXISTS interim;

-- Part A: stable sensitive->anonymous mapping
CREATE TABLE IF NOT EXISTS private.resp_id_map (
  resp_id TEXT PRIMARY KEY,
  pid UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Keep mapping stable: insert only unknown respids.
INSERT INTO private.resp_id_map (resp_id)
SELECT DISTINCT n.respid::text AS resp_id
FROM novus.novus_full_dataframe n
WHERE n.respid IS NOT NULL
ON CONFLICT (resp_id) DO NOTHING;

-- Restrict access to sensitive mapping.
REVOKE ALL ON SCHEMA private FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA private FROM PUBLIC;

-- Part B: canonical long-format point table (no resp_id)
CREATE TABLE IF NOT EXISTS interim.respondent_points (
  point_id BIGSERIAL PRIMARY KEY,
  pid UUID NOT NULL,
  point_type TEXT NOT NULL CHECK (point_type IN ('plats_1', 'plats_2', 'plats_sensitive')),
  sensitive BOOLEAN NOT NULL,
  geom geometry(Point, 3006) NOT NULL,
  fritext TEXT,
  home_municipality TEXT,
  kommungrupp INTEGER,
  source_record BIGINT,
  source_plats_nr INTEGER,
  source_kommunkod TEXT,
  source_admin_2 TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

TRUNCATE TABLE interim.respondent_points;

WITH base AS (
  SELECT
    m.pid,
    n.record::bigint AS source_record,
    NULLIF(trim(n.q1::text), '') AS home_municipality,
    CASE
      WHEN NULLIF(trim(n.kommungrupp::text), '') IS NULL THEN NULL
      ELSE NULLIF(regexp_replace(trim(n.kommungrupp::text), '[^0-9+-]', '', 'g'), '')::int
    END AS kommungrupp,
    n.plats1_kommunkod::text AS plats1_kommunkod,
    n.plats2_kommunkod::text AS plats2_kommunkod,
    n.plats1_admin_2::text AS plats1_admin_2,
    n.plats2_admin_2::text AS plats2_admin_2,
    n.q7_1_txt::text AS q7_1_txt,
    n.q9_1_txt::text AS q9_1_txt,
    n.plats1_data_10::double precision AS p1_lat,
    n.plats1_data_11::double precision AS p1_lon,
    n.plats2_data_10::double precision AS p2_lat,
    n.plats2_data_11::double precision AS p2_lon,
    NULLIF(trim(n.q7_1::text), '') AS q7_1,
    NULLIF(trim(n.q9_1::text), '') AS q9_1
  FROM novus.novus_full_dataframe n
  JOIN private.resp_id_map m
    ON m.resp_id = n.respid::text
),
plats_1 AS (
  SELECT
    pid,
    'plats_1'::text AS point_type,
    FALSE AS sensitive,
    ST_Transform(ST_SetSRID(ST_MakePoint(p1_lon, p1_lat), 4326), 3006)::geometry(Point, 3006) AS geom,
    q7_1_txt AS fritext,
    home_municipality,
    kommungrupp,
    source_record,
    1::int AS source_plats_nr,
    plats1_kommunkod AS source_kommunkod,
    plats1_admin_2 AS source_admin_2,
    q7_1
  FROM base
  WHERE p1_lat BETWEEN 54 AND 70
    AND p1_lon BETWEEN 10 AND 25
),
plats_2 AS (
  SELECT
    pid,
    'plats_2'::text AS point_type,
    FALSE AS sensitive,
    ST_Transform(ST_SetSRID(ST_MakePoint(p2_lon, p2_lat), 4326), 3006)::geometry(Point, 3006) AS geom,
    q9_1_txt AS fritext,
    home_municipality,
    kommungrupp,
    source_record,
    2::int AS source_plats_nr,
    plats2_kommunkod AS source_kommunkod,
    plats2_admin_2 AS source_admin_2,
    q9_1
  FROM base
  WHERE p2_lat BETWEEN 54 AND 70
    AND p2_lon BETWEEN 10 AND 25
),
sensitive_points AS (
  SELECT
    pid,
    'plats_sensitive'::text AS point_type,
    TRUE AS sensitive,
    geom,
    NULL::text AS fritext,
    home_municipality,
    kommungrupp,
    source_record,
    source_plats_nr,
    source_kommunkod,
    source_admin_2
  FROM (
    SELECT
      pid, geom, home_municipality, kommungrupp, source_record, source_plats_nr, source_kommunkod, source_admin_2
    FROM plats_1
    WHERE q7_1 = '1'
    UNION ALL
    SELECT
      pid, geom, home_municipality, kommungrupp, source_record, source_plats_nr, source_kommunkod, source_admin_2
    FROM plats_2
    WHERE q9_1 = '1'
  ) s
)
INSERT INTO interim.respondent_points (
  pid, point_type, sensitive, geom, fritext, home_municipality, kommungrupp,
  source_record, source_plats_nr, source_kommunkod, source_admin_2
)
SELECT
  pid, point_type, sensitive, geom, fritext, home_municipality, kommungrupp,
  source_record, source_plats_nr, source_kommunkod, source_admin_2
FROM (
  SELECT pid, point_type, sensitive, geom, fritext, home_municipality, kommungrupp, source_record, source_plats_nr, source_kommunkod, source_admin_2
  FROM plats_1
  UNION ALL
  SELECT pid, point_type, sensitive, geom, fritext, home_municipality, kommungrupp, source_record, source_plats_nr, source_kommunkod, source_admin_2
  FROM plats_2
  UNION ALL
  SELECT pid, point_type, sensitive, geom, fritext, home_municipality, kommungrupp, source_record, source_plats_nr, source_kommunkod, source_admin_2
  FROM sensitive_points
) x;

CREATE INDEX IF NOT EXISTS idx_respondent_points_pid ON interim.respondent_points(pid);
CREATE INDEX IF NOT EXISTS idx_respondent_points_point_type ON interim.respondent_points(point_type);
CREATE INDEX IF NOT EXISTS idx_respondent_points_geom ON interim.respondent_points USING GIST(geom);

-- Part C: point->polygon inherited IDs (for fast app filtering)
CREATE OR REPLACE VIEW interim.v_points_with_landscape AS
SELECT
  p.point_id,
  p.pid,
  p.point_type,
  p.sensitive,
  p.geom,
  p.fritext,
  p.home_municipality,
  p.kommungrupp,
  p.source_record,
  p.source_plats_nr,
  p.source_kommunkod,
  p.source_admin_2,
  k.id AS kommun_id,
  k.kommunnamn AS kommun_namn,
  kg.kommungrupp_id,
  kg.kommungrupp_namn,
  lt.id AS landskapstyp_id,
  lt.namn AS landskapstyp_namn,
  lk.id AS landskapskaraktar_id,
  lk.lk_namn AS landskapskaraktar_namn
FROM interim.respondent_points p
LEFT JOIN LATERAL (
  SELECT id, kommunnamn
  FROM adm_indelning.v_dalarna_kommuner_3006
  WHERE ST_Intersects(p.geom, geom)
  ORDER BY id
  LIMIT 1
) k ON TRUE
LEFT JOIN LATERAL (
  SELECT kommungrupp_id, kommungrupp_namn
  FROM adm_indelning.v_dalarna_kommungrupper_3006
  WHERE ST_Intersects(p.geom, geom)
  ORDER BY kommungrupp_id
  LIMIT 1
) kg ON TRUE
LEFT JOIN LATERAL (
  SELECT id, namn
  FROM landskap.lstw_landskapstyper
  WHERE ST_Intersects(p.geom, geom)
  ORDER BY id
  LIMIT 1
) lt ON TRUE
LEFT JOIN LATERAL (
  SELECT id, lk_namn
  FROM landskap.lstw_landskapskaraktarsomraden
  WHERE ST_Intersects(p.geom, geom)
  ORDER BY id
  LIMIT 1
) lk ON TRUE;

-- Part D: one-point-per-polygon aggregate layers
DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_kommun;
CREATE MATERIALIZED VIEW interim.agg_points_kommun AS
WITH c AS (
  SELECT
    kommun_id,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.v_points_with_landscape
  WHERE kommun_id IS NOT NULL
  GROUP BY kommun_id
)
SELECT
  k.id AS kommun_id,
  k.kommunnamn AS kommun_namn,
  ST_PointOnSurface(k.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM adm_indelning.v_dalarna_kommuner_3006 k
LEFT JOIN c ON c.kommun_id = k.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_kommun_id ON interim.agg_points_kommun(kommun_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_kommun_geom ON interim.agg_points_kommun USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_kommungrupp;
CREATE MATERIALIZED VIEW interim.agg_points_kommungrupp AS
WITH c AS (
  SELECT
    kommungrupp_id,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.v_points_with_landscape
  WHERE kommungrupp_id IS NOT NULL
  GROUP BY kommungrupp_id
)
SELECT
  kg.kommungrupp_id,
  kg.kommungrupp_namn,
  ST_PointOnSurface(kg.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM adm_indelning.v_dalarna_kommungrupper_3006 kg
LEFT JOIN c ON c.kommungrupp_id = kg.kommungrupp_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_kommungrupp_id ON interim.agg_points_kommungrupp(kommungrupp_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_kommungrupp_geom ON interim.agg_points_kommungrupp USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_landskapstyp;
CREATE MATERIALIZED VIEW interim.agg_points_landskapstyp AS
WITH c AS (
  SELECT
    landskapstyp_id,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.v_points_with_landscape
  WHERE landskapstyp_id IS NOT NULL
  GROUP BY landskapstyp_id
)
SELECT
  lt.id AS landskapstyp_id,
  lt.namn AS landskapstyp_namn,
  ST_PointOnSurface(lt.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM landskap.lstw_landskapstyper lt
LEFT JOIN c ON c.landskapstyp_id = lt.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_landskapstyp_id ON interim.agg_points_landskapstyp(landskapstyp_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_landskapstyp_geom ON interim.agg_points_landskapstyp USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_landskapskaraktar;
CREATE MATERIALIZED VIEW interim.agg_points_landskapskaraktar AS
WITH c AS (
  SELECT
    landskapskaraktar_id,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.v_points_with_landscape
  WHERE landskapskaraktar_id IS NOT NULL
  GROUP BY landskapskaraktar_id
)
SELECT
  lk.id AS landskapskaraktar_id,
  lk.lk_namn AS landskapskaraktar_namn,
  ST_PointOnSurface(lk.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM landskap.lstw_landskapskaraktarsomraden lk
LEFT JOIN c ON c.landskapskaraktar_id = lk.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_landskapskaraktar_id ON interim.agg_points_landskapskaraktar(landskapskaraktar_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_landskapskaraktar_geom ON interim.agg_points_landskapskaraktar USING GIST(geom_point);

COMMIT;
