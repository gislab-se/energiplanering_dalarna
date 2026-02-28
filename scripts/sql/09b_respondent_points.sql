CREATE EXTENSION IF NOT EXISTS postgis;

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
