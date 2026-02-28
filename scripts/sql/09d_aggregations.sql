-- Faster per-layer aggregations directly from interim.respondent_points

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_kommun;
CREATE MATERIALIZED VIEW interim.agg_points_kommun AS
WITH c AS (
  SELECT
    l.id AS kommun_id,
    l.kommunnamn AS kommun_namn,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.respondent_points p
  JOIN adm_indelning.v_dalarna_kommuner_3006 l
    ON ST_Intersects(p.geom, l.geom)
  GROUP BY l.id, l.kommunnamn
)
SELECT
  l.id AS kommun_id,
  l.kommunnamn AS kommun_namn,
  ST_PointOnSurface(l.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM adm_indelning.v_dalarna_kommuner_3006 l
LEFT JOIN c ON c.kommun_id = l.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_kommun_id ON interim.agg_points_kommun(kommun_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_kommun_geom ON interim.agg_points_kommun USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_kommungrupp;
CREATE MATERIALIZED VIEW interim.agg_points_kommungrupp AS
WITH c AS (
  SELECT
    l.kommungrupp_id,
    l.kommungrupp_namn,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.respondent_points p
  JOIN adm_indelning.v_dalarna_kommungrupper_3006 l
    ON ST_Intersects(p.geom, l.geom)
  GROUP BY l.kommungrupp_id, l.kommungrupp_namn
)
SELECT
  l.kommungrupp_id,
  l.kommungrupp_namn,
  ST_PointOnSurface(l.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM adm_indelning.v_dalarna_kommungrupper_3006 l
LEFT JOIN c ON c.kommungrupp_id = l.kommungrupp_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_kommungrupp_id ON interim.agg_points_kommungrupp(kommungrupp_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_kommungrupp_geom ON interim.agg_points_kommungrupp USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_landskapstyp;
CREATE MATERIALIZED VIEW interim.agg_points_landskapstyp AS
WITH c AS (
  SELECT
    l.id AS landskapstyp_id,
    l.namn AS landskapstyp_namn,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.respondent_points p
  JOIN landskap.lstw_landskapstyper l
    ON ST_Intersects(p.geom, l.geom)
  GROUP BY l.id, l.namn
)
SELECT
  l.id AS landskapstyp_id,
  l.namn AS landskapstyp_namn,
  ST_PointOnSurface(l.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM landskap.lstw_landskapstyper l
LEFT JOIN c ON c.landskapstyp_id = l.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_landskapstyp_id ON interim.agg_points_landskapstyp(landskapstyp_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_landskapstyp_geom ON interim.agg_points_landskapstyp USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_landskapskaraktar;
CREATE MATERIALIZED VIEW interim.agg_points_landskapskaraktar AS
WITH c AS (
  SELECT
    l.id AS landskapskaraktar_id,
    l.lk_namn AS landskapskaraktar_namn,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.respondent_points p
  JOIN landskap.lstw_landskapskaraktarsomraden l
    ON ST_Intersects(p.geom, l.geom)
  GROUP BY l.id, l.lk_namn
)
SELECT
  l.id AS landskapskaraktar_id,
  l.lk_namn AS landskapskaraktar_namn,
  ST_PointOnSurface(l.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM landskap.lstw_landskapskaraktarsomraden l
LEFT JOIN c ON c.landskapskaraktar_id = l.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_landskapskaraktar_id ON interim.agg_points_landskapskaraktar(landskapskaraktar_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_landskapskaraktar_geom ON interim.agg_points_landskapskaraktar USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_kulturmiljo;
CREATE MATERIALIZED VIEW interim.agg_points_kulturmiljo AS
WITH c AS (
  SELECT
    l.objectid AS kulturmiljo_id,
    l.namn AS kulturmiljo_namn,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.respondent_points p
  JOIN landskap.raa_ri_kulturmiljovard_mb3kap6 l
    ON ST_Intersects(p.geom, l.geom)
  GROUP BY l.objectid, l.namn
)
SELECT
  l.objectid AS kulturmiljo_id,
  l.namn AS kulturmiljo_namn,
  ST_PointOnSurface(l.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM landskap.raa_ri_kulturmiljovard_mb3kap6 l
LEFT JOIN c ON c.kulturmiljo_id = l.objectid;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_kulturmiljo_id ON interim.agg_points_kulturmiljo(kulturmiljo_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_kulturmiljo_geom ON interim.agg_points_kulturmiljo USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_friluftsliv;
CREATE MATERIALIZED VIEW interim.agg_points_friluftsliv AS
WITH c AS (
  SELECT
    l.objectid AS friluftsliv_id,
    l.namn AS friluftsliv_namn,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.respondent_points p
  JOIN landskap.lst_ri_rorligt_friluftsliv_mb4kap2 l
    ON ST_Intersects(p.geom, l.geom)
  GROUP BY l.objectid, l.namn
)
SELECT
  l.objectid AS friluftsliv_id,
  l.namn AS friluftsliv_namn,
  ST_PointOnSurface(l.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM landskap.lst_ri_rorligt_friluftsliv_mb4kap2 l
LEFT JOIN c ON c.friluftsliv_id = l.objectid;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_friluftsliv_id ON interim.agg_points_friluftsliv(friluftsliv_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_friluftsliv_geom ON interim.agg_points_friluftsliv USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_vindkraft;
CREATE MATERIALIZED VIEW interim.agg_points_vindkraft AS
WITH c AS (
  SELECT
    l.objektid AS vindkraft_id,
    l.typ_av_omr AS vindkraft_namn,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.respondent_points p
  JOIN landskap.lstw_regional_analys_vindkraft_juni2024 l
    ON ST_Intersects(p.geom, l.geom)
  GROUP BY l.objektid, l.typ_av_omr
)
SELECT
  l.objektid AS vindkraft_id,
  l.typ_av_omr AS vindkraft_namn,
  ST_PointOnSurface(l.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM landskap.lstw_regional_analys_vindkraft_juni2024 l
LEFT JOIN c ON c.vindkraft_id = l.objektid;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_vindkraft_id ON interim.agg_points_vindkraft(vindkraft_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_vindkraft_geom ON interim.agg_points_vindkraft USING GIST(geom_point);

DROP MATERIALIZED VIEW IF EXISTS interim.agg_points_naturvarden;
CREATE MATERIALIZED VIEW interim.agg_points_naturvarden AS
WITH c AS (
  SELECT
    l.objectid AS naturvarden_id,
    l.objektnamn AS naturvarden_namn,
    COUNT(*) AS total_points,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_1') AS count_plats_1,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_2') AS count_plats_2,
    COUNT(*) FILTER (WHERE p.point_type = 'plats_sensitive') AS count_sensitive
  FROM interim.respondent_points p
  JOIN landskap.lstw_pg204_naturvarden_kanda_av_lst_dalarna l
    ON ST_Intersects(p.geom, l.geom)
  GROUP BY l.objectid, l.objektnamn
)
SELECT
  l.objectid AS naturvarden_id,
  l.objektnamn AS naturvarden_namn,
  ST_PointOnSurface(l.geom)::geometry(Point, 3006) AS geom_point,
  COALESCE(c.total_points, 0) AS total_points,
  COALESCE(c.count_plats_1, 0) AS count_plats_1,
  COALESCE(c.count_plats_2, 0) AS count_plats_2,
  COALESCE(c.count_sensitive, 0) AS count_sensitive
FROM landskap.lstw_pg204_naturvarden_kanda_av_lst_dalarna l
LEFT JOIN c ON c.naturvarden_id = l.objectid;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agg_points_naturvarden_id ON interim.agg_points_naturvarden(naturvarden_id);
CREATE INDEX IF NOT EXISTS idx_agg_points_naturvarden_geom ON interim.agg_points_naturvarden USING GIST(geom_point);
