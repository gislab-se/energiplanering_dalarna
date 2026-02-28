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
  lk.lk_namn AS landskapskaraktar_namn,
  km.objectid AS kulturmiljo_id,
  km.namn AS kulturmiljo_namn,
  fr.objectid AS friluftsliv_id,
  fr.namn AS friluftsliv_namn,
  vk.objektid AS vindkraft_id,
  vk.typ_av_omr AS vindkraft_namn,
  nv.objectid AS naturvarden_id,
  nv.objektnamn AS naturvarden_namn
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
) lk ON TRUE
LEFT JOIN LATERAL (
  SELECT objectid, namn
  FROM landskap.raa_ri_kulturmiljovard_mb3kap6
  WHERE ST_Intersects(p.geom, geom)
  ORDER BY objectid
  LIMIT 1
) km ON TRUE
LEFT JOIN LATERAL (
  SELECT objectid, namn
  FROM landskap.lst_ri_rorligt_friluftsliv_mb4kap2
  WHERE ST_Intersects(p.geom, geom)
  ORDER BY objectid
  LIMIT 1
) fr ON TRUE
LEFT JOIN LATERAL (
  SELECT objektid, typ_av_omr
  FROM landskap.lstw_regional_analys_vindkraft_juni2024
  WHERE ST_Intersects(p.geom, geom)
  ORDER BY objektid
  LIMIT 1
) vk ON TRUE
LEFT JOIN LATERAL (
  SELECT objectid, objektnamn
  FROM landskap.lstw_pg204_naturvarden_kanda_av_lst_dalarna
  WHERE ST_Intersects(p.geom, geom)
  ORDER BY objectid
  LIMIT 1
) nv ON TRUE;
