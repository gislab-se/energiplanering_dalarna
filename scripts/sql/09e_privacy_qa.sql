-- Leakage check: respid/resp_id outside private.resp_id_map
SELECT table_schema, table_name, column_name
FROM information_schema.columns
WHERE lower(column_name) IN ('respid', 'resp_id')
  AND NOT (table_schema = 'private' AND table_name = 'resp_id_map')
ORDER BY table_schema, table_name, column_name;

-- Counts by type
SELECT point_type, COUNT(*) AS n
FROM interim.respondent_points
GROUP BY point_type
ORDER BY point_type;

-- Aggregate summaries (8 layers)
SELECT 'kommun' AS layer, COUNT(*) AS n_polygons, SUM(total_points) AS points_total FROM interim.agg_points_kommun
UNION ALL SELECT 'kommungrupp', COUNT(*), SUM(total_points) FROM interim.agg_points_kommungrupp
UNION ALL SELECT 'landskapstyp', COUNT(*), SUM(total_points) FROM interim.agg_points_landskapstyp
UNION ALL SELECT 'landskapskaraktar', COUNT(*), SUM(total_points) FROM interim.agg_points_landskapskaraktar
UNION ALL SELECT 'kulturmiljo', COUNT(*), SUM(total_points) FROM interim.agg_points_kulturmiljo
UNION ALL SELECT 'friluftsliv', COUNT(*), SUM(total_points) FROM interim.agg_points_friluftsliv
UNION ALL SELECT 'vindkraft', COUNT(*), SUM(total_points) FROM interim.agg_points_vindkraft
UNION ALL SELECT 'naturvarden', COUNT(*), SUM(total_points) FROM interim.agg_points_naturvarden;
