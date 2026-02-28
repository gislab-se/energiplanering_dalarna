-- QA checks for privacy-first point pipeline

-- 1) Leakage check: respid/resp_id columns outside private.resp_id_map
SELECT
  table_schema,
  table_name,
  column_name
FROM information_schema.columns
WHERE lower(column_name) IN ('respid', 'resp_id')
  AND NOT (table_schema = 'private' AND table_name = 'resp_id_map')
ORDER BY table_schema, table_name, column_name;

-- 2) Ensure interim outputs have pid but no resp_id
SELECT
  table_schema,
  table_name,
  string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'interim'
  AND table_name IN (
    'respondent_points',
    'v_points_with_landscape',
    'agg_points_kommun',
    'agg_points_kommungrupp',
    'agg_points_landskapstyp',
    'agg_points_landskapskaraktar'
  )
GROUP BY table_schema, table_name
ORDER BY table_name;

-- 3) Counts per point_type
SELECT point_type, COUNT(*) AS n
FROM interim.respondent_points
GROUP BY point_type
ORDER BY point_type;

-- 4) Basic integrity checks
SELECT
  SUM(CASE WHEN pid IS NULL THEN 1 ELSE 0 END) AS null_pid_rows,
  SUM(CASE WHEN point_type = 'plats_sensitive' AND sensitive IS NOT TRUE THEN 1 ELSE 0 END) AS bad_sensitive_flag_rows,
  SUM(CASE WHEN point_type IN ('plats_1', 'plats_2') AND sensitive IS NOT FALSE THEN 1 ELSE 0 END) AS bad_non_sensitive_flag_rows
FROM interim.respondent_points;

-- 5) Aggregation counts by polygon layer
SELECT 'kommun' AS layer, COUNT(*) AS n_polygons, SUM(total_points) AS points_total
FROM interim.agg_points_kommun
UNION ALL
SELECT 'kommungrupp', COUNT(*), SUM(total_points)
FROM interim.agg_points_kommungrupp
UNION ALL
SELECT 'landskapstyp', COUNT(*), SUM(total_points)
FROM interim.agg_points_landskapstyp
UNION ALL
SELECT 'landskapskaraktar', COUNT(*), SUM(total_points)
FROM interim.agg_points_landskapskaraktar;
