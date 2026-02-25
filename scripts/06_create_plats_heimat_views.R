suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

read_env_file <- function(path) {
  if (!file.exists(path)) return(FALSE)
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  lines <- trimws(lines)
  lines <- lines[nzchar(lines) & !startsWith(lines, "#")]
  for (ln in lines) {
    p <- strsplit(ln, "=", fixed = TRUE)[[1]]
    if (length(p) < 2) next
    do.call(Sys.setenv, setNames(list(paste(p[-1], collapse = "=")), p[1]))
  }
  TRUE
}

if (Sys.getenv("PGPASSWORD") == "") {
  read_env_file("C:/gislab/databas/generell_databas_setup/.env")
}

con <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PGHOST"),
  port = as.integer(Sys.getenv("PGPORT")),
  user = Sys.getenv("PGUSER"),
  password = Sys.getenv("PGPASSWORD"),
  dbname = Sys.getenv("PGDATABASE")
)
on.exit(dbDisconnect(con), add = TRUE)

# Plats 1 (EPSG:3006)
dbExecute(con, "
CREATE OR REPLACE VIEW novus.v_plats1_geom_3006 AS
SELECT
  row_number() OVER () AS qgis_id,
  record,
  respid,
  kommungrupp,
  plats_nr,
  kommunkod,
  admin_2,
  plats_fritext,
  lat,
  lon,
  ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 3006)::geometry(Point, 3006) AS geom
FROM novus.v_points_valid
WHERE plats_nr = 1
")

# Plats 2 (EPSG:3006)
dbExecute(con, "
CREATE OR REPLACE VIEW novus.v_plats2_geom_3006 AS
SELECT
  row_number() OVER () AS qgis_id,
  record,
  respid,
  kommungrupp,
  plats_nr,
  kommunkod,
  admin_2,
  plats_fritext,
  lat,
  lon,
  ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 3006)::geometry(Point, 3006) AS geom
FROM novus.v_points_valid
WHERE plats_nr = 2
")

# Heimat-view (keyword-based from free text)
dbExecute(con, "
CREATE OR REPLACE VIEW novus.v_heimat_geom_3006 AS
SELECT
  row_number() OVER () AS qgis_id,
  record,
  respid,
  kommungrupp,
  plats_nr,
  kommunkod,
  admin_2,
  plats_fritext,
  lat,
  lon,
  ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 3006)::geometry(Point, 3006) AS geom
FROM novus.v_points_valid
WHERE lower(coalesce(plats_fritext, '')) ~ '(hem|hemma|stuga|fabod|fjall|skog|barndom|uppvaxt|hembygd)'
")

# Point-level sensitivity views:
# plats_nr = 1 -> q7_1, plats_nr = 2 -> q9_1
dbExecute(con, "
CREATE OR REPLACE VIEW novus.v_extra_sensitive_points_3006 AS
SELECT
  row_number() OVER () AS qgis_id,
  v.record,
  v.respid,
  v.kommungrupp,
  v.plats_nr,
  v.kommunkod,
  v.admin_2,
  v.plats_fritext,
  v.lat,
  v.lon,
  ST_Transform(ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326), 3006)::geometry(Point, 3006) AS geom
FROM novus.v_points_valid v
JOIN novus.novus_full_dataframe n
  ON n.record = v.record
 AND n.respid = v.respid
 AND n.kommungrupp = v.kommungrupp
WHERE (
  v.plats_nr = 1 AND trim(coalesce(n.q7_1::text, '')) = '1'
) OR (
  v.plats_nr = 2 AND trim(coalesce(n.q9_1::text, '')) = '1'
)
")

dbExecute(con, "
CREATE OR REPLACE VIEW novus.v_not_extra_sensitive_points_3006 AS
SELECT
  row_number() OVER () AS qgis_id,
  v.record,
  v.respid,
  v.kommungrupp,
  v.plats_nr,
  v.kommunkod,
  v.admin_2,
  v.plats_fritext,
  v.lat,
  v.lon,
  ST_Transform(ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326), 3006)::geometry(Point, 3006) AS geom
FROM novus.v_points_valid v
JOIN novus.novus_full_dataframe n
  ON n.record = v.record
 AND n.respid = v.respid
 AND n.kommungrupp = v.kommungrupp
WHERE (
  v.plats_nr = 1 AND trim(coalesce(n.q7_1::text, '')) = '0'
) OR (
  v.plats_nr = 2 AND trim(coalesce(n.q9_1::text, '')) = '0'
)
")

res <- dbGetQuery(con, "
SELECT 'v_plats1_geom_3006' AS view_name, COUNT(*) AS n FROM novus.v_plats1_geom_3006
UNION ALL
SELECT 'v_plats2_geom_3006', COUNT(*) FROM novus.v_plats2_geom_3006
UNION ALL
SELECT 'v_heimat_geom_3006', COUNT(*) FROM novus.v_heimat_geom_3006
UNION ALL
SELECT 'v_extra_sensitive_points_3006', COUNT(*) FROM novus.v_extra_sensitive_points_3006
UNION ALL
SELECT 'v_not_extra_sensitive_points_3006', COUNT(*) FROM novus.v_not_extra_sensitive_points_3006
")

print(res)
cat("created_views=novus.v_plats1_geom_3006, novus.v_plats2_geom_3006, novus.v_heimat_geom_3006, novus.v_extra_sensitive_points_3006, novus.v_not_extra_sensitive_points_3006\n")
