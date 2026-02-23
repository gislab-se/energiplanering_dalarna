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

dbExecute(con, "CREATE EXTENSION IF NOT EXISTS postgis")

dbExecute(con, "
CREATE OR REPLACE VIEW novus.v_points_valid_geom_4326 AS
SELECT
  row_number() OVER () AS qgis_id,
  record,
  respid,
  kommungrupp,
  plats_nr,
  kommunkod,
  admin_2,
  lat,
  lon,
  ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geometry(Point, 4326) AS geom
FROM novus.v_points_valid
")

dbExecute(con, "
CREATE OR REPLACE VIEW novus.v_points_valid_geom_3006 AS
SELECT
  qgis_id,
  record,
  respid,
  kommungrupp,
  plats_nr,
  kommunkod,
  admin_2,
  lat,
  lon,
  ST_Transform(geom, 3006)::geometry(Point, 3006) AS geom
FROM novus.v_points_valid_geom_4326
")

dbExecute(con, "
CREATE OR REPLACE VIEW novus.v_points_by_kommunkod_with_centroid AS
SELECT
  row_number() OVER () AS qgis_id,
  kommunkod,
  COUNT(*) AS n_points,
  COUNT(DISTINCT respid) AS n_respondents,
  ST_SetSRID(ST_MakePoint(AVG(lon), AVG(lat)), 4326)::geometry(Point, 4326) AS geom
FROM novus.v_points_valid
GROUP BY kommunkod
")

cat('created_views=novus.v_points_valid_geom_4326, novus.v_points_valid_geom_3006, novus.v_points_by_kommunkod_with_centroid\n')