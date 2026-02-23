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
WHERE lower(coalesce(plats_fritext, '')) ~ '(hem|hemma|stuga|f[aä]bod|fj[aä]ll|skog|barndom|uppv[aä]xt|hembygd)'
")

res <- dbGetQuery(con, "
SELECT 'v_plats1_geom_3006' AS view_name, COUNT(*) AS n FROM novus.v_plats1_geom_3006
UNION ALL
SELECT 'v_plats2_geom_3006', COUNT(*) FROM novus.v_plats2_geom_3006
UNION ALL
SELECT 'v_heimat_geom_3006', COUNT(*) FROM novus.v_heimat_geom_3006
")

print(res)
cat('created_views=novus.v_plats1_geom_3006, novus.v_plats2_geom_3006, novus.v_heimat_geom_3006\n')