suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(sf)
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

required <- c("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE")
missing <- required[Sys.getenv(required) == ""]
if (length(missing) > 0) stop("Missing DB env vars: ", paste(missing, collapse = ", "))

con <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PGHOST"),
  port = as.integer(Sys.getenv("PGPORT")),
  user = Sys.getenv("PGUSER"),
  password = Sys.getenv("PGPASSWORD"),
  dbname = Sys.getenv("PGDATABASE")
)
on.exit(dbDisconnect(con), add = TRUE)

schema <- "adm_indelning"
gpkg <- "C:/gislab/data/deso/DeSO_2025.gpkg"

if (!file.exists(gpkg)) stop("Missing file: ", gpkg)

cat("Creating schema...\n")
dbExecute(con, sprintf("CREATE SCHEMA IF NOT EXISTS %s", DBI::dbQuoteIdentifier(con, schema)))

cat("Reading DeSO...\n")
lyr <- st_layers(gpkg)$name[1]
deso <- st_read(gpkg, layer = lyr, quiet = TRUE)

# Normalize column names
names(deso) <- tolower(names(deso))

cat("Writing table adm_indelning.deso_2025 ...\n")
suppressWarnings(
  st_write(
    deso,
    dsn = con,
    layer = DBI::Id(schema = schema, table = "deso_2025"),
    append = FALSE,
    quiet = TRUE
  )
)

cat("Creating views...\n")
dbExecute(con, "CREATE EXTENSION IF NOT EXISTS postgis")

dbExecute(con, "
CREATE OR REPLACE VIEW adm_indelning.v_dalarna_deso_3006 AS
SELECT
  objectid,
  objektidentitet,
  desokod,
  regsokod,
  lanskod,
  kommunkod,
  kommunnamn,
  version,
  ST_Transform(geometry, 3006)::geometry(MultiPolygon, 3006) AS geom
FROM adm_indelning.deso_2025
WHERE lanskod = '20'
")

dbExecute(con, "
CREATE OR REPLACE VIEW adm_indelning.v_dalarna_lan_3006 AS
SELECT
  1::int AS id,
  '20'::text AS lanskod,
  'Dalarna'::text AS lansnamn,
  ST_Multi(ST_UnaryUnion(ST_Collect(geom)))::geometry(MultiPolygon, 3006) AS geom
FROM adm_indelning.v_dalarna_deso_3006
")

dbExecute(con, "
CREATE OR REPLACE VIEW adm_indelning.v_dalarna_kommuner_3006 AS
SELECT
  row_number() OVER (ORDER BY kommunkod) AS id,
  kommunkod,
  kommunnamn,
  ST_Multi(ST_UnaryUnion(ST_Collect(geom)))::geometry(MultiPolygon, 3006) AS geom
FROM adm_indelning.v_dalarna_deso_3006
GROUP BY kommunkod, kommunnamn
")

dbExecute(con, "
CREATE OR REPLACE VIEW adm_indelning.v_dalarna_kommungrupper_3006 AS
WITH k AS (
  SELECT
    kommunkod,
    kommunnamn,
    CASE
      WHEN kommunkod IN ('2023','2039','2021') THEN 1
      WHEN kommunkod IN ('2031','2029','2026') THEN 2
      WHEN kommunkod IN ('2080','2081') THEN 3
      WHEN kommunkod IN ('2061','2085') THEN 4
      WHEN kommunkod IN ('2062','2034') THEN 5
      WHEN kommunkod IN ('2084','2083','2082') THEN 6
      ELSE NULL
    END AS kommungrupp_id
  FROM adm_indelning.v_dalarna_kommuner_3006
)
SELECT
  row_number() OVER (ORDER BY kommungrupp_id) AS id,
  kommungrupp_id,
  CASE
    WHEN kommungrupp_id = 1 THEN 'Kommungrupp 1: Malung-Sälen, Älvdalen, Vansbro'
    WHEN kommungrupp_id = 2 THEN 'Kommungrupp 2: Rättvik, Leksand, Gagnef'
    WHEN kommungrupp_id = 3 THEN 'Kommungrupp 3: Falun, Borlänge'
    WHEN kommungrupp_id = 4 THEN 'Kommungrupp 4: Smedjebacken, Ludvika'
    WHEN kommungrupp_id = 5 THEN 'Kommungrupp 5: Mora, Orsa'
    WHEN kommungrupp_id = 6 THEN 'Kommungrupp 6: Avesta, Hedemora, Säter'
    ELSE 'Okänd'
  END AS kommungrupp_namn,
  string_agg(kommunnamn, ', ' ORDER BY kommunnamn) AS kommuner,
  ST_Multi(ST_UnaryUnion(ST_Collect(vk.geom)))::geometry(MultiPolygon, 3006) AS geom
FROM k
JOIN adm_indelning.v_dalarna_kommuner_3006 vk USING (kommunkod, kommunnamn)
WHERE kommungrupp_id IS NOT NULL
GROUP BY kommungrupp_id
")

cnt <- dbGetQuery(con, "
SELECT
  (SELECT COUNT(*) FROM adm_indelning.deso_2025) AS n_deso_total,
  (SELECT COUNT(*) FROM adm_indelning.v_dalarna_deso_3006) AS n_deso_dalarna,
  (SELECT COUNT(*) FROM adm_indelning.v_dalarna_kommuner_3006) AS n_kommuner,
  (SELECT COUNT(*) FROM adm_indelning.v_dalarna_kommungrupper_3006) AS n_kommungrupper
")

print(cnt)
cat('created=adm_indelning.deso_2025 + views in adm_indelning\n')