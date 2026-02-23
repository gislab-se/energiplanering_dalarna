suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(readr)
})

read_env_file <- function(path) {
  if (!file.exists(path)) return(FALSE)
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  lines <- trimws(lines)
  lines <- lines[nzchar(lines) & !startsWith(lines, "#")]
  for (ln in lines) {
    parts <- strsplit(ln, "=", fixed = TRUE)[[1]]
    if (length(parts) < 2) next
    key <- parts[1]
    val <- paste(parts[-1], collapse = "=")
    do.call(Sys.setenv, setNames(list(val), key))
  }
  TRUE
}

sanitize_names <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  x
}

repo_root <- normalizePath(".", winslash = "/", mustWork = TRUE)
input_csv <- file.path(repo_root, "data", "interim", "novus", "novus_full_dataframe.csv")
if (!file.exists(input_csv)) stop("Missing input file: ", input_csv)

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

schema <- "novus"
table_name <- "novus_full_dataframe"

cat("Reading CSV...\n")
df <- readr::read_csv(input_csv, show_col_types = FALSE, progress = FALSE)
names(df) <- sanitize_names(names(df))

cat("Creating schema/table...\n")
dbExecute(con, "CREATE SCHEMA IF NOT EXISTS novus")
dbExecute(con, "DROP TABLE IF EXISTS novus.novus_full_dataframe")
DBI::dbWriteTable(con, DBI::Id(schema = schema, table = table_name), df, overwrite = TRUE, row.names = FALSE)

pk_check <- dbGetQuery(con, "SELECT COUNT(*) n, COUNT(respid) n_respid, COUNT(DISTINCT respid) n_distinct FROM novus.novus_full_dataframe")
if (pk_check$n == pk_check$n_respid && pk_check$n == pk_check$n_distinct) {
  dbExecute(con, "ALTER TABLE novus.novus_full_dataframe ADD CONSTRAINT novus_full_dataframe_pkey PRIMARY KEY (respid)")
}

cat("Creating views...\n")

sql_long <- "
CREATE OR REPLACE VIEW novus.v_points_long AS
SELECT
  record,
  respid,
  kommungrupp,
  1::int AS plats_nr,
  plats1_kommunkod AS kommunkod,
  plats1_admin_2 AS admin_2,
  CASE
    WHEN NULLIF(trim(plats1_data_10::text), '') IS NULL THEN NULL
    ELSE NULLIF(regexp_replace(trim(plats1_data_10::text), '[^0-9.+-]', '', 'g'), '')::double precision
  END AS lat,
  CASE
    WHEN NULLIF(trim(plats1_data_11::text), '') IS NULL THEN NULL
    ELSE NULLIF(regexp_replace(trim(plats1_data_11::text), '[^0-9.+-]', '', 'g'), '')::double precision
  END AS lon,
  q7_1_txt AS plats_fritext
FROM novus.novus_full_dataframe
UNION ALL
SELECT
  record,
  respid,
  kommungrupp,
  2::int AS plats_nr,
  plats2_kommunkod AS kommunkod,
  plats2_admin_2 AS admin_2,
  CASE
    WHEN NULLIF(trim(plats2_data_10::text), '') IS NULL THEN NULL
    ELSE NULLIF(regexp_replace(trim(plats2_data_10::text), '[^0-9.+-]', '', 'g'), '')::double precision
  END AS lat,
  CASE
    WHEN NULLIF(trim(plats2_data_11::text), '') IS NULL THEN NULL
    ELSE NULLIF(regexp_replace(trim(plats2_data_11::text), '[^0-9.+-]', '', 'g'), '')::double precision
  END AS lon,
  q9_1_txt AS plats_fritext
FROM novus.novus_full_dataframe
"

sql_valid <- "
CREATE OR REPLACE VIEW novus.v_points_valid AS
SELECT *
FROM novus.v_points_long
WHERE lat IS NOT NULL
  AND lon IS NOT NULL
  AND lat BETWEEN 54 AND 70
  AND lon BETWEEN 10 AND 25
"

sql_agg <- "
CREATE OR REPLACE VIEW novus.v_points_by_kommunkod AS
SELECT
  kommunkod,
  COUNT(*) AS n_points,
  COUNT(DISTINCT respid) AS n_respondents
FROM novus.v_points_valid
GROUP BY kommunkod
ORDER BY n_points DESC
"

sql_resp <- "
CREATE OR REPLACE VIEW novus.v_response_summary AS
SELECT
  record,
  respid,
  kommungrupp,
  CASE WHEN NULLIF(trim(plats1_data_10::text), '') IS NOT NULL AND NULLIF(trim(plats1_data_11::text), '') IS NOT NULL THEN 1 ELSE 0 END AS has_plats1_coords,
  CASE WHEN NULLIF(trim(plats2_data_10::text), '') IS NOT NULL AND NULLIF(trim(plats2_data_11::text), '') IS NOT NULL THEN 1 ELSE 0 END AS has_plats2_coords
FROM novus.novus_full_dataframe
"

dbExecute(con, sql_long)
dbExecute(con, sql_valid)
dbExecute(con, sql_agg)
dbExecute(con, sql_resp)

counts <- dbGetQuery(con, "
SELECT
  (SELECT COUNT(*) FROM novus.novus_full_dataframe) AS n_rows,
  (SELECT COUNT(*) FROM novus.v_points_long) AS n_points_long,
  (SELECT COUNT(*) FROM novus.v_points_valid) AS n_points_valid
")

cat(sprintf("schema=%s\n", schema))
cat(sprintf("table=%s.%s\n", schema, table_name))
cat(sprintf("rows=%s\n", counts$n_rows))
cat(sprintf("points_long=%s\n", counts$n_points_long))
cat(sprintf("points_valid=%s\n", counts$n_points_valid))
cat("views=novus.v_points_long, novus.v_points_valid, novus.v_points_by_kommunkod, novus.v_response_summary\n")