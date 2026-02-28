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

find_source_path <- function(dir_name, base_name) {
  root <- file.path("data", "raw", "unpacked", dir_name)
  gpkg <- file.path(root, paste0(base_name, ".gpkg"))
  shp <- file.path(root, paste0(base_name, ".shp"))
  if (file.exists(gpkg)) return(gpkg)
  if (file.exists(shp)) return(shp)
  stop("Missing source layer (gpkg/shp): ", root, "/", base_name, call. = FALSE)
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

schema <- "landskap"
dbExecute(con, "CREATE EXTENSION IF NOT EXISTS postgis")
dbExecute(con, sprintf("CREATE SCHEMA IF NOT EXISTS %s", DBI::dbQuoteIdentifier(con, schema)))

layers <- list(
  list(
    table = "lstw_landskapskaraktarsomraden",
    dir_name = "Lstw.LstW_Landskapskaraktarsomraden",
    base_name = "Lstw.LstW_Landskapskaraktarsomraden"
  ),
  list(
    table = "lstw_landskapstyper",
    dir_name = "Lstw.LstW_Landskapstyper",
    base_name = "Lstw.LstW_Landskapstyper"
  ),
  list(
    table = "lst_ri_rorligt_friluftsliv_mb4kap2",
    dir_name = "lst.LST_RI_Rorligt_friluftsliv_MB4kap2",
    base_name = "lst.LST_RI_Rorligt_friluftsliv_MB4kap2"
  ),
  list(
    table = "lstw_regional_analys_vindkraft_juni2024",
    dir_name = "Lstw.LstW_Regional_analys_utbyggnad_vindkraft_juni2024",
    base_name = "Lstw.LstW_Regional_analys_utbyggnad_vindkraft_juni2024"
  ),
  list(
    table = "lstw_pg204_naturvarden_kanda_av_lst_dalarna",
    dir_name = "Lstw.PG204_naturvarden_kanda_av_lst_dalarna",
    base_name = "Lstw.PG204_naturvarden_kanda_av_lst_dalarna"
  )
)

for (item in layers) {
  src <- find_source_path(item$dir_name, item$base_name)
  cat("Reading:", src, "\n")
  g <- st_read(src, quiet = TRUE)

  # Use lowercase columns for predictable SQL usage.
  names(g) <- tolower(names(g))

  cat("Writing:", paste0(schema, ".", item$table), "\n")
  suppressWarnings(
    st_write(
      g,
      dsn = con,
      layer = DBI::Id(schema = schema, table = item$table),
      append = FALSE,
      quiet = TRUE
    )
  )
}

report <- dbGetQuery(
  con,
  "
  SELECT
    f_table_name AS table_name,
    f_geometry_column AS geometry_column,
    srid,
    type
  FROM public.geometry_columns
  WHERE f_table_schema = 'landskap'
  ORDER BY f_table_name
  "
)

cat('\nLoaded geometry tables in schema landskap:\n')
print(report)
cat('\nDone.\n')
