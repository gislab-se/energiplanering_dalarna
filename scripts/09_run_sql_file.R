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

execute_sql_file <- function(con, path) {
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  if (length(lines) > 0) {
    # Remove UTF-8 BOM if present.
    lines[[1]] <- sub("^\ufeff", "", lines[[1]])
  }

  buf <- character()
  n <- 0L
  for (ln in lines) {
    buf <- c(buf, ln)
    if (grepl(";\\s*$", ln)) {
      stmt <- paste(buf, collapse = "\n")
      stmt_exec <- paste(
        Filter(function(x) !grepl("^\\s*--", x), strsplit(stmt, "\n", fixed = TRUE)[[1]]),
        collapse = "\n"
      )
      if (nchar(trimws(stmt_exec)) > 0) {
        dbExecute(con, stmt_exec)
        n <- n + 1L
      }
      buf <- character()
    }
  }
  n
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript scripts/09_run_sql_file.R <sql_file_path>", call. = FALSE)
}

sql_path <- args[[1]]
if (!file.exists(sql_path)) stop("SQL file not found: ", sql_path, call. = FALSE)

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

n <- execute_sql_file(con, sql_path)
cat(sprintf("executed_statements=%d\n", n))
