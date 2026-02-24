#!/usr/bin/env Rscript

in_dir <- file.path("data", "interim", "hem_kommun_network")
out_dir <- file.path("data", "processed", "hem_kommun_network")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

files <- c(
  "nodes.csv",
  "edges.csv",
  "hem_by_kommun.csv",
  "hem_context_strict.csv",
  "hem_context_lemma.csv",
  "word_frequency.csv",
  "word_frequency_excl_hem.csv",
  "response_tokens.csv"
)

paths <- file.path(in_dir, files)
missing <- paths[!file.exists(paths)]
if (length(missing) > 0) {
  stop(
    "Missing input files:\n",
    paste(missing, collapse = "\n"),
    call. = FALSE
  )
}

tabs <- lapply(paths, function(p) read.csv(p, stringsAsFactors = FALSE, check.names = FALSE))
names(tabs) <- sub("\\.csv$", "", files)

xlsx_path <- file.path(out_dir, "hem_kommun_network.xlsx")

if (requireNamespace("openxlsx", quietly = TRUE)) {
  wb <- openxlsx::createWorkbook()
  for (nm in names(tabs)) {
    openxlsx::addWorksheet(wb, nm)
    openxlsx::writeData(wb, nm, tabs[[nm]])
  }
  openxlsx::saveWorkbook(wb, xlsx_path, overwrite = TRUE)
} else if (requireNamespace("writexl", quietly = TRUE)) {
  writexl::write_xlsx(tabs, path = xlsx_path)
} else {
  stop("Neither 'openxlsx' nor 'writexl' is installed.", call. = FALSE)
}

cat("Excel written to:\n", xlsx_path, "\n", sep = "")
