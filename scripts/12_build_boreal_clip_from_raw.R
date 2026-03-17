args <- commandArgs(trailingOnly = TRUE)

repo_root <- if (length(args) >= 1) normalizePath(args[[1]], winslash = "/", mustWork = FALSE) else getwd()

suppressPackageStartupMessages({
  library(terra)
})

raw_tif <- file.path(repo_root, "data", "raw", "skoglig_vardekarna", "tathetsanalys_3000m_procent.tif")
admin_gpkg <- file.path(repo_root, "data", "cloud", "admin_boundaries.gpkg")
out_tif <- file.path(repo_root, "data", "cloud", "tathetsanalys_3000m_procent_lan_clip.tif")

if (!file.exists(raw_tif)) {
  stop("Missing raw raster: ", raw_tif, call. = FALSE)
}
if (!file.exists(admin_gpkg)) {
  stop("Missing admin gpkg: ", admin_gpkg, call. = FALSE)
}

max_width <- 1800L
max_height <- 2000L

r <- rast(raw_tif)
lan <- vect(admin_gpkg, layer = "lan")

if (!same.crs(r, lan)) {
  lan <- project(lan, crs(r))
}

masked <- mask(crop(r, lan, snap = "out"), lan)
masked <- trim(masked)

fact_x <- ceiling(ncol(masked) / max_width)
fact_y <- ceiling(nrow(masked) / max_height)
fact <- max(1L, fact_x, fact_y)

if (fact > 1L) {
  masked <- aggregate(masked, fact = fact, fun = "max", na.rm = TRUE)
}

writeRaster(
  masked,
  out_tif,
  overwrite = TRUE,
  datatype = "INT1U",
  NAflag = 0,
  gdal = c("COMPRESS=LZW", "TFW=YES")
)

cat("raw_dim=", paste(dim(r), collapse = " x "), "\n", sep = "")
cat("clip_dim=", paste(dim(masked), collapse = " x "), "\n", sep = "")
cat("clip_res=", paste(res(masked), collapse = ", "), "\n", sep = "")
cat("clip_ext=", paste(ext(masked), collapse = ", "), "\n", sep = "")
cat("output=", out_tif, "\n", sep = "")
