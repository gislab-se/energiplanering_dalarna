library(sf)
library(dplyr)
library(readr)
library(tidyr)

# ---- Input/Output ------------------------------------------------------------
deso_gpkg  <- "C:/Users/henri/data/DeSO_2025.gpkg"
deso_layer <- "DeSO_2025"
lan_keep   <- 20
kg_csv     <- "C:/gislab/energiplanering_dalarna/data/interim/novus/novus_full_dataframe.csv"
out_gpkg   <- "C:/gislab/energiplanering_dalarna/data/cloud/admin_boundaries.gpkg"


norm_code <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x <- sub("\\.0$", "", x)
  x
}

keep_with_geometry <- function(sf_obj, fields) {
  sf_col <- attr(sf_obj, "sf_column")
  sf_obj |>
    dplyr::select(dplyr::any_of(fields), dplyr::all_of(sf_col))
}


# ---- 1) Read + filter DeSO ---------------------------------------------------
deso <- st_read(deso_gpkg, layer = deso_layer, quiet = TRUE) |>
  mutate(
    lanskod = suppressWarnings(as.integer(lanskod)),
    kommunkod = norm_code(kommunkod)
  ) |>
  filter(lanskod == lan_keep) |>
  st_make_valid()


# ---- 2) Dissolve to municipalities ------------------------------------------
kommuner <- deso |>
  group_by(kommunkod, kommunnamn) |>
  summarise(do_union = TRUE, .groups = "drop") |>
  st_cast("MULTIPOLYGON") |>
  st_make_valid()


# ---- 3) Canonical mapping (deterministic) -----------------------------------
canonical_map <- tibble::tribble(
  ~kommunkod, ~kommungrupp_id,
  "2084", 1,  # Avesta
  "2083", 1,  # Hedemora
  "2082", 1,  # Sater
  "2080", 2,  # Falun
  "2081", 2,  # Borlange
  "2023", 3,  # Malung-Salen
  "2039", 3,  # Alvdalen
  "2021", 3,  # Vansbro
  "2062", 4,  # Mora
  "2034", 4,  # Orsa
  "2031", 5,  # Rattvik
  "2029", 5,  # Leksand
  "2026", 5,  # Gagnef
  "2061", 6,  # Smedjebacken
  "2085", 6   # Ludvika
)

kg_names <- tibble::tribble(
  ~kommungrupp_id, ~kommungrupp_namn,
  1, "Avesta, Hedemora, Sater",
  2, "Falun, Borlange",
  3, "Malung-Salen, Alvdalen, Vansbro",
  4, "Mora, Orsa",
  5, "Rattvik, Leksand, Gagnef",
  6, "Smedjebacken, Ludvika"
)


# ---- 4) Optional fallback from Novus majority mapping ------------------------
kg_from_csv <- read_csv(kg_csv, show_col_types = FALSE) |>
  transmute(
    kommunkod = norm_code(Q1),
    kommungrupp_id = suppressWarnings(as.integer(Kommungrupp))
  ) |>
  filter(!is.na(kommunkod), kommunkod != "", !is.na(kommungrupp_id)) |>
  count(kommunkod, kommungrupp_id, name = "n") |>
  group_by(kommunkod) |>
  slice_max(order_by = n, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(kommunkod, kommungrupp_id)


# ---- 5) Assign kommungrupp_id + name to municipalities -----------------------
kommuner <- kommuner |>
  left_join(canonical_map, by = "kommunkod", suffix = c("", "_canon")) |>
  left_join(kg_from_csv, by = "kommunkod", suffix = c("", "_csv")) |>
  mutate(
    kommungrupp_id = coalesce(kommungrupp_id, kommungrupp_id_csv)
  ) |>
  select(-kommungrupp_id_csv) |>
  left_join(kg_names, by = "kommungrupp_id")

missing <- kommuner |>
  st_drop_geometry() |>
  filter(is.na(kommungrupp_id))
if (nrow(missing) > 0) {
  warning(
    "Missing kommungrupp_id for: ",
    paste(missing$kommunnamn, collapse = ", ")
  )
}


# ---- 6) Dissolve to kommungrupper -------------------------------------------
kg_members <- kommuner |>
  st_drop_geometry() |>
  filter(!is.na(kommungrupp_id), !is.na(kommungrupp_namn)) |>
  group_by(kommungrupp_id, kommungrupp_namn) |>
  summarise(kommuner = paste(sort(unique(kommunnamn)), collapse = ", "), .groups = "drop")

kommungrupper <- kommuner |>
  filter(!is.na(kommungrupp_id), !is.na(kommungrupp_namn)) |>
  group_by(kommungrupp_id, kommungrupp_namn) |>
  summarise(do_union = TRUE, .groups = "drop") |>
  left_join(kg_members, by = c("kommungrupp_id", "kommungrupp_namn")) |>
  st_cast("MULTIPOLYGON") |>
  st_make_valid()


# ---- 7) Dissolve to lan ------------------------------------------------------
lan <- deso |>
  group_by(lanskod) |>
  summarise(do_union = TRUE, .groups = "drop") |>
  st_cast("MULTIPOLYGON") |>
  st_make_valid() |>
  mutate(
    lanskod = as.character(lanskod),
    lansnamn = "Dalarnas lan"
  )


# ---- 8) Keep only fields used by the app ------------------------------------
kommuner_clean <- kommuner |>
  keep_with_geometry(c("kommunkod", "kommunnamn", "kommungrupp_id", "kommungrupp_namn"))

kommungrupper_clean <- kommungrupper |>
  keep_with_geometry(c("kommungrupp_id", "kommungrupp_namn", "kommuner"))

lan_clean <- lan |>
  keep_with_geometry(c("lanskod", "lansnamn"))


# ---- 9) Write bundle ----------------------------------------------------------
# Overwrite each layer in-place (safer when file already exists).
st_write(
  lan_clean,
  out_gpkg,
  layer = "lan",
  delete_layer = TRUE,
  quiet = TRUE
)
st_write(
  kommuner_clean,
  out_gpkg,
  layer = "kommuner",
  delete_layer = TRUE,
  quiet = TRUE
)
st_write(
  kommungrupper_clean,
  out_gpkg,
  layer = "kommungrupper",
  delete_layer = TRUE,
  quiet = TRUE
)

message("Wrote: ", out_gpkg)
