library(sf)
library(dplyr)
library(readr)

# ---- ändra här ----
deso_gpkg   <- "C:/Users/henri/data/DeSO_2025.gpkg"
st_layers(deso_gpkg)
deso_layer  <- "DeSO_2025"                       # byt om ditt lager heter annat
lan_keep    <- 20                           # t.ex. Dalarna
kg_csv      <- "C:/gislab/energiplanering_dalarna/data/interim/novus/novus_full_dataframe.csv" # CSV med kolumnerna Q1 och Kommungrupp
out_gpkg    <- "C:/gislab/energiplanering_dalarna/data/cloud/admin_boundaries.gpkg"

# ---- 1) Läs + filtrera DESO ----
deso <- st_read(deso_gpkg, layer = deso_layer, quiet = TRUE) |>
  filter(lanskod == lan_keep) |>
  st_make_valid()

# ---- 2) Dissolve till kommuner (kommunkod + kommunnamn) ----
kommuner <- deso |>
  group_by(kommunkod, kommunnamn) |>
  summarise(geometry = st_union(sp_geometry), .groups = "drop") |>
  st_cast("MULTIPOLYGON") |>
  st_make_valid()

# ---- 3) Läs kommungrupp-lookup (Q1 = kommunkod) ----
# ---- 3) Läs kommungrupp-lookup (Q1 = kommunkod) ----
kg_lookup <- read_csv(kg_csv, show_col_types = FALSE) |>
  transmute(
    kommunkod = as.character(Q1),
    kommungrupp_id = as.integer(Kommungrupp)
  ) |>
  distinct()

# ---- 4) Se till att kommuner också har kommunkod som text ----
kommuner <- kommuner |>
  mutate(kommunkod = as.character(kommunkod)) |>
  left_join(kg_lookup, by = "kommunkod")

# (valfritt men bra) varna om något saknar kommungrupp
missing <- kommuner |> filter(is.na(kommungrupp_id)) |> st_drop_geometry()
if (nrow(missing) > 0) {
  warning("Saknar kommungrupp_id för: ",
          paste(missing$kommunnamn, collapse = ", "))
}

# ---- Lägg till kommungrupp_namn ----
kg_names <- tibble::tribble(
  ~kommungrupp_id, ~kommungrupp_namn,
  1, "Avesta, Hedemora, Säter",
  2, "Falun, Borlänge",
  3, "Malung-Sälen, Älvdalen, Vansbro",
  4, "Mora, Orsa",
  5, "Rättvik, Leksand, Gagnef",
  6, "Smedjebacken, Ludvika"
)

kommuner <- kommuner |>
  left_join(kg_names, by = "kommungrupp_id")

# ---- 5) Dissolve till kommungrupper (kommungrupp_id) ----
kommungrupper <- kommuner |>
  filter(!is.na(kommungrupp_id)) |>
  group_by(kommungrupp_id, kommungrupp_namn) |>
  summarise(
    geometry = st_union(geometry),
    .groups = "drop"
  ) |>
  st_cast("MULTIPOLYGON") |>
  st_make_valid()

# ---- 6) Dissolve till län ----
lan <- deso |>
  group_by(lanskod) |>
  summarise(geometry = st_union(sp_geometry), .groups = "drop") |>
  st_cast("MULTIPOLYGON") |>
  st_make_valid()

lan_lookup <- tibble::tribble(
  ~lanskod, ~lansnamn,
  "20", "Dalarnas län"
)

lan <- lan |>
  mutate(lanskod = as.character(lanskod)) |>
  left_join(lan_lookup, by = "lanskod")

mapview::mapview(kommuner)+
  mapview::mapview(kommungrupper)+
  mapview::mapview(lan)
  

kommuner_clean <- kommuner |>
  select(kommunkod, kommunnamn, geometry)

# ---- 7) Skriv till GeoPackage (3 lager) ----
if (file.exists(out_gpkg)) file.remove(out_gpkg)

st_write(lan,           out_gpkg, layer = "lan",           quiet = TRUE)
st_write(kommuner_clean,      out_gpkg, layer = "kommuner",      append = TRUE, quiet = TRUE)
st_write(kommungrupper, out_gpkg, layer = "kommungrupper", append = TRUE, quiet = TRUE)
