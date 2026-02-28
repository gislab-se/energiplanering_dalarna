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

# =========================
# 2. LÄS + FILTRERA DESO
# =========================

deso <- st_read(deso_gpkg, layer = deso_layer, quiet = TRUE) |>
  mutate(lanskod = as.character(lanskod)) |>
  filter(lanskod == lan_keep) |>
  st_make_valid()

# =========================
# 3. DISSOLVE TILL KOMMUNER
# =========================

kommuner <- deso |>
  mutate(kommunkod = as.character(kommunkod)) |>
  group_by(kommunkod, kommunnamn) |>
  summarise(geometry = st_union(sp_geometry), .groups = "drop") |>
  st_cast("MULTIPOLYGON") |>
  st_make_valid()

# =========================
# 4. LÄS KOMMUNGRUPP (Q1 = kommunkod)
# =========================
library(dplyr)
library(sf)
library(readr)

# 1) Läs lookup från csv: Q1 = kommunkod, Kommungrupp = kommungrupp_id
kg_lookup <- read_csv(kg_csv, show_col_types = FALSE) |>
  transmute(
    kommunkod = trimws(as.character(Q1)),
    kommungrupp_id = as.integer(Kommungrupp)
  ) |>
  distinct()

# 2) Lägg på kommungrupp_id på kommuner (se till att kommunkod är text)
kommuner <- kommuner |>
  mutate(kommunkod = trimws(as.character(kommunkod))) |>
  select(-any_of(c("kommungrupp_id", "kommungrupp_namn"))) |>  # droppa ev gamla kolumner
  left_join(kg_lookup, by = "kommunkod")

# 3) Skapa kommungrupp_namn automatiskt per grupp-id
kg_names_auto <- kommuner |>
  st_drop_geometry() |>
  filter(!is.na(kommungrupp_id)) |>
  group_by(kommungrupp_id) |>
  summarise(
    kommungrupp_namn = paste(sort(unique(kommunnamn)), collapse = ", "),
    .groups = "drop"
  )

# 4) Lägg på kommungrupp_namn (nu utan .x/.y-trassel)
kommuner <- kommuner |>
  left_join(kg_names_auto, by = "kommungrupp_id")

mapview::mapview(kommungrupper)
# =========================
# 5. LÄGG TILL KOMMUNGRUPP-NAMN
# =========================

kommungrupper <- kommuner |>
  filter(!is.na(kommungrupp_id)) |>
  group_by(kommungrupp_id, kommungrupp_namn) |>
  summarise(geometry = st_union(geometry), .groups = "drop") |>
  st_cast("MULTIPOLYGON") |>
  st_make_valid()

# =========================
# 7. DISSOLVE TILL LÄN
# =========================

lan <- deso |>
  group_by(lanskod) |>
  summarise(geometry = st_union(sp_geometry), .groups = "drop") |>
  st_cast("MULTIPOLYGON") |>
  st_make_valid()

# Lägg till lansnamn
lan_lookup <- tribble(
  ~lanskod, ~lansnamn,
  "20", "Dalarnas län"
)

lan <- lan |>
  left_join(lan_lookup, by = "lanskod")

kommuner |> st_drop_geometry() |> count(kommunkod, kommunnamn, kommungrupp_id) |> arrange(desc(n))

kg_names_auto |> arrange(kommungrupp_id)
# =========================
# 8. SPARA SOM GPKG (3 lager)
# =========================

st_write(lan,           out_gpkg, layer = "lan",           delete_layer = TRUE, quiet = TRUE)
st_write(kommuner,      out_gpkg, layer = "kommuner",      delete_layer = TRUE, quiet = TRUE)
st_write(kommungrupper, out_gpkg, layer = "kommungrupper", delete_layer = TRUE, quiet = TRUE)

# =========================
# 9. REN MAPVIEW-VISNING
# =========================
library(mapview)
mapview(
  kommuner,
  popup = c("kommunnamn")
) +
  mapview(
    kommungrupper,
    popup = c("kommungrupp_namn")
  ) +
  mapview(
    lan,
    popup = c("lansnamn")
  )
