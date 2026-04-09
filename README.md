# energiplanering_dalarna

GIS workspace for building map products from project data.

## Purpose
This repository is organized to keep source data, processing steps, map projects, and exported outputs separated and reproducible.

## Folder structure
- `data/raw/` original files exactly as received (read-only in practice)
- `data/external/` third-party datasets
- `data/interim/` temporary/cleaning outputs
- `data/processed/` analysis-ready GIS layers
- `data/reference/` code lists, lookup tables, metadata tables
- `maps/qgis_projects/` `.qgz/.qgs` project files
- `maps/styles/` QGIS style files (`.qml`, `.sld`)
- `maps/layouts/` print/layout templates
- `maps/exports/` final map exports for delivery
- `docs/inputs/` source PDFs, briefs, requirements
- `docs/reports/` reports and notes
- `docs/slides/` PowerPoint and presentation material
- `scripts/` R/Python/SQL/PowerShell processing scripts
- `config/` project settings and environment templates
- `notebooks/` exploration notebooks (non-production)
- `logs/` run logs
- `apps/` interactive apps (for example Streamlit dashboards)

## Working rules
1. Never edit files inside `data/raw/`.
2. All transformations should happen through scripts in `scripts/`.
3. Save QGIS projects in `maps/qgis_projects/` and keep paths relative.
4. Export map deliverables to `maps/exports/`.
5. Keep large binary source files in Git LFS.

## Naming convention
Use lowercase and underscores:
- layers: `theme_area_version.ext` (example: `grid_capacity_dalarna_v01.gpkg`)
- maps: `map_topic_area_yyyymmdd.ext` (example: `map_wind_sites_dalarna_20260223.pdf`)
- scripts: `NN_step_description.ext` (example: `01_import_source_data.R`)

## Suggested workflow
1. Place incoming files in `data/raw/` or `docs/inputs/`.
2. Run scripts to produce `data/interim/` and `data/processed/`.
3. Build map projects in `maps/qgis_projects/` using processed layers.
4. Export final outputs to `maps/exports/`.
5. Commit script and config changes with each data-processing milestone.

## Next steps
- Add your first processing script in `scripts/`.
- Add a QGIS project file to `maps/qgis_projects/`.
- Track large GIS/doc binaries with Git LFS (see `.gitattributes`).

## Apps (Streamlit)
Use the project virtual environment to avoid PATH issues.

### Python dependencies (once per environment)
```bash
.\.venv\Scripts\python.exe -m pip install streamlit streamlit-folium geopandas folium psycopg2-binary
```

### 1) Text Analysis App (Hem x kommun)
Regenerate the app artifact bundle, then launch the app:

```bash
python scripts/build_hem_kommun_network.py
.\.venv\Scripts\python.exe -m streamlit run apps/hem_kommun_app.py
```

Deploy note:
- For Streamlit Cloud, keep a root `requirements.txt`.
- The app expects committed artifacts in `data/interim/hem_kommun_network/`.
- Recommended Streamlit Cloud settings:
  - Branch: `main`
  - Main file path: `streamlit_app.py` (or `main.py`)
  - Advanced setting: clear cache and redeploy if an old main file was configured.

### 2) Geodata App (Landskapstyper i Dalarna)
Launch directly:

```bash
.\.venv\Scripts\python.exe -m streamlit run app.py
```

Optional: build compact Cloud bundles (fewer files, faster/more robust layer loading):

```bash
.\.venv\Scripts\python.exe scripts/10_build_streamlit_cloud_bundles.py
```

This writes:
- `data/cloud/background_layers.gpkg` (layer: `lan_boundary`)
- `data/cloud/lst_layers.gpkg` (layers: `landskapstyp`, `landskapskaraktar`, `rorligt_friluftsliv`, `utbyggnad_vindkraft`, `nature_reserve`, `kulturmiljovard`)

`app.py` will auto-prefer these bundles if present.

#### Byt naturvårdsområden till Lantmäteriet Topografi 50
Om du vill skriva om lagret `nature_reserve` i `data/cloud/lst_layers.gpkg` med Lantmäteriets Topografi 50
(`naturvard_ln20.gpkg`, layer `skyddadnatur`), kör:

```bash
.\.venv\Scripts\python.exe scripts/13_update_nature_reserve_from_topografi50.py
```

Som standard letar scriptet efter `C:\gislab\data\dataraw\topologi50\naturvard_ln20.gpkg` och använder hela lagret `skyddadnatur` utan klippning.
Lägg till `--clip-to-admin` om du senare vill klippa till länsgränsen.

Optional: add a compact raster overlay layer (for example boreal density TIFF):

```bash
.\.venv\Scripts\python.exe scripts/11_prepare_raster_overlay.py --source-tif "C:\path\to\tathetsanalys_3000m_procent.tif" --max-width 1800 --max-height 1800 --clip-admin-layer lan --crop-to-mask-bbox --color-ramp turbo --ramp-min 1 --ramp-max 94 --opacity 1.0
```

This writes:
- `data/cloud/tathetsanalys_3000m_procent_light.png`
- `data/cloud/tathetsanalys_3000m_procent_values.png`
- `data/cloud/tathetsanalys_3000m_procent.overlay.json`

Then in app sidebar:
- enable `Tathetsanalys boreal region (raster)`
- optionally enable `Filtrera alla punktlager med skoglig värdekärna` and use the slider range.
- Overlay opacity follows class value: `1 -> 0%`, `2 -> 2%`, ..., `94 -> 94%`.

### 3) Layer Review App (8 lager)
Explore aggregated and raw points across:
`kommun`, `kommungrupp`, `landskapstyp`, `landskapskaraktar`, `kulturmiljo`, `friluftsliv`, `vindkraft`, `naturvarden`.

```bash
.\.venv\Scripts\python.exe -m streamlit run apps/layer_review_app.py
```

If you see `ModuleNotFoundError` (for example `streamlit_folium` or `geopandas`), run the dependency install command above in the same environment.

## Hemvist (QI) Logic
Definition in app:
- `Hemvist (QI)` filters by respondent home (`Q1`), not by point location.
- `Koordinatlage (spatialt)` filters by where points are located.

Expected behavior:
- If `Arbetsomrade = kommun/kommungrupp` and `Filtergrund = Hemvist (QI)`, points can appear anywhere in the county.
- The selected area controls who is included (home), not where their points are.

### Future Home Layer (recommended)
To make Hemvist robust and independent from raw Novus group coding, build a dedicated respondent-home layer/table with one row per `respid`:
- `respid`
- `home_kommunkod` (from `Q1`)
- `home_kommungrupp_id_current` (derived from current kommun mapping)
- `home_kommungrupp_namn_current`
- optional: `record` (for audit/debug)

Join this once to point layers (`plats_1`, `plats_2`, sensitive/non-sensitive).  
Then app filtering can use only these fields for Hemvist.

Suggested build order:
1. Build/update admin mapping (`kommuner` + `kommungrupper`) in `data/cloud/admin_boundaries.gpkg`.
2. Build/update respondent home table from Novus (`Q1` -> `home_kommunkod` -> current group id/name).
3. Attach home fields to each exported point row in `novus_locked_points.gpkg`.
4. Deploy/reboot Streamlit app.

### Rebuild Impact
If you introduce the dedicated home layer:
- Rebuild needed: point export bundle (`novus_locked_points.gpkg`) or equivalent DB view that feeds the app.
- Sometimes needed: `admin_boundaries.gpkg` only if kommun/kommungrupp definitions changed.
- Not needed: LST polygon bundles (`lst_layers.gpkg`), boundary bundles (`background_layers.gpkg`) unless their own source data changed.
- App code changes are small: switch Hemvist fields to the new `*_current` columns.

## Data policy
To support Streamlit Cloud, this repo commits a small app-ready bundle:
- Commit: `data/interim/hem_kommun_network/**`
- Do not commit: large/private data (for example `data/interim/novus/**`, `novus_full_dataframe.csv`, and other raw/heavy files)

Why:
- Streamlit Cloud needs the prebuilt hem_kommun_network artifacts at runtime.
- novus and other heavy/private datasets should stay out of git for privacy and repo size.

Update workflow for app artifacts:
```bash
python scripts/build_hem_kommun_network.py
git add data/interim/hem_kommun_network
git commit -m "Update hem_kommun_network artifacts"
git push
```

## Arbetsplan För Appförändringar (Mars 2026)
Överenskommet arbetssätt:
1. Vi diskuterar en förändring i taget innan implementation.
2. Vi implementerar endast en avgränsad förändring per steg.
3. När steget fungerar verifierat, gör vi en separat commit.
4. Sedan går vi vidare till nästa förändring, så det är enkelt att backa ett steg.

Förändringar att ta en och en:
1. Byta lagernamn till mer läsbara namn (fortfarande nära originalkällor):
- `Landskapstyper.lst`
- `Landskapskaraktärsområden.lst`
- `Rörligt friluftsliv.lst`
- `Utbyggnad av vindkraft.lst`
- `Kulturmiljövård.lst`
- Naturvårdsområden: använd Lantmäteriet Topografi 50 (`naturvard_ln20`) som permanent källa
2. Använda svenska tecken i UI (exempel):
- `Tanda` -> `Tända`
- `Analyslage` -> `Analysläge`
- `Energiomstallning i Dalarna` -> `Energiomställning i Dalarna`
- `kansliga punkter` -> `känsliga punkter`
3. Analysutveckling (måste problematiseras innan implementation):
- Utöka rulllistan `Arbetsomrade` med `landskapstyp` och `landskapskaraktar`.
- Gör dessa lager valbara för analys så användaren kan välja t.ex. `lan + landskapstyp + punkt 1`.
- Visa antal punkter inom vald landskapstyp/karaktär, uppdelat enligt överenskommen indelning.
- Denna punkt ska designas och riskbedömas innan kodändring.



