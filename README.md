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

### 3) Layer Review App (8 lager)
Explore aggregated and raw points across:
`kommun`, `kommungrupp`, `landskapstyp`, `landskapskaraktar`, `kulturmiljo`, `friluftsliv`, `vindkraft`, `naturvarden`.

```bash
.\.venv\Scripts\python.exe -m streamlit run apps/layer_review_app.py
```

If you see `ModuleNotFoundError` (for example `streamlit_folium` or `geopandas`), run the dependency install command above in the same environment.

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



