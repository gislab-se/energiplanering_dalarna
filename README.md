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
Run analysis export first, then launch the app:

```bash
Rscript scripts/hem_kommun_network.R
.\.venv\Scripts\python.exe -m streamlit run apps/hem_kommun_app.py
```

Deploy note:
- For Streamlit Cloud, keep a root `requirements.txt`.
- If `data/interim/hem_kommun_network/response_tokens.csv` is not available in deploy, the app now supports uploading `response_tokens.csv` (and optional `word_frequency.csv`) from the UI.

### 2) Geodata App (Landskapstyper i Dalarna)
Launch directly:

```bash
.\.venv\Scripts\python.exe -m streamlit run app.py
```

If you see `ModuleNotFoundError` (for example `streamlit_folium` or `geopandas`), run the dependency install command above in the same environment.

## Data policy
All datasets and source documents are local-only and should not be pushed to GitHub. Keep only structure (.gitkeep), scripts, config, and documentation text under version control.



