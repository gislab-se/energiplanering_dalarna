# Startup Tomorrow

## 1) Quick Start
- Open terminal in `C:\gislab\energiplanering_dalarna`
- Activate environment (Conda/py) used for Streamlit
- Run app:
  - `py -m streamlit run app.py`

## 2) Database Prep (if needed)
- Ensure Postgres is running on `localhost:5432`
- Load/update DeSO + admin views:
  - `$env:PIPELINE_ENV_PATH='C:/gislab/databas/generell_databas_setup/.env'`
  - `Rscript C:\gislab\databas\script\08_load_deso_adm_indelning.R`

## 3) Validate Layers in App
- Check these toggles/layers:
  - Landskapstyper
  - Kommunpolygoner
  - Kommungrupper
  - (optional) Landskapskaraktar

## 4) Novus / Point Analysis Next
- Confirm target view strategy:
  - `Plats 1`
  - `Plats 2`
  - `Heimat` (definition to be finalized)
- Build/adjust SQL views as needed under schema `novus`

## 5) QGIS Checks
- Open PostGIS connection (`speedlocal`)
- Verify `adm_indelning` views:
  - `v_dalarna_lan_3006`
  - `v_dalarna_kommuner_3006`
  - `v_dalarna_kommungrupper_3006`
- Verify `novus` views for points

## 6) Deliverables to Continue
- Map-ready app with kommun filters/groups
- Optional narrative panel (northwest vs southeast framing)
- Optional export tables for workshop/dialog use

## 7) Known Notes
- PowerShell in this environment prints an `OutputEncoding` warning; safe to ignore.
- Use `py -m ...` instead of `python -m ...` on this machine.
