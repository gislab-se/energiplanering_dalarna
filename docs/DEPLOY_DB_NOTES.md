# Deploy Notes: Option 1 (Database-backed on Streamlit Cloud)

## Goal
Run the app in Streamlit Cloud using PostGIS as primary data source.

## Required Streamlit secrets
Add these in `App settings -> Secrets`:

```toml
PGHOST = "..."
PGPORT = "5432"
PGDATABASE = "..."
PGUSER = "..."
PGPASSWORD = "..."
```

## Network requirement
Streamlit Cloud must be able to reach the database host/port.

Options:
- Publicly reachable Postgres host with strict IP allow-list and TLS.
- Managed DB endpoint (recommended).
- Secure tunnel/proxy endpoint dedicated for app traffic.

If network is blocked, app cannot load DB-backed layers even if secrets are set.

## Runtime behavior in this repo
- If DB secrets are missing or DB is unreachable, app now degrades gracefully.
- It shows sidebar warnings and continues with available local cloud bundle data.

## Operational checklist
1. Set secrets listed above.
2. Confirm DB user has `SELECT` on required views/tables.
3. Verify host and port are reachable from Streamlit Cloud.
4. Reboot app after updating secrets.
5. Check logs for missing view/table names.

## Minimal SQL permissions (example)
- `adm_indelning.v_dalarna_lan_4326`
- `adm_indelning.v_dalarna_kommuner_4326`
- `adm_indelning.v_dalarna_kommungrupper_4326`
- `novus` point views used by app

## Current fallback (Option 2)
This repo now also supports a lightweight file bundle in `data/cloud/`:
- `novus_locked_points.gpkg`
- `lst_layers.gpkg`

That keeps deployment functional without DB, but with fewer layers.
