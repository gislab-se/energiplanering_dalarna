# Privacy Pipeline Notes

## Requirement summary
- `resp_id` is sensitive and must not be present in exported/tracked artifacts.
- Downstream pipeline outputs must use `pid` only.
- `private.resp_id_map` is the only place where `resp_id` may exist.

## Implemented SQL assets
- `scripts/sql/09_privacy_pipeline.sql`
- `scripts/sql/09_privacy_pipeline_qa.sql`

## What the pipeline builds
- `private.resp_id_map`:
  - `resp_id` -> stable UUID `pid`
  - inserts only new `resp_id` values
- `interim.respondent_points`:
  - canonical long-format points with `pid`
  - `point_type` in `plats_1`, `plats_2`, `plats_sensitive`
  - no `resp_id` column
- `interim.v_points_with_landscape`:
  - inherits polygon IDs/names for kommun, kommungrupp, landskapstyp, landskapskaraktar
  - enables app filtering on IDs without app-side spatial joins
- Aggregated one-point-per-polygon layers:
  - `interim.agg_points_kommun`
  - `interim.agg_points_kommungrupp`
  - `interim.agg_points_landskapstyp`
  - `interim.agg_points_landskapskaraktar`

## Access and export policy
- `private` schema is restricted with:
  - `REVOKE ALL ON SCHEMA private FROM PUBLIC`
  - `REVOKE ALL ON ALL TABLES IN SCHEMA private FROM PUBLIC`
- Never export or commit data from `private.resp_id_map`.
- Any Git-tracked or Streamlit-facing artifacts must contain only `pid`, never `resp_id`.

## Run order
1. Execute `scripts/sql/09_privacy_pipeline.sql`.
2. Execute `scripts/sql/09_privacy_pipeline_qa.sql`.
3. If any QA leakage check returns rows, remove `resp_id` exposure before exporting artifacts.
