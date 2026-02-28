from __future__ import annotations

import os
from pathlib import Path

import folium
import geopandas as gpd
import pandas as pd
import psycopg2
import streamlit as st
from streamlit_folium import st_folium


st.set_page_config(page_title="Layer Review", layout="wide")
st.title("Layer Review: 8 landskaps/omradeslager")


def _parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _db_settings() -> dict[str, str]:
    env_path = Path(os.getenv("PIPELINE_ENV_PATH", "C:/gislab/databas/generell_databas_setup/.env"))
    vals = _parse_env_file(env_path)
    out = {
        "host": os.getenv("PGHOST", vals.get("PGHOST", "localhost")),
        "port": os.getenv("PGPORT", vals.get("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", vals.get("PGDATABASE", "")),
        "user": os.getenv("PGUSER", vals.get("PGUSER", "")),
        "password": os.getenv("PGPASSWORD", vals.get("PGPASSWORD", "")),
    }
    missing = [k for k in ("dbname", "user", "password") if not out[k]]
    if missing:
        raise RuntimeError(f"Missing DB settings: {', '.join(missing)}")
    return out


def _connect():
    cfg = _db_settings()
    return psycopg2.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )


def _table_exists(con, schema: str, table: str) -> bool:
    q = """
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = %s
        AND table_name = %s
    )
    """
    with con.cursor() as cur:
        cur.execute(q, (schema, table))
        return bool(cur.fetchone()[0])


LAYER_CFG = {
    "kommun": {
        "label": "Kommun",
        "poly_table": "adm_indelning.v_dalarna_kommuner_3006",
        "id_col": "id",
        "name_col": "kommunnamn",
        "vp_id_col": "kommun_id",
        "vp_name_col": "kommun_namn",
        "agg_view": "interim.agg_points_kommun",
    },
    "kommungrupp": {
        "label": "Kommungrupp",
        "poly_table": "adm_indelning.v_dalarna_kommungrupper_3006",
        "id_col": "kommungrupp_id",
        "name_col": "kommungrupp_namn",
        "vp_id_col": "kommungrupp_id",
        "vp_name_col": "kommungrupp_namn",
        "agg_view": "interim.agg_points_kommungrupp",
    },
    "landskapstyp": {
        "label": "Landskapstyp",
        "poly_table": "landskap.lstw_landskapstyper",
        "id_col": "id",
        "name_col": "namn",
        "vp_id_col": "landskapstyp_id",
        "vp_name_col": "landskapstyp_namn",
        "agg_view": "interim.agg_points_landskapstyp",
    },
    "landskapskaraktar": {
        "label": "Landskapskaraktar",
        "poly_table": "landskap.lstw_landskapskaraktarsomraden",
        "id_col": "id",
        "name_col": "lk_namn",
        "vp_id_col": "landskapskaraktar_id",
        "vp_name_col": "landskapskaraktar_namn",
        "agg_view": "interim.agg_points_landskapskaraktar",
    },
    "kulturmiljo": {
        "label": "Kulturmiljo",
        "poly_table": "landskap.raa_ri_kulturmiljovard_mb3kap6",
        "id_col": "objectid",
        "name_col": "namn",
        "vp_id_col": "kulturmiljo_id",
        "vp_name_col": "kulturmiljo_namn",
        "agg_view": "interim.agg_points_kulturmiljo",
    },
    "friluftsliv": {
        "label": "Rorligt friluftsliv",
        "poly_table": "landskap.lst_ri_rorligt_friluftsliv_mb4kap2",
        "id_col": "objectid",
        "name_col": "namn",
        "vp_id_col": "friluftsliv_id",
        "vp_name_col": "friluftsliv_namn",
        "agg_view": "interim.agg_points_friluftsliv",
    },
    "vindkraft": {
        "label": "Vindkraft utbyggnadsomrade",
        "poly_table": "landskap.lstw_regional_analys_vindkraft_juni2024",
        "id_col": "objektid",
        "name_col": "typ_av_omr",
        "vp_id_col": "vindkraft_id",
        "vp_name_col": "vindkraft_namn",
        "agg_view": "interim.agg_points_vindkraft",
    },
    "naturvarden": {
        "label": "Naturvarden",
        "poly_table": "landskap.lstw_pg204_naturvarden_kanda_av_lst_dalarna",
        "id_col": "objectid",
        "name_col": "objektnamn",
        "vp_id_col": "naturvarden_id",
        "vp_name_col": "naturvarden_namn",
        "agg_view": "interim.agg_points_naturvarden",
    },
}


def _points_where(point_type: str) -> str:
    if point_type == "all":
        return "TRUE"
    return f"p.point_type = '{point_type}'"


@st.cache_data(ttl=120, show_spinner=False)
def load_agg(layer_key: str, point_type: str) -> pd.DataFrame:
    cfg = LAYER_CFG[layer_key]
    con = _connect()
    try:
        agg_schema, agg_table = cfg["agg_view"].split(".", 1)
        if point_type == "all" and _table_exists(con, agg_schema, agg_table):
            q = f"""
            SELECT
              *,
              ST_AsText(ST_Transform(geom_point, 4326)) AS geom_wkt
            FROM {cfg["agg_view"]}
            """
            return pd.read_sql(q, con)

        q = f"""
        WITH c AS (
          SELECT
            l.{cfg["id_col"]} AS polygon_id,
            COUNT(*) AS total_points,
            COUNT(*) FILTER (WHERE p.point_type = 'plats_1') AS count_plats_1,
            COUNT(*) FILTER (WHERE p.point_type = 'plats_2') AS count_plats_2,
            COUNT(*) FILTER (WHERE p.point_type = 'plats_sensitive') AS count_sensitive
          FROM interim.respondent_points p
          JOIN {cfg["poly_table"]} l
            ON ST_Intersects(p.geom, l.geom)
          WHERE {_points_where(point_type)}
          GROUP BY l.{cfg["id_col"]}
        )
        SELECT
          l.{cfg["id_col"]} AS polygon_id,
          COALESCE(NULLIF(l.{cfg["name_col"]}::text, ''), l.{cfg["id_col"]}::text) AS polygon_name,
          ST_AsText(ST_Transform(ST_PointOnSurface(l.geom), 4326)) AS geom_wkt,
          COALESCE(c.total_points, 0) AS total_points,
          COALESCE(c.count_plats_1, 0) AS count_plats_1,
          COALESCE(c.count_plats_2, 0) AS count_plats_2,
          COALESCE(c.count_sensitive, 0) AS count_sensitive
        FROM {cfg["poly_table"]} l
        LEFT JOIN c ON c.polygon_id = l.{cfg["id_col"]}
        """
        return pd.read_sql(q, con)
    finally:
        con.close()


@st.cache_data(ttl=120, show_spinner=False)
def load_raw(layer_key: str, point_type: str) -> gpd.GeoDataFrame:
    cfg = LAYER_CFG[layer_key]
    con = _connect()
    try:
        has_view = _table_exists(con, "interim", "v_points_with_landscape")
        if has_view and cfg["vp_id_col"]:
            q = f"""
            SELECT
              point_id,
              pid::text AS pid,
              point_type,
              sensitive,
              {cfg["vp_id_col"]} AS polygon_id,
              {cfg["vp_name_col"]} AS polygon_name,
              ST_Transform(geom, 4326) AS geom
            FROM interim.v_points_with_landscape
            WHERE {_points_where(point_type)}
            """
            return gpd.read_postgis(q, con, geom_col="geom")

        q = f"""
        SELECT
          p.point_id,
          p.pid::text AS pid,
          p.point_type,
          p.sensitive,
          l.{cfg["id_col"]} AS polygon_id,
          COALESCE(NULLIF(l.{cfg["name_col"]}::text, ''), l.{cfg["id_col"]}::text) AS polygon_name,
          ST_Transform(p.geom, 4326) AS geom
        FROM interim.respondent_points p
        LEFT JOIN {cfg["poly_table"]} l
          ON ST_Intersects(p.geom, l.geom)
        WHERE {_points_where(point_type)}
        """
        return gpd.read_postgis(q, con, geom_col="geom")
    finally:
        con.close()


def _parse_point_wkt(wkt: str) -> tuple[float, float] | None:
    if not isinstance(wkt, str) or not wkt.startswith("POINT("):
        return None
    core = wkt.replace("POINT(", "").replace(")", "")
    parts = core.split()
    if len(parts) != 2:
        return None
    x, y = float(parts[0]), float(parts[1])
    return y, x


with st.sidebar:
    layer_key = st.selectbox("Lager", list(LAYER_CFG.keys()), format_func=lambda k: LAYER_CFG[k]["label"])
    point_type = st.selectbox("Point type", ["all", "plats_1", "plats_2", "plats_sensitive"])
    show_agg = st.checkbox("Visa aggregerade punkter", value=True)
    show_raw = st.checkbox("Visa raw punkter", value=False)
    min_total = st.slider("Min total_points (agg)", min_value=0, max_value=200, value=1, step=1)

cfg = LAYER_CFG[layer_key]
st.caption(
    f"Lager: {cfg['label']} | Polygon source: `{cfg['poly_table']}` | Agg view preferred: `{cfg['agg_view']}`"
)

agg_df = pd.DataFrame()
raw_gdf = gpd.GeoDataFrame()
errors: list[str] = []

try:
    agg_df = load_agg(layer_key, point_type)
except Exception as exc:
    errors.append(f"Agg load failed: {exc}")

if show_raw:
    try:
        raw_gdf = load_raw(layer_key, point_type)
    except Exception as exc:
        errors.append(f"Raw load failed: {exc}")

if errors:
    for msg in errors:
        st.warning(msg)
    st.info(
        "Kontrollera att `interim.respondent_points` finns och att privacy-pipeline SQL är körd. "
        "Om agg-vyer saknas används DB-fallback."
    )

if not agg_df.empty:
    agg_df = agg_df[agg_df["total_points"] >= min_total].copy()
    agg_df = agg_df.sort_values("total_points", ascending=False)

left, right = st.columns([2, 1])
with right:
    st.subheader("QA")
    if not agg_df.empty:
        st.metric("Polygoner (visas)", int(len(agg_df)))
        st.metric("Sum total_points", int(agg_df["total_points"].sum()))
        st.dataframe(
            agg_df[["polygon_name", "total_points", "count_plats_1", "count_plats_2", "count_sensitive"]].head(20),
            use_container_width=True,
            hide_index=True,
        )
    if show_raw and not raw_gdf.empty:
        st.metric("Raw points (visas)", int(len(raw_gdf)))
        unmatched = int(raw_gdf["polygon_id"].isna().sum()) if "polygon_id" in raw_gdf.columns else 0
        st.metric("Raw points utan polygonmatch", unmatched)

with left:
    st.subheader("Karta")
    m = folium.Map(location=[60.5, 14.5], zoom_start=7, tiles="CartoDB positron")

    if show_agg and not agg_df.empty:
        max_total = max(1, int(agg_df["total_points"].max()))
        for _, r in agg_df.iterrows():
            coords = _parse_point_wkt(r.get("geom_wkt"))
            if coords is None:
                continue
            radius = 4 + 18 * ((float(r["total_points"]) / max_total) ** 0.5)
            folium.CircleMarker(
                location=coords,
                radius=radius,
                color="#1f2937",
                weight=1,
                fill=True,
                fill_color="#3b82f6",
                fill_opacity=0.75,
                tooltip=f"{r.get('polygon_name', '(saknas)')}: {int(r['total_points'])}",
                popup=(
                    f"{r.get('polygon_name', '(saknas)')}<br>"
                    f"total={int(r['total_points'])}<br>"
                    f"plats_1={int(r['count_plats_1'])}, plats_2={int(r['count_plats_2'])}, sensitive={int(r['count_sensitive'])}"
                ),
            ).add_to(m)

    if show_raw and not raw_gdf.empty:
        for _, r in raw_gdf.head(20000).iterrows():
            g = r.geometry
            if g is None or g.is_empty:
                continue
            color = "#ef4444" if r.get("point_type") == "plats_sensitive" else "#111827"
            folium.CircleMarker(
                location=[g.y, g.x],
                radius=2,
                color=color,
                weight=1,
                fill=True,
                fill_color=color,
                fill_opacity=0.8,
                tooltip=f"{r.get('point_type', '')} | {r.get('polygon_name', '(saknas)')}",
            ).add_to(m)

    st_folium(m, width=None, height=860)
