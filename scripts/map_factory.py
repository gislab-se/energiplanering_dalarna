from __future__ import annotations

import os
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, Tuple

import folium
import geopandas as gpd


SLIDE_PALETTE = [
    "#1f77b4",
    "#d62728",
    "#2ca02c",
    "#9467bd",
    "#ff7f0e",
    "#17becf",
    "#8c564b",
    "#e377c2",
]

GROUP_PALETTE = {
    1: "#4e79a7",
    2: "#f28e2b",
    3: "#59a14f",
    4: "#e15759",
    5: "#76b7b2",
    6: "#af7aa1",
}


def _first_existing(paths: Iterable[Path]) -> Path:
    for p in paths:
        if p.exists():
            return p
    raise FileNotFoundError("Could not find expected shapefile path")


def locate_layers(repo_root: Path) -> Tuple[Path, Path]:
    base = (
        repo_root
        / "data"
        / "raw"
        / "unpacked"
        / "Geodata-20260223T113354Z-1-001"
        / "Geodata"
        / "Landskapstyper gis"
    )
    sty = _first_existing(base.glob("*Landskapstyper*.shp"))
    kar = _first_existing(base.glob("*Landskapskar*.shp"))
    return sty, kar


def load_layers(repo_root: Path) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    sty_path, kar_path = locate_layers(repo_root)
    sty = gpd.read_file(sty_path).to_crs(4326)
    kar = gpd.read_file(kar_path).to_crs(4326)
    return sty, kar


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


def load_admin_layers_from_db() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    import psycopg2

    cfg = _db_settings()
    con = psycopg2.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )
    try:
        lan = gpd.read_postgis("SELECT id, lanskod, lansnamn, geom FROM adm_indelning.v_dalarna_lan_4326", con, geom_col="geom")
        kommuner = gpd.read_postgis("SELECT id, kommunkod, kommunnamn, geom FROM adm_indelning.v_dalarna_kommuner_4326", con, geom_col="geom")
        grupper = gpd.read_postgis(
            "SELECT id, kommungrupp_id, kommungrupp_namn, kommuner, geom FROM adm_indelning.v_dalarna_kommungrupper_4326",
            con,
            geom_col="geom",
        )
    finally:
        con.close()

    return lan.to_crs(4326), kommuner.to_crs(4326), grupper.to_crs(4326)


def non_geometry_columns(gdf: gpd.GeoDataFrame) -> list[str]:
    return [c for c in gdf.columns if c != gdf.geometry.name]


def choose_default_field(gdf: gpd.GeoDataFrame) -> str:
    cols = non_geometry_columns(gdf)
    priorities = ("landskap", "typ", "ltyp", "namn", "name")
    for p in priorities:
        for c in cols:
            if p in c.lower():
                return c
    return cols[0]


def _palette_map(values: Iterable[str]) -> Dict[str, str]:
    unique = sorted({str(v) for v in values})
    return {v: SLIDE_PALETTE[i % len(SLIDE_PALETTE)] for i, v in enumerate(unique)}


def _canonical(value: str) -> str:
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def _normalize_landscape_type(value: str) -> str:
    v = str(value).strip()
    k = _canonical(v)
    if k in {"bergkullandskap", "bergkullslandskap"}:
        return "Bergkullslandskap"
    if k in {"fjallanskap", "fjallandskap"}:
        return "Fjällandskap"
    return v[:1].upper() + v[1:] if v else v


def build_map(
    sty: gpd.GeoDataFrame,
    kar: gpd.GeoDataFrame,
    sty_field: str,
    kar_field: str,
    show_kar: bool = True,
    dalarna_lan: gpd.GeoDataFrame | None = None,
    kommuner: gpd.GeoDataFrame | None = None,
    kommungrupper: gpd.GeoDataFrame | None = None,
    show_kommuner: bool = False,
    show_kommungrupper: bool = False,
) -> folium.Map:
    sty_vals = sty[sty_field].fillna("(saknas)").astype(str).map(_normalize_landscape_type)
    kar_vals = kar[kar_field].fillna("(saknas)").astype(str)
    sty = sty.assign(_sty_val=sty_vals, _sty_popup=sty_vals)
    kar = kar.assign(_kar_val=kar_vals, _kar_popup=kar_vals)

    colors = _palette_map(sty["_sty_val"])
    centroid = sty.unary_union.centroid
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=8, tiles="CartoDB positron")

    def sty_style(feature: dict) -> dict:
        val = str(feature["properties"].get("_sty_val", "(saknas)"))
        return {"fillColor": colors.get(val, "#cccccc"), "color": "#444444", "weight": 1.4, "fillOpacity": 0.6, "opacity": 1}

    folium.GeoJson(sty, name="Landskapstyper", style_function=sty_style, popup=folium.GeoJsonPopup(fields=["_sty_popup"], labels=False)).add_to(m)

    if show_kar:
        folium.GeoJson(
            kar,
            name="Landskapskaraktar",
            style_function=lambda _: {"fillOpacity": 0, "color": "#2f2f2f", "weight": 0.9, "opacity": 0.45},
            popup=folium.GeoJsonPopup(fields=["_kar_popup"], labels=False),
        ).add_to(m)

    if dalarna_lan is not None and len(dalarna_lan) > 0:
        folium.GeoJson(
            dalarna_lan,
            name="Dalarna lan",
            style_function=lambda _: {"fillOpacity": 0, "color": "#111827", "weight": 2.2, "opacity": 1},
            popup=folium.GeoJsonPopup(fields=["lansnamn"], labels=False),
        ).add_to(m)

    if show_kommuner and kommuner is not None and len(kommuner) > 0:
        folium.GeoJson(
            kommuner,
            name="Kommunpolygoner",
            style_function=lambda _: {"fillOpacity": 0, "color": "#4b5563", "weight": 1.1, "opacity": 0.9},
            popup=folium.GeoJsonPopup(fields=["kommunnamn", "kommunkod"], aliases=["Kommun", "Kod"], labels=True),
        ).add_to(m)

    if show_kommungrupper and kommungrupper is not None and len(kommungrupper) > 0:
        kg = kommungrupper.copy()
        kg["_grp_color"] = kg["kommungrupp_id"].map(GROUP_PALETTE).fillna("#9ca3af")

        def grp_style(feature: dict) -> dict:
            return {
                "fillColor": feature["properties"].get("_grp_color", "#9ca3af"),
                "fillOpacity": 0.2,
                "color": "#374151",
                "weight": 1.2,
                "opacity": 0.9,
            }

        folium.GeoJson(
            kg,
            name="Kommungrupper",
            style_function=grp_style,
            popup=folium.GeoJsonPopup(fields=["kommungrupp_namn", "kommuner"], aliases=["Grupp", "Kommuner"], labels=True),
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    legend_items = "".join(
        f"<div><span style='display:inline-block;width:12px;height:12px;background:{c};margin-right:6px;border:1px solid #666;'></span>{l}</div>"
        for l, c in colors.items()
    )
    legend_html = (
        "<div style='position: fixed; bottom: 20px; right: 20px; z-index: 9999;"
        " background: white; padding: 10px 12px; border: 1px solid #999; border-radius: 4px;"
        " font-size: 12px; max-height: 260px; overflow-y: auto;'>"
        "<b>Landskapstyper</b>"
        f"{legend_items}"
        "</div>"
    )
    m.get_root().html.add_child(folium.Element(legend_html))
    return m
