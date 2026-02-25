from __future__ import annotations

import os
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, Tuple

import folium
import geopandas as gpd
import pandas as pd


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


def load_admin_layers_from_db() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
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
        kommuner = gpd.read_postgis("SELECT id, kommunkod, kommunnamn, geom FROM adm_indelning.v_dalarna_kommuner_4326", con, geom_col="geom")
        grupper = gpd.read_postgis(
            "SELECT id, kommungrupp_id, kommungrupp_namn, kommuner, geom FROM adm_indelning.v_dalarna_kommungrupper_4326",
            con,
            geom_col="geom",
        )
    finally:
        con.close()

    return kommuner.to_crs(4326), grupper.to_crs(4326)


def load_plats_layers_from_db() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
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
        plats1 = gpd.read_postgis(
            """
            SELECT
              qgis_id, record, respid, kommungrupp, plats_nr, kommunkod, admin_2, plats_fritext, lat, lon,
              ST_Transform(geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_plats1_geom_3006
            """,
            con,
            geom_col="geom",
        )
        plats2 = gpd.read_postgis(
            """
            SELECT
              qgis_id, record, respid, kommungrupp, plats_nr, kommunkod, admin_2, plats_fritext, lat, lon,
              ST_Transform(geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_plats2_geom_3006
            """,
            con,
            geom_col="geom",
        )
    finally:
        con.close()

    return plats1.to_crs(4326), plats2.to_crs(4326)


def load_sensitivity_layers_from_db() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
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
        sensitive = gpd.read_postgis(
            """
            SELECT
              qgis_id, record, respid, kommungrupp, plats_nr, kommunkod, admin_2, plats_fritext, lat, lon,
              ST_Transform(geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_extra_sensitive_points_3006
            """,
            con,
            geom_col="geom",
        )
        non_sensitive = gpd.read_postgis(
            """
            SELECT
              qgis_id, record, respid, kommungrupp, plats_nr, kommunkod, admin_2, plats_fritext, lat, lon,
              ST_Transform(geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_not_extra_sensitive_points_3006
            """,
            con,
            geom_col="geom",
        )
    finally:
        con.close()

    return sensitive.to_crs(4326), non_sensitive.to_crs(4326)


def load_dalarna_boundary_from_db() -> gpd.GeoDataFrame:
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
        lan = gpd.read_postgis(
            "SELECT id, lanskod, lansnamn, geom FROM adm_indelning.v_dalarna_lan_4326",
            con,
            geom_col="geom",
        )
    finally:
        con.close()
    return lan.to_crs(4326)


def locate_wind_layer(repo_root: Path) -> Path:
    base = (
        repo_root
        / "data"
        / "raw"
        / "unpacked"
        / "Geodata-20260223T113354Z-1-001"
        / "Geodata"
        / "lst.vbk_vindkraftverk"
    )
    gpkg = base / "LST.vbk_vindkraftverk.gpkg"
    shp = base / "LST.vbk_vindkraftverk.shp"
    if gpkg.exists():
        return gpkg
    if shp.exists():
        return shp
    raise FileNotFoundError("Could not find expected wind turbine layer")


def load_wind_turbines_dalarna_buffer(repo_root: Path, buffer_m: int = 30000) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    wind_path = locate_wind_layer(repo_root)
    wind = gpd.read_file(wind_path, columns=["VERKID", "STATUS", "KOMNAMN", "LANSNAMN", "geometry"])
    if wind.crs is None:
        wind = wind.set_crs(3006)
    wind_3006 = wind.to_crs(3006)

    try:
        dalarna = load_dalarna_boundary_from_db()
    except Exception:
        fallback = (
            repo_root
            / "data"
            / "raw"
            / "unpacked"
            / "Geodata-20260223T113354Z-1-001"
            / "Geodata"
            / "Dalarna lansgrans"
            / "Dalarna lansgrans"
            / "Dalarna lansgrans.shp"
        )
        dalarna = gpd.read_file(fallback)
        if dalarna.crs is None:
            dalarna = dalarna.set_crs(3006)

    dalarna_3006 = dalarna.to_crs(3006)
    geom = dalarna_3006.geometry.unary_union
    buf_geom = geom.buffer(float(buffer_m))
    buffer_gdf = gpd.GeoDataFrame({"name": [f"dalarna_plus_{buffer_m}m"]}, geometry=[buf_geom], crs=3006)
    selected = wind_3006[wind_3006.geometry.intersects(buf_geom)].copy()
    return selected.to_crs(4326), buffer_gdf.to_crs(4326)


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


def _group_color(value: object) -> str:
    try:
        key = int(float(str(value)))
        return GROUP_PALETTE.get(key, "#9ca3af")
    except Exception:
        return "#9ca3af"


def _combine_point_layers_for_landscape(
    plats1_points: gpd.GeoDataFrame | None,
    plats2_points: gpd.GeoDataFrame | None,
    sensitive_points: gpd.GeoDataFrame | None,
) -> gpd.GeoDataFrame:
    frames: list[gpd.GeoDataFrame] = []

    def _normalize_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        geom_col = gdf.geometry.name
        out = gdf.copy()
        out["geometry"] = out.geometry
        if geom_col != "geometry":
            out = out.drop(columns=[geom_col], errors="ignore")
        return gpd.GeoDataFrame(out, geometry="geometry", crs=gdf.crs)
    if plats1_points is not None and len(plats1_points) > 0:
        p1 = _normalize_geometry(plats1_points)
        p1["_source"] = "Plats 1"
        frames.append(p1)
    if plats2_points is not None and len(plats2_points) > 0:
        p2 = _normalize_geometry(plats2_points)
        p2["_source"] = "Plats 2"
        frames.append(p2)
    if sensitive_points is not None and len(sensitive_points) > 0:
        sp = _normalize_geometry(sensitive_points)
        sp["_source"] = "Extra kanslig"
        frames.append(sp)
    if not frames:
        return gpd.GeoDataFrame(geometry=[], crs=4326)
    points = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry", crs=frames[0].crs)
    return points.to_crs(4326)


def _landscape_point_products(sty: gpd.GeoDataFrame, points: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if points is None or len(points) == 0:
        empty = gpd.GeoDataFrame(geometry=[], crs=4326)
        return empty, empty

    class_col = "_sty_val" if "_sty_val" in sty.columns else non_geometry_columns(sty)[0]
    sty_lu = sty[[class_col, sty.geometry.name]].rename(columns={class_col: "_sty_val", sty.geometry.name: "geometry"})
    sty_lu["_sty_val"] = sty_lu["_sty_val"].astype(str)
    joined = gpd.sjoin(points, sty_lu, how="left", predicate="intersects").drop(columns=["index_right"], errors="ignore")
    joined["_sty_val"] = joined["_sty_val"].fillna("(utanför landskapstyp)").astype(str)

    counts = joined.groupby("_sty_val", dropna=False).size().rename("n_points").reset_index()
    sty_area = sty_lu.dissolve(by="_sty_val", as_index=False)
    summary = sty_area.merge(counts, on="_sty_val", how="left")
    summary["n_points"] = summary["n_points"].fillna(0).astype(int)
    summary = summary[summary["n_points"] > 0].copy()
    if len(summary) > 0:
        summary["geometry"] = summary.geometry.representative_point()
    return joined.to_crs(4326), summary.to_crs(4326)


def build_map(
    sty: gpd.GeoDataFrame,
    kar: gpd.GeoDataFrame,
    sty_field: str,
    kar_field: str,
    show_kar: bool = True,
    kommuner: gpd.GeoDataFrame | None = None,
    kommungrupper: gpd.GeoDataFrame | None = None,
    show_kommuner: bool = False,
    show_kommungrupper: bool = False,
    plats1_points: gpd.GeoDataFrame | None = None,
    plats2_points: gpd.GeoDataFrame | None = None,
    show_plats1_points: bool = False,
    show_plats2_points: bool = False,
    sensitive_points: gpd.GeoDataFrame | None = None,
    non_sensitive_points: gpd.GeoDataFrame | None = None,
    show_sensitive_points: bool = False,
    show_non_sensitive_points: bool = False,
    sensitive_buffer_m: int = 0,
    sty_opacity: float = 0.6,
    show_landscape_colored_points: bool = False,
    show_landscape_aggregated_points: bool = False,
    wind_turbines: gpd.GeoDataFrame | None = None,
    show_wind_turbines: bool = False,
    wind_buffer: gpd.GeoDataFrame | None = None,
    show_wind_buffer: bool = False,
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
        return {"fillColor": colors.get(val, "#cccccc"), "color": "#444444", "weight": 1.4, "fillOpacity": float(sty_opacity), "opacity": 1}

    folium.GeoJson(sty, name="Landskapstyper", style_function=sty_style, popup=folium.GeoJsonPopup(fields=["_sty_popup"], labels=False)).add_to(m)

    if show_kar:
        folium.GeoJson(
            kar,
            name="Landskapskaraktar",
            style_function=lambda _: {"fillOpacity": 0, "color": "#2f2f2f", "weight": 0.9, "opacity": 0.45},
            popup=folium.GeoJsonPopup(fields=["_kar_popup"], labels=False),
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

    if show_plats1_points and plats1_points is not None and len(plats1_points) > 0:
        p1 = plats1_points.copy()
        p1["_grp_color"] = p1["kommungrupp"].map(_group_color)
        folium.GeoJson(
            p1,
            name="Plats 1-punkter",
            marker=folium.CircleMarker(radius=4, weight=1, color="#1f2937", fill=True, fill_opacity=0.8),
            style_function=lambda f: {
                "fillColor": f["properties"].get("_grp_color", "#9ca3af"),
                "color": "#1f2937",
                "weight": 1,
                "fillOpacity": 0.85,
            },
            popup=folium.GeoJsonPopup(
                fields=["kommungrupp", "admin_2", "kommunkod", "plats_fritext"],
                aliases=["Kommungrupp", "Kommun", "Kommunkod", "Fritext"],
                labels=True,
            ),
        ).add_to(m)

    if show_plats2_points and plats2_points is not None and len(plats2_points) > 0:
        p2 = plats2_points.copy()
        p2["_grp_color"] = p2["kommungrupp"].map(_group_color)
        folium.GeoJson(
            p2,
            name="Plats 2-punkter",
            marker=folium.CircleMarker(radius=4, weight=1, color="#1f2937", fill=True, fill_opacity=0.8),
            style_function=lambda f: {
                "fillColor": f["properties"].get("_grp_color", "#9ca3af"),
                "color": "#1f2937",
                "weight": 1,
                "fillOpacity": 0.85,
            },
            popup=folium.GeoJsonPopup(
                fields=["kommungrupp", "admin_2", "kommunkod", "plats_fritext"],
                aliases=["Kommungrupp", "Kommun", "Kommunkod", "Fritext"],
                labels=True,
            ),
        ).add_to(m)

    if show_sensitive_points and sensitive_points is not None and len(sensitive_points) > 0:
        sp = sensitive_points.copy()
        sp["_grp_color"] = sp["kommungrupp"].map(_group_color)
        folium.GeoJson(
            sp,
            name="Extra kansliga punkter",
            marker=folium.CircleMarker(radius=4, weight=1, color="#7f1d1d", fill=True, fill_opacity=0.9),
            style_function=lambda f: {
                "fillColor": f["properties"].get("_grp_color", "#9ca3af"),
                "color": "#7f1d1d",
                "weight": 1,
                "fillOpacity": 0.9,
            },
            popup=folium.GeoJsonPopup(
                fields=["plats_nr", "kommungrupp", "admin_2", "kommunkod", "plats_fritext"],
                aliases=["Plats nr", "Kommungrupp", "Kommun", "Kommunkod", "Fritext"],
                labels=True,
            ),
        ).add_to(m)

        if sensitive_buffer_m > 0:
            geom_col = sp.geometry.name
            buffers = sp.to_crs(3006).copy()
            buffers[geom_col] = buffers.geometry.buffer(float(sensitive_buffer_m))
            merged = buffers.geometry.unary_union
            merged_gdf = gpd.GeoDataFrame({"name": ["sensitive_buffer"]}, geometry=[merged], crs=3006)
            merged_gdf = merged_gdf.to_crs(4326)
            folium.GeoJson(
                merged_gdf,
                name=f"Buffer extra kansliga ({sensitive_buffer_m} m)",
                style_function=lambda _: {
                    "fillColor": "#ef4444",
                    "fillOpacity": 0.12,
                    "color": "#b91c1c",
                    "weight": 1,
                    "opacity": 0.8,
                },
            ).add_to(m)

    if show_non_sensitive_points and non_sensitive_points is not None and len(non_sensitive_points) > 0:
        nsp = non_sensitive_points.copy()
        nsp["_grp_color"] = nsp["kommungrupp"].map(_group_color)
        folium.GeoJson(
            nsp,
            name="Inte extra kansliga punkter",
            marker=folium.CircleMarker(radius=4, weight=1, color="#1e3a8a", fill=True, fill_opacity=0.9),
            style_function=lambda f: {
                "fillColor": f["properties"].get("_grp_color", "#9ca3af"),
                "color": "#1e3a8a",
                "weight": 1,
                "fillOpacity": 0.9,
            },
            popup=folium.GeoJsonPopup(
                fields=["plats_nr", "kommungrupp", "admin_2", "kommunkod", "plats_fritext"],
                aliases=["Plats nr", "Kommungrupp", "Kommun", "Kommunkod", "Fritext"],
                labels=True,
            ),
        ).add_to(m)

    if show_landscape_colored_points or show_landscape_aggregated_points:
        combo = _combine_point_layers_for_landscape(plats1_points, plats2_points, sensitive_points)
        classified, summary = _landscape_point_products(sty, combo)

        if show_landscape_colored_points and len(classified) > 0:
            folium.GeoJson(
                classified,
                name="Punkter fargade efter landskapstyp",
                marker=folium.CircleMarker(radius=3, weight=1, color="#1f2937", fill=True, fill_opacity=0.8),
                style_function=lambda f: {
                    "fillColor": colors.get(f["properties"].get("_sty_val", "(saknas)"), "#9ca3af"),
                    "color": "#1f2937",
                    "weight": 1,
                    "fillOpacity": 0.85,
                },
                popup=folium.GeoJsonPopup(
                    fields=["_source", "_sty_val", "plats_nr", "kommungrupp", "admin_2", "kommunkod"],
                    aliases=["Kalla", "Landskapstyp", "Plats nr", "Kommungrupp", "Kommun", "Kommunkod"],
                    labels=True,
                ),
            ).add_to(m)

        if show_landscape_aggregated_points and len(summary) > 0:
            agg_layer = folium.FeatureGroup(name="Aggregerade punkter per landskapstyp")
            for _, row in summary.iterrows():
                pt = row.geometry
                label = str(row["_sty_val"])
                n_points = int(row["n_points"])
                folium.CircleMarker(
                    location=[pt.y, pt.x],
                    radius=5,
                    color="#111827",
                    weight=1,
                    fill=True,
                    fill_color=colors.get(label, "#9ca3af"),
                    fill_opacity=0.95,
                    popup=folium.Popup(f"Landskapstyp: {label}<br>Antal punkter: {n_points}", max_width=260),
                    tooltip=f"{label}: {n_points}",
                ).add_to(agg_layer)
            agg_layer.add_to(m)

    if show_wind_turbines and wind_turbines is not None and len(wind_turbines) > 0:
        folium.GeoJson(
            wind_turbines,
            name="Vindkraftverk (Dalarna + 30 km)",
            marker=folium.CircleMarker(radius=3, weight=1, color="#14532d", fill=True, fill_opacity=0.9),
            style_function=lambda _: {
                "fillColor": "#22c55e",
                "color": "#14532d",
                "weight": 1,
                "fillOpacity": 0.9,
            },
            popup=folium.GeoJsonPopup(
                fields=["VERKID", "STATUS", "KOMNAMN", "LANSNAMN"],
                aliases=["VerkID", "Status", "Kommun", "Lan"],
                labels=True,
            ),
        ).add_to(m)

    if show_wind_buffer and wind_buffer is not None and len(wind_buffer) > 0:
        folium.GeoJson(
            wind_buffer,
            name="Dalarna + 30 km buffer",
            style_function=lambda _: {
                "fillColor": "#16a34a",
                "fillOpacity": 0.08,
                "color": "#166534",
                "weight": 1.2,
                "opacity": 0.9,
            },
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
