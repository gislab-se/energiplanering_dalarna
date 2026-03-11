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

THEME_LAYER_SPECS = {
    "landskapstyp": ("Lstw.LstW_Landskapstyper", "Lstw.LstW_Landskapstyper"),
    "landskapskaraktar": ("Lstw.LstW_Landskapskaraktarsomraden", "Lstw.LstW_Landskapskaraktarsomraden"),
    "rorligt_friluftsliv": ("lst.LST_RI_Rorligt_friluftsliv_MB4kap2", "lst.LST_RI_Rorligt_friluftsliv_MB4kap2"),
    "utbyggnad_vindkraft": ("Lstw.LstW_Regional_analys_utbyggnad_vindkraft_juni2024", "Lstw.LstW_Regional_analys_utbyggnad_vindkraft_juni2024"),
    "nature_reserve": ("qgis_osm", "naturereserve"),
    "kulturmiljovard": ("raa.RAA_RI_kulturmiljovard_MB3kap6", "raa.RAA_RI_kulturmiljovard_MB3kap6"),
}


def _first_existing(paths: Iterable[Path]) -> Path:
    for p in paths:
        if p.exists():
            return p
    raise FileNotFoundError("Could not find expected shapefile path")


def _prefer_vector_path(folder: Path, stem: str) -> Path:
    gpkg = folder / f"{stem}.gpkg"
    shp = folder / f"{stem}.shp"
    if gpkg.exists():
        return gpkg
    if shp.exists():
        return shp
    raise FileNotFoundError(f"Could not find expected layer: {stem}")


def locate_layers(repo_root: Path) -> Tuple[Path, Path]:
    unpacked = repo_root / "data" / "raw" / "unpacked"
    try:
        sty = _prefer_vector_path(unpacked / "Lstw.LstW_Landskapstyper", "Lstw.LstW_Landskapstyper")
        kar = _prefer_vector_path(unpacked / "Lstw.LstW_Landskapskaraktarsomraden", "Lstw.LstW_Landskapskaraktarsomraden")
        return sty, kar
    except FileNotFoundError:
        base = unpacked / "Geodata-20260223T113354Z-1-001" / "Geodata" / "Landskapstyper gis"
        sty = _first_existing(base.glob("*Landskapstyper*.shp"))
        kar = _first_existing(base.glob("*Landskapskar*.shp"))
        return sty, kar


def load_layers(repo_root: Path) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    sty_path, kar_path = locate_layers(repo_root)
    sty = gpd.read_file(sty_path).to_crs(4326)
    kar = gpd.read_file(kar_path).to_crs(4326)
    return sty, kar


def _load_dalarna_boundary(repo_root: Path) -> gpd.GeoDataFrame:
    cloud = repo_root / "data" / "cloud"
    admin_bundle = cloud / "admin_boundaries.gpkg"
    if admin_bundle.exists():
        for layer in ["lan", "lan_boundary", "county", "lansgrans"]:
            try:
                return gpd.read_file(admin_bundle, layer=layer).to_crs(4326)
            except Exception:
                continue

    background_bundle = cloud / "background_layers.gpkg"
    if background_bundle.exists():
        try:
            return gpd.read_file(background_bundle, layer="lan_boundary").to_crs(4326)
        except Exception:
            pass

    cloud_shp = cloud / "Dalarna lansgrans.shp"
    if cloud_shp.exists():
        lan = gpd.read_file(cloud_shp)
        if lan.crs is None:
            lan = lan.set_crs(3006)
        return lan.to_crs(4326)

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
    lan = gpd.read_file(fallback)
    if lan.crs is None:
        lan = lan.set_crs(3006)
    return lan.to_crs(4326)


def _clip_and_simplify_to_dalarna(gdf: gpd.GeoDataFrame, dalarna_4326: gpd.GeoDataFrame, layer_key: str | None = None) -> gpd.GeoDataFrame:
    if gdf is None or len(gdf) == 0:
        return gdf
    if gdf.crs is None:
        gdf = gdf.set_crs(3006)
    gdf = gdf.to_crs(4326)
    clipped = gpd.clip(gdf, dalarna_4326)
    if len(clipped) == 0:
        return clipped

    # Förenkla stora lager i meter för snabbare rendering i Streamlit/Folium.
    tol = 0.0
    n = len(clipped)
    if layer_key == "nature_reserve":
        if n > 25000:
            tol = 140.0
        elif n > 10000:
            tol = 100.0
        elif n > 2500:
            tol = 60.0
    elif n > 25000:
        tol = 80.0
    elif n > 10000:
        tol = 50.0
    elif n > 2500:
        tol = 25.0

    if tol > 0:
        tmp = clipped.to_crs(3006).copy()
        tmp["geometry"] = tmp.geometry.simplify(tolerance=tol, preserve_topology=True)
        clipped = tmp.to_crs(4326)

    # Avoid unary_union for this layer; source geometries may trigger topology errors.
    if layer_key == "nature_reserve":
        return clipped

    return clipped


def load_theme_layer(repo_root: Path, key: str) -> gpd.GeoDataFrame:
    if key not in THEME_LAYER_SPECS:
        raise KeyError(f"Unknown theme layer key: {key}")
    if key == "nature_reserve":
        path = repo_root / "data" / "qgis_osm" / "naturereserve.gpkg"
        if not path.exists():
            raise FileNotFoundError(f"Could not find expected layer: {path}")
    else:
        unpacked = repo_root / "data" / "raw" / "unpacked"
        folder_name, stem = THEME_LAYER_SPECS[key]
        path = _prefer_vector_path(unpacked / folder_name, stem)

    # Persist en förenklad "light"-version av naturvärden för snabbare återladdning.
    if key == "nature_reserve":
        cache_dir = repo_root / "data" / "processed" / "light_layers"
        cache_path = cache_dir / "nature_reserve_dalarna_light.gpkg"
        if cache_path.exists():
            return gpd.read_file(cache_path)

    dalarna = _load_dalarna_boundary(repo_root)
    gdf = gpd.read_file(path)
    out = _clip_and_simplify_to_dalarna(gdf, dalarna, key)

    if key == "nature_reserve":
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            out.to_file(cache_path, driver="GPKG")
        except Exception:
            pass
    return out


def load_theme_layers(repo_root: Path) -> dict[str, gpd.GeoDataFrame]:
    return {key: load_theme_layer(repo_root, key) for key in THEME_LAYER_SPECS}


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
              p.qgis_id, p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod, p.admin_2, p.plats_fritext, p.lat, p.lon,
              n.q1::text AS home_kommunkod,
              n.kommungrupp::text AS home_kommungrupp,
              ST_Transform(geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_plats1_geom_3006 p
            LEFT JOIN novus.novus_full_dataframe n
              ON n.record = p.record
             AND n.respid = p.respid
            """,
            con,
            geom_col="geom",
        )
        plats2 = gpd.read_postgis(
            """
            SELECT
              p.qgis_id, p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod, p.admin_2, p.plats_fritext, p.lat, p.lon,
              n.q1::text AS home_kommunkod,
              n.kommungrupp::text AS home_kommungrupp,
              ST_Transform(geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_plats2_geom_3006 p
            LEFT JOIN novus.novus_full_dataframe n
              ON n.record = p.record
             AND n.respid = p.respid
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
              p.qgis_id, p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod, p.admin_2, p.plats_fritext, p.lat, p.lon,
              n.q1::text AS home_kommunkod,
              n.kommungrupp::text AS home_kommungrupp,
              ST_Transform(geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_extra_sensitive_points_3006 p
            LEFT JOIN novus.novus_full_dataframe n
              ON n.record = p.record
             AND n.respid = p.respid
            """,
            con,
            geom_col="geom",
        )
        non_sensitive = gpd.read_postgis(
            """
            SELECT
              p.qgis_id, p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod, p.admin_2, p.plats_fritext, p.lat, p.lon,
              n.q1::text AS home_kommunkod,
              n.kommungrupp::text AS home_kommungrupp,
              ST_Transform(geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_not_extra_sensitive_points_3006 p
            LEFT JOIN novus.novus_full_dataframe n
              ON n.record = p.record
             AND n.respid = p.respid
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

    dalarna_3006 = _load_dalarna_boundary(repo_root).to_crs(3006)
    geom = dalarna_3006.geometry.unary_union
    buf_geom = geom.buffer(float(buffer_m))
    buffer_gdf = gpd.GeoDataFrame({"name": [f"dalarna_plus_{buffer_m}m"]}, geometry=[buf_geom], crs=3006)
    selected = wind_3006[wind_3006.geometry.intersects(buf_geom)].copy()
    return selected.to_crs(4326), buffer_gdf.to_crs(4326)


def non_geometry_columns(gdf: gpd.GeoDataFrame) -> list[str]:
    return [c for c in gdf.columns if c != gdf.geometry.name]


def choose_default_field(gdf: gpd.GeoDataFrame) -> str:
    cols = non_geometry_columns(gdf)
    if not cols:
        return gdf.geometry.name
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
    show_sty: bool = True,
    show_kar: bool = True,
    lan_boundary: gpd.GeoDataFrame | None = None,
    show_lan_boundary: bool = False,
    theme_layers: dict[str, gpd.GeoDataFrame] | None = None,
    theme_visibility: dict[str, bool] | None = None,
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
    satellite_base: bool = False,
    extra_image_overlays: list[dict[str, object]] | None = None,
) -> folium.Map:
    sty_vals = sty[sty_field].fillna("(saknas)").astype(str).map(_normalize_landscape_type)
    kar_vals = kar[kar_field].fillna("(saknas)").astype(str)
    sty = sty.assign(_sty_val=sty_vals, _sty_popup=sty_vals)
    kar = kar.assign(_kar_val=kar_vals, _kar_popup=kar_vals)

    colors = _palette_map(sty["_sty_val"])
    centroid = sty.unary_union.centroid
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=8, tiles=None)
    folium.TileLayer(
        tiles="CartoDB positron",
        name="Bakgrund: Ljus karta",
        overlay=False,
        control=True,
        show=not satellite_base,
    ).add_to(m)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles © Esri, Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community",
        name="Bakgrund: Satellit",
        overlay=False,
        control=True,
        show=satellite_base,
        max_zoom=19,
    ).add_to(m)

    def sty_style(feature: dict) -> dict:
        val = str(feature["properties"].get("_sty_val", "(saknas)"))
        return {"fillColor": colors.get(val, "#cccccc"), "color": "#444444", "weight": 1.4, "fillOpacity": float(sty_opacity), "opacity": 1}

    if show_sty:
        folium.GeoJson(sty, name="Landskapstyper.lst", style_function=sty_style, popup=folium.GeoJsonPopup(fields=["_sty_popup"], labels=False)).add_to(m)

    if show_kar:
        kar_colors = _palette_map(kar["_kar_val"])

        def kar_style(feature: dict) -> dict:
            val = str(feature["properties"].get("_kar_val", "(saknas)"))
            return {
                "fillColor": kar_colors.get(val, "#94a3b8"),
                "fillOpacity": 0.2,
                "color": "#334155",
                "weight": 0.9,
                "opacity": 0.7,
            }

        folium.GeoJson(
            kar,
            name="Landskapskaraktärsområden.lst",
            style_function=kar_style,
            popup=folium.GeoJsonPopup(fields=["_kar_popup"], labels=False),
        ).add_to(m)

    if False and show_kar:
        folium.GeoJson(
            kar,
            name="Landskapskaraktär",
            style_function=lambda _: {"fillOpacity": 0, "color": "#2f2f2f", "weight": 0.9, "opacity": 0.45},
            popup=folium.GeoJsonPopup(fields=["_kar_popup"], labels=False),
        ).add_to(m)

    if show_lan_boundary and lan_boundary is not None and len(lan_boundary) > 0:
        lan_popup_fields = [c for c in ["lansnamn", "lanskod", "LANSNAMN", "LANSKOD", "name", "id"] if c in lan_boundary.columns]
        lan_popup_alias = []
        for c in lan_popup_fields:
            cl = c.lower()
            if "namn" in cl or cl == "name":
                lan_popup_alias.append("Lan")
            elif "kod" in cl:
                lan_popup_alias.append("Kod")
            else:
                lan_popup_alias.append(c)
        lan_popup = folium.GeoJsonPopup(fields=lan_popup_fields, aliases=lan_popup_alias, labels=True) if lan_popup_fields else None

        folium.GeoJson(
            lan_boundary,
            name="Länsgräns",
            style_function=lambda _: {"fillOpacity": 0, "color": "#b91c1c", "weight": 3.2, "opacity": 0.98},
            popup=lan_popup,
        ).add_to(m)

    if theme_layers:
        label_by_key = {
            "rorligt_friluftsliv": "Rörligt friluftsliv.lst",
            "utbyggnad_vindkraft": "Utbyggnad av vindkraft.lst",
            "nature_reserve": "Naturreservat.osm",
            "kulturmiljovard": "Kulturmiljövård.lst",
        }
        style_by_key = {
            "rorligt_friluftsliv": {"fillColor": "#0891b2", "fillOpacity": 0.2, "color": "#0e7490", "weight": 1.0, "opacity": 0.9},
            "utbyggnad_vindkraft": {"fillColor": "#22c55e", "fillOpacity": 0.2, "color": "#15803d", "weight": 1.0, "opacity": 0.9},
            "nature_reserve": {"fillColor": "#16a34a", "fillOpacity": 0.16, "color": "#166534", "weight": 0.6, "opacity": 0.7},
            "kulturmiljovard": {"fillColor": "#f59e0b", "fillOpacity": 0.2, "color": "#b45309", "weight": 1.0, "opacity": 0.9},
        }
        popup_fields_by_key = {
            "rorligt_friluftsliv": ["namn"],
            "utbyggnad_vindkraft": ["Bebyggelse"],
            "nature_reserve": ["name"],
            "kulturmiljovard": ["NAMN", "BESKRIVNIN"],
        }
        popup_alias_by_key = {
            "rorligt_friluftsliv": ["Namn"],
            "utbyggnad_vindkraft": ["Bebyggelse"],
            "nature_reserve": ["Namn"],
            "kulturmiljovard": ["Namn", "Beskrivning"],
        }

        for key, gdf in theme_layers.items():
            if key in {"landskapstyp", "landskapskaraktar"}:
                continue
            if gdf is None or len(gdf) == 0:
                continue
            if theme_visibility and not theme_visibility.get(key, False):
                continue

            requested_fields = popup_fields_by_key.get(key, [])
            fields = [f for f in requested_fields if f in gdf.columns]
            aliases = popup_alias_by_key.get(key, ["Info"] * max(1, len(fields)))
            if not fields:
                field = choose_default_field(gdf)
                fields = [field]
                aliases = ["Info"]

            style_cfg = style_by_key.get(
                key,
                {"fillColor": "#64748b", "fillOpacity": 0.16, "color": "#334155", "weight": 1.0, "opacity": 0.9},
            )
            smooth_factor = 2.0 if key == "nature_reserve" else None
            folium.GeoJson(
                gdf,
                name=label_by_key.get(key, key),
                style_function=lambda _, s=style_cfg: s,
                popup=folium.GeoJsonPopup(fields=fields, aliases=aliases, labels=True),
                smooth_factor=smooth_factor,
            ).add_to(m)

    if False and theme_layers:
        label_by_key = {
            "rorligt_friluftsliv": "lst.LST_RI_Rorligt_friluftsliv_MB4kap2",
            "utbyggnad_vindkraft": "Lstw.LstW_Regional_analys_utbyggnad_vindkraft_juni2024",
            "naturvarden_lst": "Lstw.PG204_naturvarden_kanda_av_lst_dalarna",
            "kulturmiljovard": "raa.RAA_RI_kulturmiljovard_MB3kap6",
        }
        base_color_by_key = {
            "rorligt_friluftsliv": "#0891b2",
            "utbyggnad_vindkraft": "#22c55e",
            "naturvarden_lst": "#16a34a",
            "kulturmiljovard": "#f59e0b",
        }
        for key, gdf in theme_layers.items():
            if key in {"landskapstyp", "landskapskaraktar"}:
                continue
            if gdf is None or len(gdf) == 0:
                continue
            if theme_visibility and not theme_visibility.get(key, False):
                continue
            field = choose_default_field(gdf)
            cat_col = f"_{key}_cat"
            tmp = gdf.copy()
            tmp[cat_col] = tmp[field].fillna("(saknas)").astype(str)
            cat_colors = _palette_map(tmp[cat_col])

            def themed_style(
                feature: dict,
                category_col: str = cat_col,
                colors_map: dict[str, str] = cat_colors,
                fallback: str = base_color_by_key.get(key, "#64748b"),
            ) -> dict:
                val = str(feature["properties"].get(category_col, "(saknas)"))
                clr = colors_map.get(val, fallback)
                return {"fillColor": clr, "fillOpacity": 0.2, "color": "#334155", "weight": 1.0, "opacity": 0.9}

            folium.GeoJson(
                tmp,
                name=label_by_key.get(key, key),
                style_function=themed_style,
                popup=folium.GeoJsonPopup(fields=[field], aliases=["Kategori"], labels=True),
            ).add_to(m)

    if False and theme_layers:
        style_by_key = {
            "rorligt_friluftsliv": {"fillColor": "#0891b2", "fillOpacity": 0.18, "color": "#0e7490", "weight": 1.1, "opacity": 0.9},
            "utbyggnad_vindkraft": {"fillColor": "#22c55e", "fillOpacity": 0.16, "color": "#15803d", "weight": 1.0, "opacity": 0.85},
            "naturvarden_lst": {"fillColor": "#16a34a", "fillOpacity": 0.16, "color": "#166534", "weight": 1.0, "opacity": 0.85},
            "kulturmiljovard": {"fillColor": "#f59e0b", "fillOpacity": 0.16, "color": "#b45309", "weight": 1.0, "opacity": 0.9},
        }
        label_by_key = {
            "rorligt_friluftsliv": "Riksintresse för rörligt friluftsliv",
            "utbyggnad_vindkraft": "Områden för utbyggnad av vindkraft",
            "naturvarden_lst": "Naturvärden kända av LST Dalarna",
            "kulturmiljovard": "Riksintresse kulturmiljövård",
        }
        for key, gdf in theme_layers.items():
            if key in {"landskapstyp", "landskapskaraktar"}:
                continue
            if gdf is None or len(gdf) == 0:
                continue
            if theme_visibility and not theme_visibility.get(key, False):
                continue
            field = choose_default_field(gdf)
            style_cfg = style_by_key.get(
                key,
                {"fillColor": "#64748b", "fillOpacity": 0.16, "color": "#334155", "weight": 1.0, "opacity": 0.9},
            )
            folium.GeoJson(
                gdf,
                name=label_by_key.get(key, key),
                style_function=lambda _, s=style_cfg: s,
                popup=folium.GeoJsonPopup(fields=[field], aliases=["Namn"], labels=True),
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
                "fillOpacity": 0,
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

    buffer_sources: list[gpd.GeoDataFrame] = []

    if show_plats1_points and plats1_points is not None and len(plats1_points) > 0:
        p1 = plats1_points.copy()
        p1["_grp_color"] = p1["kommungrupp"].map(_group_color)
        folium.GeoJson(
            p1,
            name="Vald plats 1",
            marker=folium.CircleMarker(radius=5, weight=0, color="transparent", fill=True, fill_opacity=0.8),
            style_function=lambda f: {
                "fillColor": f["properties"].get("_grp_color", "#9ca3af"),
                "color": "transparent",
                "weight": 0,
                "fillOpacity": 0.85,
            },
            popup=folium.GeoJsonPopup(
                fields=["kommungrupp", "admin_2", "kommunkod", "plats_fritext"],
                aliases=["Kommungrupp", "Kommun", "Kommunkod", "Fritext"],
                labels=True,
            ),
        ).add_to(m)
        buffer_sources.append(p1)

    if show_plats2_points and plats2_points is not None and len(plats2_points) > 0:
        p2 = plats2_points.copy()
        p2["_grp_color"] = p2["kommungrupp"].map(_group_color)
        folium.GeoJson(
            p2,
            name="Vald plats 2",
            marker=folium.CircleMarker(radius=5, weight=0, color="transparent", fill=True, fill_opacity=0.8),
            style_function=lambda f: {
                "fillColor": f["properties"].get("_grp_color", "#9ca3af"),
                "color": "transparent",
                "weight": 0,
                "fillOpacity": 0.85,
            },
            popup=folium.GeoJsonPopup(
                fields=["kommungrupp", "admin_2", "kommunkod", "plats_fritext"],
                aliases=["Kommungrupp", "Kommun", "Kommunkod", "Fritext"],
                labels=True,
            ),
        ).add_to(m)
        buffer_sources.append(p2)

    if show_sensitive_points and sensitive_points is not None and len(sensitive_points) > 0:
        sp = sensitive_points.copy()
        sp["_grp_color"] = sp["kommungrupp"].map(_group_color)
        folium.GeoJson(
            sp,
            name="Valda platser som är extra känsliga för ny infrastruktur",
            marker=folium.CircleMarker(radius=5, weight=0, color="transparent", fill=True, fill_opacity=0.9),
            style_function=lambda f: {
                "fillColor": f["properties"].get("_grp_color", "#9ca3af"),
                "color": "transparent",
                "weight": 0,
                "fillOpacity": 0.9,
            },
            popup=folium.GeoJsonPopup(
                fields=["plats_nr", "kommungrupp", "admin_2", "kommunkod", "plats_fritext"],
                aliases=["Plats nr", "Kommungrupp", "Kommun", "Kommunkod", "Fritext"],
                labels=True,
            ),
        ).add_to(m)
        buffer_sources.append(sp)

    if show_non_sensitive_points and non_sensitive_points is not None and len(non_sensitive_points) > 0:
        nsp = non_sensitive_points.copy()
        nsp["_grp_color"] = nsp["kommungrupp"].map(_group_color)
        folium.GeoJson(
            nsp,
            name="Valda platser som INTE är känsliga för ny infrastruktur",
            marker=folium.CircleMarker(radius=5, weight=0, color="transparent", fill=True, fill_opacity=0.9),
            style_function=lambda f: {
                "fillColor": f["properties"].get("_grp_color", "#9ca3af"),
                "color": "transparent",
                "weight": 0,
                "fillOpacity": 0.9,
            },
            popup=folium.GeoJsonPopup(
                fields=["plats_nr", "kommungrupp", "admin_2", "kommunkod", "plats_fritext"],
                aliases=["Plats nr", "Kommungrupp", "Kommun", "Kommunkod", "Fritext"],
                labels=True,
            ),
        ).add_to(m)
        buffer_sources.append(nsp)

    if sensitive_buffer_m > 0 and buffer_sources:
        combo = pd.concat(buffer_sources, ignore_index=True)
        pts = gpd.GeoDataFrame(combo, geometry="geometry", crs=buffer_sources[0].crs).to_crs(3006)
        geom_col = pts.geometry.name
        pts[geom_col] = pts.geometry.buffer(float(sensitive_buffer_m))
        merged = pts.geometry.unary_union
        merged_gdf = gpd.GeoDataFrame({"name": ["active_points_buffer"]}, geometry=[merged], crs=3006).to_crs(4326)
        folium.GeoJson(
            merged_gdf,
            name=f"Buffert tända punktlager ({sensitive_buffer_m} m)",
            style_function=lambda _: {
                "fillColor": "#ef4444",
                "fillOpacity": 0.12,
                "color": "#b91c1c",
                "weight": 1,
                "opacity": 0.8,
            },
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
                    aliases=["Källa", "Landskapstyp", "Plats nr", "Kommungrupp", "Kommun", "Kommunkod"],
                    labels=True,
                ),
            ).add_to(m)

        if show_landscape_aggregated_points and len(summary) > 0:
            agg_layer = folium.FeatureGroup(name="Aggregerade punkter per landskapstyp")
            max_n = max(1, int(summary["n_points"].max()))
            for _, row in summary.iterrows():
                pt = row.geometry
                label = str(row["_sty_val"])
                n_points = int(row["n_points"])
                radius = 6 + (20 * ((n_points / max_n) ** 0.5))
                folium.CircleMarker(
                    location=[pt.y, pt.x],
                    radius=radius,
                    color="#111827",
                    weight=1,
                    fill=True,
                    fill_color=colors.get(label, "#9ca3af"),
                    fill_opacity=0.95,
                    popup=folium.Popup(f"Landskapstyp: {label}<br>Antal punkter: {n_points}", max_width=260),
                    tooltip=f"{label}: {n_points}",
                ).add_to(agg_layer)
                folium.Marker(
                    location=[pt.y, pt.x],
                    icon=folium.DivIcon(
                        html=(
                            "<div style='font-size:11px;font-weight:700;color:#111827;"
                            "white-space:nowrap;text-shadow: 0 0 3px rgba(255,255,255,0.95),"
                            " 0 0 6px rgba(255,255,255,0.95);"
                            "transform: translate(-50%, -50%);'>"
                            f"{label}: {n_points}</div>"
                        )
                    ),
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

    if extra_image_overlays:
        for overlay in extra_image_overlays:
            if not isinstance(overlay, dict):
                continue
            image = overlay.get("image")
            bounds = overlay.get("bounds")
            if image is None or bounds is None:
                continue
            try:
                folium.raster_layers.ImageOverlay(
                    image=image,
                    bounds=bounds,
                    name=str(overlay.get("name", "Rasteroverlay")),
                    opacity=float(overlay.get("opacity", 0.55)),
                    interactive=False,
                    cross_origin=False,
                    zindex=int(overlay.get("zindex", 5)),
                ).add_to(m)
            except Exception:
                continue

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
