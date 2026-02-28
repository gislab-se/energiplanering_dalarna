from __future__ import annotations

from pathlib import Path
import inspect
import importlib.util
import os
import re
import sys

import folium
import geopandas as gpd
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from streamlit_folium import folium_static
from shapely.geometry import Point

try:
    import scripts.map_factory as map_factory
except Exception:
    # Streamlit Cloud may resolve "scripts" to a non-local namespace.
    # Fallback: load local scripts/map_factory.py directly.
    _mf_path = Path(__file__).resolve().parent / "scripts" / "map_factory.py"
    if str(_mf_path.parent.parent) not in sys.path:
        sys.path.insert(0, str(_mf_path.parent.parent))
    _spec = importlib.util.spec_from_file_location("local_map_factory", _mf_path)
    if _spec is None or _spec.loader is None:
        raise ImportError(f"Could not load local map_factory at {_mf_path}")
    map_factory = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(map_factory)

build_map = map_factory.build_map
choose_default_field = map_factory.choose_default_field
load_admin_layers_from_db = map_factory.load_admin_layers_from_db
load_dalarna_boundary_from_db = map_factory.load_dalarna_boundary_from_db
load_layers = map_factory.load_layers
load_wind_turbines_dalarna_buffer = map_factory.load_wind_turbines_dalarna_buffer
load_plats_layers_from_db = map_factory.load_plats_layers_from_db
load_sensitivity_layers_from_db = map_factory.load_sensitivity_layers_from_db


st.set_page_config(page_title="Energiomstallning i Dalarna", layout="wide")
st.title("Energiomstallning i Dalarna")

repo_root = Path(__file__).resolve().parent
cloud_dir = repo_root / "data" / "cloud"

BACKGROUND_BUNDLE_GPKG = "background_layers.gpkg"
LST_BUNDLE_GPKG = "lst_layers.gpkg"
ADMIN_BUNDLE_GPKG = "admin_boundaries.gpkg"

LST_BUNDLE_LAYER_BY_KEY = {
    "landskapstyp": "landskapstyp",
    "landskapskaraktar": "landskapskaraktar",
    "rorligt_friluftsliv": "rorligt_friluftsliv",
    "utbyggnad_vindkraft": "utbyggnad_vindkraft",
    "nature_reserve": "nature_reserve",
    "kulturmiljovard": "kulturmiljovard",
}

# Canonical Dalarna grouping for Hemvist (QI) filtering.
CODE_TO_GROUP_NAME = {
    "2084": "Avesta, Hedemora, Sater",
    "2083": "Avesta, Hedemora, Sater",
    "2082": "Avesta, Hedemora, Sater",
    "2080": "Falun, Borlange",
    "2081": "Falun, Borlange",
    "2023": "Malung-Salen, Alvdalen, Vansbro",
    "2039": "Malung-Salen, Alvdalen, Vansbro",
    "2021": "Malung-Salen, Alvdalen, Vansbro",
    "2062": "Mora, Orsa",
    "2034": "Mora, Orsa",
    "2031": "Rattvik, Leksand, Gagnef",
    "2029": "Rattvik, Leksand, Gagnef",
    "2026": "Rattvik, Leksand, Gagnef",
    "2061": "Smedjebacken, Ludvika",
    "2085": "Smedjebacken, Ludvika",
}


def _read_vector_4326(path: Path, layer: str | None = None, default_crs: int | None = None) -> gpd.GeoDataFrame:
    if layer:
        gdf = gpd.read_file(path, layer=layer)
    else:
        gdf = gpd.read_file(path)
    if gdf.crs is None:
        # If CRS metadata is missing, infer likely CRS from coordinate ranges.
        # This avoids "invisible layer" when data is already WGS84 but treated as SWEREF99.
        minx, miny, maxx, maxy = gdf.total_bounds
        looks_like_wgs84 = (
            -180.0 <= minx <= 180.0
            and -180.0 <= maxx <= 180.0
            and -90.0 <= miny <= 90.0
            and -90.0 <= maxy <= 90.0
        )
        inferred = 4326 if looks_like_wgs84 else (default_crs or 3006)
        gdf = gdf.set_crs(inferred)
    return gdf.to_crs(4326)


def _normalize_lan_boundary_schema(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf is None or len(gdf) == 0:
        return gdf
    out = gdf.copy()
    cols_lower = {c.lower(): c for c in out.columns}

    if "lansnamn" not in out.columns:
        source_name = None
        for k in ["lansnamn", "name", "lan", "namn"]:
            if k in cols_lower:
                source_name = cols_lower[k]
                break
        if source_name is not None:
            out["lansnamn"] = out[source_name].astype(str)
        else:
            out["lansnamn"] = "Dalarnas lan"

    if "lanskod" not in out.columns:
        source_code = None
        for k in ["lanskod", "id", "code", "kod"]:
            if k in cols_lower:
                source_code = cols_lower[k]
                break
        if source_code is not None:
            out["lanskod"] = out[source_code].astype(str)
        else:
            out["lanskod"] = ""

    # Keep only polygon/line geometries for boundary rendering/analysis.
    # Point geometries render as a blue marker in Folium, which is not desired here.
    gtype = out.geometry.geom_type.astype(str)
    keep = gtype.isin(["Polygon", "MultiPolygon", "LineString", "MultiLineString"])
    out = out[keep].copy()
    if len(out) == 0:
        return out

    return out


def _pick_col(columns: list[str], candidates: list[str]) -> str | None:
    lookup = {c.lower(): c for c in columns}
    for c in candidates:
        if c.lower() in lookup:
            return lookup[c.lower()]
    return None


def _normalize_kommuner_schema(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    cols = list(out.columns)
    name_col = _pick_col(cols, ["kommunnamn", "kommun_namn", "namn", "name"])
    code_col = _pick_col(cols, ["kommunkod", "kommun_kod", "kod", "id"])
    if "kommunnamn" not in out.columns:
        out["kommunnamn"] = out[name_col].astype(str) if name_col else "Kommun"
    if "kommunkod" not in out.columns:
        out["kommunkod"] = out[code_col].astype(str) if code_col else ""
    gtype = out.geometry.geom_type.astype(str)
    out = out[gtype.isin(["Polygon", "MultiPolygon", "LineString", "MultiLineString"])].copy()
    return out


def _normalize_kommungrupper_schema(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = gdf.copy()
    cols = list(out.columns)
    name_col = _pick_col(cols, ["kommungrupp_namn", "grupp_namn", "namn", "name"])
    id_col = _pick_col(cols, ["kommungrupp_id", "grupp_id", "id", "kod"])
    members_col = _pick_col(cols, ["kommuner", "members", "kommunlista"])
    if "kommungrupp_namn" not in out.columns:
        out["kommungrupp_namn"] = out[name_col].astype(str) if name_col else "Kommungrupp"
    if "kommungrupp_id" not in out.columns:
        out["kommungrupp_id"] = out[id_col].astype(str) if id_col else ""
    if "kommuner" not in out.columns:
        out["kommuner"] = out[members_col].astype(str) if members_col else ""
    gtype = out.geometry.geom_type.astype(str)
    out = out[gtype.isin(["Polygon", "MultiPolygon", "LineString", "MultiLineString"])].copy()
    return out


def _read_first_layer(path: Path, layer_candidates: list[str], default_crs: int = 3006) -> gpd.GeoDataFrame | None:
    for layer in layer_candidates:
        try:
            return _read_vector_4326(path, layer=layer, default_crs=default_crs)
        except Exception:
            continue
    return None


def _load_admin_layers_local(repo: Path) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame] | None:
    admin_bundle = repo / "data" / "cloud" / ADMIN_BUNDLE_GPKG
    if not admin_bundle.exists():
        return None

    kommuner = _read_first_layer(admin_bundle, ["kommuner", "kommungrans", "kommungräns", "kommun", "municipalities"])
    kommungrupper = _read_first_layer(admin_bundle, ["kommungrupper", "kommungrupp", "groups"])
    if kommuner is None or kommungrupper is None:
        return None
    kommuner = _normalize_kommuner_schema(kommuner)
    kommungrupper = _normalize_kommungrupper_schema(kommungrupper)

    # Guard against accidentally exported län-geometry in admin layers.
    if len(kommuner) <= 1 or len(kommungrupper) <= 1:
        return None
    return kommuner, kommungrupper


@st.cache_data(show_spinner=False, ttl=300)
def _cached_admin_layers(repo_root_str: str):
    repo = Path(repo_root_str)
    local = _load_admin_layers_local(repo)
    if local is not None:
        return local
    return load_admin_layers_from_db()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_lan_boundary():
    try:
        return _normalize_lan_boundary_schema(load_dalarna_boundary_from_db())
    except Exception:
        admin_bundle = cloud_dir / ADMIN_BUNDLE_GPKG
        if admin_bundle.exists():
            lan_local = _read_first_layer(admin_bundle, ["lan", "lan_boundary", "county", "lansgrans"])
            if lan_local is not None:
                return _normalize_lan_boundary_schema(lan_local)
        background_bundle = cloud_dir / BACKGROUND_BUNDLE_GPKG
        if background_bundle.exists():
            try:
                return _normalize_lan_boundary_schema(
                    _read_vector_4326(background_bundle, layer="lan_boundary", default_crs=3006)
                )
            except Exception:
                pass
        cloud_shp = cloud_dir / "Dalarna lansgrans.shp"
        if cloud_shp.exists():
            return _normalize_lan_boundary_schema(_read_vector_4326(cloud_shp, default_crs=3006))
        raise


@st.cache_data(show_spinner=False, ttl=300)
def _cached_base_layers(repo_root_str: str):
    repo = Path(repo_root_str)
    lst_bundle = repo / "data" / "cloud" / LST_BUNDLE_GPKG
    if lst_bundle.exists():
        try:
            return (
                _read_vector_4326(lst_bundle, layer=LST_BUNDLE_LAYER_BY_KEY["landskapstyp"], default_crs=3006),
                _read_vector_4326(lst_bundle, layer=LST_BUNDLE_LAYER_BY_KEY["landskapskaraktar"], default_crs=3006),
            )
        except Exception:
            pass
    cloud_sty = repo / "data" / "cloud" / "lst_landskapstyper.gpkg"
    cloud_kar = repo / "data" / "cloud" / "lst_landskapskaraktar.gpkg"
    if cloud_sty.exists() and cloud_kar.exists():
        return _read_vector_4326(cloud_sty, default_crs=3006), _read_vector_4326(cloud_kar, default_crs=3006)
    return load_layers(repo)


@st.cache_data(show_spinner=False, ttl=300)
def _cached_theme_layer(repo_root_str: str, key: str):
    repo = Path(repo_root_str)
    lst_bundle = repo / "data" / "cloud" / LST_BUNDLE_GPKG
    if key in LST_BUNDLE_LAYER_BY_KEY and lst_bundle.exists():
        try:
            return _read_vector_4326(lst_bundle, layer=LST_BUNDLE_LAYER_BY_KEY[key], default_crs=3006)
        except Exception:
            pass
    cloud_map = {
        "nature_reserve": "nature_reserve_dalarna_light.gpkg",
        "rorligt_friluftsliv": "lst_rorligt_friluftsliv.gpkg",
        "utbyggnad_vindkraft": "lst_utbyggnad_vindkraft.gpkg",
        "kulturmiljovard": "lst_kulturmiljovard.gpkg",
        "landskapstyp": "lst_landskapstyper.gpkg",
        "landskapskaraktar": "lst_landskapskaraktar.gpkg",
    }
    if key in cloud_map:
        cloud_path = repo / "data" / "cloud" / cloud_map[key]
        if cloud_path.exists():
            return _read_vector_4326(cloud_path, default_crs=3006)
    if hasattr(map_factory, "load_theme_layer"):
        return map_factory.load_theme_layer(repo, key)
    if hasattr(map_factory, "load_theme_layers"):
        return map_factory.load_theme_layers(repo)[key]
    raise AttributeError("Neither load_theme_layer nor load_theme_layers exists in scripts.map_factory")


@st.cache_data(show_spinner=False, ttl=300)
def _cached_plats_layers():
    return load_plats_layers_from_db()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_sensitivity_layers():
    return load_sensitivity_layers_from_db()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_locked_point_layers(repo_root_str: str):
    repo = Path(repo_root_str)
    gpkg = repo / "data" / "cloud" / "novus_locked_points.gpkg"
    if not gpkg.exists():
        gpkg = repo / "data" / "processed" / "locked_layers" / "novus_locked_points.gpkg"
    if not gpkg.exists():
        return None
    plats1 = gpd.read_file(gpkg, layer="plats_1").to_crs(4326)
    plats2 = gpd.read_file(gpkg, layer="plats_2").to_crs(4326)
    sensitive = gpd.read_file(gpkg, layer="plats_3_sensitive").to_crs(4326)
    non_sensitive = gpd.read_file(gpkg, layer="plats_4_not_sensitive").to_crs(4326)

    # Add respondent home fields so Q1/QI filtering behaves the same as DB-backed layers.
    csv_path = repo / "data" / "interim" / "novus" / "novus_full_dataframe.csv"
    if csv_path.exists():
        def _norm_key(series: pd.Series) -> pd.Series:
            return series.astype(str).str.replace(".0", "", regex=False).str.strip()

        base = pd.read_csv(csv_path, usecols=["Record", "respid", "Q1", "Kommungrupp"])
        base["record_key"] = _norm_key(base["Record"])
        base["respid_key"] = base["respid"].astype(str).str.strip()
        base = base.rename(columns={"Q1": "home_kommunkod", "Kommungrupp": "home_kommungrupp"})
        # Canonical current grouping from Q1 (kommunkod), independent of stale Kommungrupp in source.
        base["home_kommunkod_norm"] = _norm_key(base["home_kommunkod"])
        base["home_kommungrupp_current"] = base["home_kommunkod_norm"].map(CODE_TO_GROUP_NAME)
        home_cols = base[["record_key", "respid_key", "home_kommunkod", "home_kommungrupp"]].drop_duplicates()
        home_cols_current = base[
            ["record_key", "respid_key", "home_kommunkod", "home_kommungrupp", "home_kommungrupp_current"]
        ].drop_duplicates()
        # Secondary lookup by respid only (when record ids differ between sources).
        by_respid = (
            base[["respid_key", "home_kommunkod", "home_kommungrupp", "home_kommungrupp_current"]]
            .dropna(subset=["respid_key"])
            .drop_duplicates()
        )

        def _attach_home(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
            if gdf is None or len(gdf) == 0:
                return gdf
            out = gdf.copy()
            out["record_key"] = _norm_key(out["record"])
            out["respid_key"] = out["respid"].astype(str).str.strip()
            out = out.merge(home_cols_current, on=["record_key", "respid_key"], how="left")
            miss = out["home_kommunkod"].isna() | (out["home_kommunkod"].astype(str).str.strip() == "")
            if miss.any():
                fill = out.loc[miss, ["respid_key"]].merge(by_respid, on="respid_key", how="left")
                out.loc[miss, "home_kommunkod"] = fill["home_kommunkod"].values
                out.loc[miss, "home_kommungrupp"] = fill["home_kommungrupp"].values
                out.loc[miss, "home_kommungrupp_current"] = fill["home_kommungrupp_current"].values
            return out.drop(columns=["record_key", "respid_key"])

        plats1 = _attach_home(plats1)
        plats2 = _attach_home(plats2)
        sensitive = _attach_home(sensitive)
        non_sensitive = _attach_home(non_sensitive)

    return plats1, plats2, sensitive, non_sensitive


@st.cache_data(show_spinner=False, ttl=600)
def _cached_wind_layers(repo_root_str: str, buffer_m: int):
    return load_wind_turbines_dalarna_buffer(Path(repo_root_str), buffer_m=buffer_m)


def _empty_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry=[], crs=4326)


def _fallback_center_layer() -> gpd.GeoDataFrame:
    # Safe fallback center in Dalarna so older build_map versions do not crash on empty sty.
    return gpd.GeoDataFrame({"name": ["Dalarna"]}, geometry=[Point(15.0, 61.0)], crs=4326)


def _build_map_compat(**kwargs):
    sig = inspect.signature(build_map)
    accepted = {k: v for k, v in kwargs.items() if k in sig.parameters}
    return build_map(**accepted)


def _numkey(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace(".0", "", regex=False).str.strip()


def _norm_group_name(value: str) -> str:
    s = str(value).strip().lower()
    s = (
        s.replace("å", "a")
        .replace("ä", "a")
        .replace("ö", "o")
        .replace("é", "e")
    )
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _db_ready() -> bool:
    return bool(os.getenv("PGDATABASE") and os.getenv("PGUSER") and os.getenv("PGPASSWORD"))


def _apply_area_filter(
    gdf: gpd.GeoDataFrame | None,
    filter_mode: str,
    area_kind: str,
    area_value: str,
    kommun_code_by_name: dict[str, str],
    group_id_by_name: dict[str, str],
    kommuner: gpd.GeoDataFrame | None,
    kommungrupper: gpd.GeoDataFrame | None,
) -> gpd.GeoDataFrame | None:
    if gdf is None or len(gdf) == 0 or area_kind in {"lan", "all_kommuner", "all_kommungrupper"}:
        return gdf

    # Spatial filter mode: clip points by selected polygon geometry.
    if filter_mode == "Koordinatlage (spatialt)":
        target = None
        if area_kind == "kommun" and kommuner is not None and len(kommuner) > 0:
            target = kommuner[kommuner["kommunnamn"].astype(str) == str(area_value)]
        elif area_kind == "kommungrupp" and kommungrupper is not None and len(kommungrupper) > 0:
            target = kommungrupper[kommungrupper["kommungrupp_namn"].astype(str) == str(area_value)]
        if target is None or len(target) == 0:
            return gdf.iloc[0:0].copy()
        return gdf[gdf.geometry.intersects(target.geometry.unary_union)]

    # Hemvist (QI): filter by respondent home, not point location.
    if area_kind == "kommun":
        code = kommun_code_by_name.get(area_value)
        if code is None:
            return gdf.iloc[0:0].copy()
        col = "home_kommunkod" if "home_kommunkod" in gdf.columns else None
        if col is None:
            return gdf.iloc[0:0].copy()
        return gdf[_numkey(gdf[col]) == str(code)]

    if area_kind == "kommungrupp":
        gid = group_id_by_name.get(area_value)
        if gid is None:
            return gdf.iloc[0:0].copy()
        out = gdf.iloc[0:0].copy()
        # Preferred field when available (derived from Q1 with canonical mapping).
        if filter_mode == "Hemvist (QI)" and "home_kommungrupp_current" in gdf.columns:
            wanted = _norm_group_name(area_value)
            curr = gdf["home_kommungrupp_current"].fillna("").astype(str).map(_norm_group_name)
            has_curr = curr.str.len() > 0
            if has_curr.any():
                out = gdf[curr == wanted]
                return out
        # For Hemvist (QI): use canonical home kommunkod -> kommungrupp mapping.
        if filter_mode == "Hemvist (QI)" and "home_kommunkod" in gdf.columns:
            wanted = _norm_group_name(area_value)
            allowed_codes = [k for k, grp in CODE_TO_GROUP_NAME.items() if _norm_group_name(grp) == wanted]
            if allowed_codes:
                out = gdf[_numkey(gdf["home_kommunkod"]).isin(allowed_codes)]
                return out
        # Prefer deriving group from home_kommunkod -> kommungrupp_id when available.
        # In Hemvist-lage this should be authoritative; stale home_kommungrupp values can be wrong.
        if (
            filter_mode == "Hemvist (QI)"
            and "home_kommunkod" in gdf.columns
            and kommuner is not None
            and len(kommuner) > 0
            and "kommunkod" in kommuner.columns
            and "kommungrupp_id" in kommuner.columns
        ):
            km = kommuner[["kommunkod", "kommungrupp_id"]].dropna().drop_duplicates().copy()
            km["kommunkod_norm"] = _numkey(km["kommunkod"])
            km["kommungrupp_id_norm"] = _numkey(km["kommungrupp_id"])
            code_to_gid = dict(zip(km["kommunkod_norm"], km["kommungrupp_id_norm"]))
            derived_gid = _numkey(gdf["home_kommunkod"]).map(code_to_gid)
            out = gdf[derived_gid.astype(str) == str(gid)]
        # No fallback to stale home_kommungrupp/kommungrupp fields and no spatial fallback in Hemvist mode.
        return out

    return gdf


def _analysis_units(
    area_kind: str,
    area_value: str,
    lan_boundary: gpd.GeoDataFrame | None,
    kommuner: gpd.GeoDataFrame | None,
    kommungrupper: gpd.GeoDataFrame | None,
) -> tuple[gpd.GeoDataFrame | None, str]:
    if area_kind == "lan" and lan_boundary is not None and len(lan_boundary) > 0:
        out = lan_boundary.copy()
        out["kategori"] = "Dalarna"
        return out[["kategori", out.geometry.name]].rename(columns={out.geometry.name: "geometry"}), "kategori"
    if area_kind == "all_kommuner" and kommuner is not None and len(kommuner) > 0:
        return kommuner[["kommunnamn", kommuner.geometry.name]].rename(columns={kommuner.geometry.name: "geometry"}), "kommunnamn"
    if area_kind == "all_kommungrupper" and kommungrupper is not None and len(kommungrupper) > 0:
        return kommungrupper[["kommungrupp_namn", kommungrupper.geometry.name]].rename(columns={kommungrupper.geometry.name: "geometry"}), "kommungrupp_namn"
    if area_kind == "kommun" and kommuner is not None and len(kommuner) > 0:
        k = kommuner[kommuner["kommunnamn"].astype(str) == str(area_value)].copy()
        if len(k) == 0:
            return None, "kategori"
        k["kategori"] = str(area_value)
        return k[["kategori", k.geometry.name]].rename(columns={k.geometry.name: "geometry"}), "kategori"
    if area_kind == "kommungrupp" and kommungrupper is not None and len(kommungrupper) > 0:
        kg = kommungrupper[kommungrupper["kommungrupp_namn"].astype(str) == str(area_value)].copy()
        if len(kg) == 0:
            return None, "kategori"
        kg["kategori"] = str(area_value)
        return kg[["kategori", kg.geometry.name]].rename(columns={kg.geometry.name: "geometry"}), "kategori"
    return None, "kategori"


def _analysis_points(
    show_plats1_points: bool,
    show_plats2_points: bool,
    show_sensitive_points: bool,
    show_non_sensitive_points: bool,
    plats1_points: gpd.GeoDataFrame | None,
    plats2_points: gpd.GeoDataFrame | None,
    sensitive_points: gpd.GeoDataFrame | None,
    non_sensitive_points: gpd.GeoDataFrame | None,
) -> gpd.GeoDataFrame | None:
    by_name = {
        "Plats 1": (plats1_points, show_plats1_points),
        "Plats 2": (plats2_points, show_plats2_points),
        "Extra kansliga": (sensitive_points, show_sensitive_points),
        "Inte extra kansliga": (non_sensitive_points, show_non_sensitive_points),
    }
    selected: list[gpd.GeoDataFrame] = []

    for gdf, on in by_name.values():
        if gdf is not None and len(gdf) > 0:
            if on:
                selected.append(gdf)

    if not selected:
        return None
    base = selected[0]
    return gpd.GeoDataFrame(pd.concat(selected, ignore_index=True), geometry=base.geometry.name, crs=base.crs)


def _apply_single_lst_mask(
    points: gpd.GeoDataFrame,
    mask_layer: gpd.GeoDataFrame | None,
    near_m: int = 0,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame | None]:
    if points is None or len(points) == 0:
        return points, None
    if mask_layer is None or len(mask_layer) == 0:
        return points, None

    pts = points.to_crs(3006).copy()

    def _safe_union(layer: gpd.GeoDataFrame):
        g = layer.to_crs(3006).copy()
        g = g[g.geometry.notna() & (~g.geometry.is_empty)].copy()
        if len(g) == 0:
            return None
        # Repair invalid polygons before union to avoid TopologyException.
        try:
            g["geometry"] = g.geometry.make_valid()
        except Exception:
            pass
        g["geometry"] = g.geometry.buffer(0)
        g = g[g.geometry.notna() & (~g.geometry.is_empty)].copy()
        if len(g) == 0:
            return None
        try:
            return g.geometry.union_all()
        except Exception:
            return g.geometry.unary_union

    mask = _safe_union(mask_layer)
    if mask is None:
        return pts.to_crs(4326), None
    if near_m > 0:
        mask = mask.buffer(float(near_m))
    mask_zone = gpd.GeoDataFrame({"geometry": [mask]}, geometry="geometry", crs=3006).to_crs(4326)
    return pts[pts.geometry.intersects(mask)].to_crs(4326), mask_zone


def _analysis_summary(
    points: gpd.GeoDataFrame,
    units: gpd.GeoDataFrame,
    unit_col: str,
    metric_mode: str,
) -> gpd.GeoDataFrame:
    if points is None or len(points) == 0 or units is None or len(units) == 0:
        if units is None or len(units) == 0:
            return gpd.GeoDataFrame(columns=["kategori", "n", "geometry"], geometry="geometry", crs=4326)
        u0 = gpd.GeoDataFrame(units.copy(), geometry="geometry", crs=4326).copy()
        u0 = u0.rename(columns={unit_col: "kategori"}) if unit_col in u0.columns else u0
        if "kategori" not in u0.columns:
            u0["kategori"] = "Omrade"
        u0["n"] = 0
        u0["geometry"] = u0.geometry.representative_point()
        return u0[["kategori", "n", "geometry"]].to_crs(4326)
    p = points.to_crs(3006).copy()
    if metric_mode == "Unika respondenter":
        dedup_cols = ["respid"] if "respid" in p.columns else (["record"] if "record" in p.columns else None)
    else:
        dedup_cols = [c for c in ["record", "respid", "plats_nr", "lat", "lon"] if c in p.columns]
    if dedup_cols:
        p = p.drop_duplicates(subset=dedup_cols)
    u = gpd.GeoDataFrame(units.copy(), geometry="geometry", crs=4326).to_crs(3006)
    # Repair unit geometries to avoid empty/invalid representative points in edge cases.
    try:
        u["geometry"] = u.geometry.make_valid()
    except Exception:
        pass
    u["geometry"] = u.geometry.buffer(0)
    u = u[u.geometry.notna() & (~u.geometry.is_empty)].copy()
    if len(u) == 0:
        return gpd.GeoDataFrame(columns=["kategori", "n", "geometry"], geometry="geometry", crs=4326)
    p_join = gpd.GeoDataFrame(p[[p.geometry.name]].rename(columns={p.geometry.name: "geometry"}), geometry="geometry", crs=p.crs)
    u_join = gpd.GeoDataFrame(u[[unit_col, u.geometry.name]].rename(columns={unit_col: "kategori", u.geometry.name: "geometry"}), geometry="geometry", crs=u.crs)
    joined = gpd.sjoin(p_join, u_join, how="inner", predicate="intersects")
    counts = joined.groupby("kategori").size().rename("n").reset_index()
    out = u_join.merge(counts, on="kategori", how="left")
    out["n"] = out["n"].fillna(0).astype(int)
    # Bubble position:
    # - if a category has hits, place bubble at centroid of the hit points (more visible/contextual)
    # - else fallback to analysis-unit representative point
    rep = gpd.GeoSeries(out.geometry, crs=3006).representative_point()
    center_by_cat: dict[str, object] = {}
    if len(joined) > 0:
        j = gpd.GeoDataFrame(joined[["kategori", "geometry"]].copy(), geometry="geometry", crs=3006)
        for cat, grp in j.groupby("kategori"):
            try:
                c = grp.geometry.unary_union.centroid
                if c is not None and not getattr(c, "is_empty", False):
                    center_by_cat[str(cat)] = c
            except Exception:
                pass

    new_geom = []
    for i, row in out.iterrows():
        cat = str(row["kategori"])
        g = center_by_cat.get(cat)
        if g is None or getattr(g, "is_empty", False):
            g = rep.iloc[i]
        new_geom.append(g)
    out["geometry"] = new_geom
    return gpd.GeoDataFrame(out, geometry="geometry", crs=3006).to_crs(4326)


def _add_analysis_bubbles(m: folium.Map, summary: gpd.GeoDataFrame) -> int:
    if summary is None or len(summary) == 0:
        return 0
    max_n = max(1, int(summary["n"].max()))
    palette = ["#ef4444", "#3b82f6", "#10b981", "#f59e0b", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16"]
    cats = [str(x) for x in summary["kategori"].astype(str).unique().tolist()]
    cat_color = {c: palette[i % len(palette)] for i, c in enumerate(cats)}
    drawn = 0
    for _, row in summary.iterrows():
        n = int(row["n"])
        label = str(row["kategori"])
        pt = row.geometry
        if pt is None or getattr(pt, "is_empty", False):
            continue
        if pt.geom_type != "Point":
            try:
                pt = pt.representative_point()
            except Exception:
                continue
        fill_color = cat_color.get(label, "#ef4444")
        radius = 20 + (36 * ((n / max_n) ** 0.5))
        folium.CircleMarker(
            location=[pt.y, pt.x],
            radius=radius + 4,
            color="#ffffff",
            weight=5,
            fill=False,
            opacity=1.0,
        ).add_to(m)
        folium.CircleMarker(
            location=[pt.y, pt.x],
            radius=radius,
            color="#111827",
            weight=3,
            fill=True,
            fill_color=fill_color,
            fill_opacity=0.85,
            tooltip=f"{label}: {n}",
            popup=folium.Popup(f"{label}<br>Antal: {n}", max_width=320),
        ).add_to(m)
        folium.CircleMarker(
            location=[pt.y, pt.x],
            radius=5,
            color="#111827",
            weight=1,
            fill=True,
            fill_color="#111827",
            fill_opacity=1.0,
        ).add_to(m)
        folium.Marker(
            location=[pt.y, pt.x],
            icon=folium.DivIcon(
                icon_size=(260, 52),
                icon_anchor=(130, 56),
                html=(
                    "<div style=\""
                    "font-size:12px;font-weight:700;color:#111827;"
                    "background:rgba(255,255,255,0.92);"
                    "border:1px solid rgba(17,24,39,0.35);border-radius:8px;"
                    "padding:3px 7px;line-height:1.15;text-align:center;"
                    "box-shadow:0 2px 4px rgba(0,0,0,0.20);"
                    "max-width:260px;white-space:normal;"
                    "transform: translate(-50%, -108%);"
                    f"\">{label}: {n}</div>"
                ),
            ),
        ).add_to(m)
        drawn += 1
    return drawn


def _add_lst_zone_overlay(m: folium.Map, zone: gpd.GeoDataFrame | None) -> None:
    if zone is None or len(zone) == 0:
        return
    folium.GeoJson(
        zone,
        name="Analyszon",
        style_function=lambda _: {
            "color": "#0ea5e9",
            "weight": 2.0,
            "dashArray": "6,6",
            "fillColor": "#38bdf8",
            "fillOpacity": 0.16,
        },
    ).add_to(m)


area_mode_options = ["Hela länet", "Samtliga kommuner", "Samtliga kommungrupper"]
kommun_code_by_name: dict[str, str] = {}
group_id_by_name: dict[str, str] = {}
try:
    _k, _kg = _cached_admin_layers(str(repo_root))
    kp = _k[["kommunnamn", "kommunkod"]].dropna().drop_duplicates().sort_values("kommunnamn")
    gp = _kg[["kommungrupp_namn", "kommungrupp_id"]].dropna().drop_duplicates().sort_values("kommungrupp_namn")
    kommun_code_by_name = {str(r["kommunnamn"]): _numkey(pd.Series([r["kommunkod"]])).iloc[0] for _, r in kp.iterrows()}
    group_id_by_name = {str(r["kommungrupp_namn"]): _numkey(pd.Series([r["kommungrupp_id"]])).iloc[0] for _, r in gp.iterrows()}
    area_mode_options += [f"Kommun: {x}" for x in kommun_code_by_name.keys()]
    area_mode_options += [f"Kommungrupp: {x}" for x in group_id_by_name.keys()]
except Exception:
    pass

with st.sidebar:
    st.header("Kartinstallningar")
    selected_area = st.selectbox("Arbetsomrade", area_mode_options, index=0)
    filter_mode = st.selectbox("Filtergrund", ["Hemvist (QI)", "Koordinatlage (spatialt)"], index=0)
    if filter_mode == "Hemvist (QI)":
        st.caption(
            "Hemvist (QI): For kommun och kommungrupp visas punkter fran respondenter som bor i valt arbetsomrade (Q1). "
            "Punkterna kan ligga var som helst i lanet."
        )

    st.subheader("Bakgrund")
    show_lan_boundary = st.checkbox("Visa länsgräns", value=False)
    show_kommungrupper = st.checkbox("Visa kommungrupper", value=False)
    show_kommuner = st.checkbox("Visa kommungrans", value=False)

    st.subheader("Lager fran Lansstyrelsens geodatakatalog")
    show_sty = st.checkbox("Lstw.LstW_Landskapstyper", value=False)
    show_kar = st.checkbox("Lstw.LstW_Landskapskaraktarsomraden", value=False)
    show_rorligt_friluftsliv = st.checkbox("lst.LST_RI_Rorligt_friluftsliv_MB4kap2", value=False)
    show_utbyggnad_vindkraft = st.checkbox("Lstw.LstW_Regional_analys_utbyggnad_vindkraft_juni2024", value=False)
    show_nature_reserve = st.checkbox("qgis_osm.naturereserve", value=False)
    show_kulturmiljovard = st.checkbox("raa.RAA_RI_kulturmiljovard_MB3kap6", value=False)

    st.subheader("Punktlager")
    show_plats1_points = st.checkbox("Visa Plats 1-punkter (farg efter kommungrupp)", value=True)
    show_plats2_points = st.checkbox("Visa Plats 2-punkter (farg efter kommungrupp)", value=False)
    show_sensitive_points = st.checkbox("Visa extra kansliga punkter", value=False)
    show_non_sensitive_points = st.checkbox("Visa inte extra kansliga punkter", value=False)

    st.subheader("Vind")
    show_wind_turbines = st.checkbox("Visa vindkraftverk (Dalarna + 30 km)", value=False)

main_col, right_col = st.columns([4.8, 1.2], gap="medium")
with right_col:
    st.subheader("Punktbuffer")
    point_buffer_m = st.slider("Buffer runt tanda punktlager (meter)", 0, 3000, 0, 100, key="point_buffer_right")
    st.subheader("Analys")
    analysis_enabled = st.checkbox("Aktivera analys", value=False)
    analysis_metric = st.selectbox("Matt", ["Punkter", "Unika respondenter"], index=0)
    analysis_near_m = st.slider("Narhetszon runt valt LST-lager (meter)", 0, 3000, 0, 50)

if selected_area == "Hela länet":
    area_kind, area_value = "lan", ""
elif selected_area == "Samtliga kommuner":
    area_kind, area_value = "all_kommuner", ""
elif selected_area == "Samtliga kommungrupper":
    area_kind, area_value = "all_kommungrupper", ""
elif selected_area.startswith("Kommun: "):
    area_kind, area_value = "kommun", selected_area.replace("Kommun: ", "", 1)
else:
    area_kind, area_value = "kommungrupp", selected_area.replace("Kommungrupp: ", "", 1)


def _analysis_scope_label(kind: str, value: str) -> str:
    if kind == "lan":
        return "hela länet"
    if kind == "all_kommuner":
        return "samtliga kommuner"
    if kind == "all_kommungrupper":
        return "samtliga kommungrupper"
    if kind == "kommun":
        return f"kommun: {value}"
    if kind == "kommungrupp":
        return f"kommungrupp: {value}"
    return kind


if analysis_enabled:
    mismatch = False
    if show_kommungrupper and area_kind not in {"kommungrupp", "all_kommungrupper"}:
        mismatch = True
    if show_kommuner and area_kind not in {"kommun", "all_kommuner"}:
        mismatch = True
    if show_lan_boundary and area_kind != "lan":
        mismatch = True
    if mismatch:
        with right_col:
            st.info(
                "OBS: Analysen styrs av Arbetsområde, inte av vilka bakgrundslager som visas. "
                f"Nu används: {_analysis_scope_label(area_kind, area_value)}."
            )

active_point_labels: list[str] = []
if show_plats1_points:
    active_point_labels.append("Plats 1")
if show_plats2_points:
    active_point_labels.append("Plats 2")
if show_sensitive_points:
    active_point_labels.append("extra kansliga")
if show_non_sensitive_points:
    active_point_labels.append("inte extra kansliga")
if len(active_point_labels) == 0:
    q_points = "aktiva punktlager"
elif len(active_point_labels) == 1:
    q_points = active_point_labels[0]
else:
    q_points = ", ".join(active_point_labels[:-1]) + " och " + active_point_labels[-1]

if area_kind == "lan":
    q_area = "hela länet"
elif area_kind == "kommun":
    q_area = f"kommunen {area_value}"
elif area_kind == "kommungrupp":
    q_area = f"kommungruppen {area_value}"
elif area_kind == "all_kommuner":
    q_area = "samtliga kommuner"
else:
    q_area = "samtliga kommungrupper"

sty, kar = _empty_gdf(), _empty_gdf()
sty_field, kar_field = "geometry", "geometry"
if show_sty or show_kar:
    try:
        sty, kar = _cached_base_layers(str(repo_root))
        sty_field, kar_field = choose_default_field(sty), choose_default_field(kar)
    except Exception:
        st.sidebar.warning("Kunde inte lasa in landskapstyper/landskapskaraktar fran lokala datafiler i deployment.")
        show_sty = False
        show_kar = False

theme_layers: dict[str, gpd.GeoDataFrame] = {}
for key, on in [
    ("rorligt_friluftsliv", show_rorligt_friluftsliv),
    ("utbyggnad_vindkraft", show_utbyggnad_vindkraft),
    ("nature_reserve", show_nature_reserve),
    ("kulturmiljovard", show_kulturmiljovard),
]:
    if on:
        try:
            theme_layers[key] = _cached_theme_layer(str(repo_root), key)
        except Exception:
            st.sidebar.warning(f"Kunde inte lasa in lagret: {key}")

kommuner, kommungrupper, lan_boundary = None, None, None
if show_kommuner or show_kommungrupper or analysis_enabled or area_kind in {"kommun", "kommungrupp", "all_kommuner", "all_kommungrupper"}:
    try:
        kommuner, kommungrupper = _cached_admin_layers(str(repo_root))
    except Exception:
        st.sidebar.warning("Kunde inte lasa in administrativa lager (lokal bundle eller DB).")
        show_kommuner = False
        show_kommungrupper = False
if show_lan_boundary or analysis_enabled or area_kind == "lan":
    try:
        lan_boundary = _cached_lan_boundary()
        if lan_boundary is None or len(lan_boundary) == 0:
            st.sidebar.warning("Länsgräns-lagret saknar polygon/linje-geometri.")
            show_lan_boundary = False
    except Exception:
        st.sidebar.warning("Kunde inte läsa in länsgräns.")
        show_lan_boundary = False

plats1_points = plats2_points = sensitive_points = non_sensitive_points = None
if show_plats1_points or show_plats2_points or show_sensitive_points or show_non_sensitive_points or analysis_enabled:
    locked_layers = _cached_locked_point_layers(str(repo_root))
    if locked_layers is not None:
        plats1_points, plats2_points, sensitive_points, non_sensitive_points = locked_layers
    else:
        try:
            plats1_points, plats2_points = _cached_plats_layers()
            sensitive_points, non_sensitive_points = _cached_sensitivity_layers()
        except Exception:
            st.sidebar.warning("Kunde inte lasa in punktlager fran DB och inga lasa lager hittades lokalt.")
            show_plats1_points = False
            show_plats2_points = False
            show_sensitive_points = False
            show_non_sensitive_points = False

    plats1_points = _apply_area_filter(plats1_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    plats2_points = _apply_area_filter(plats2_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    sensitive_points = _apply_area_filter(sensitive_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    non_sensitive_points = _apply_area_filter(non_sensitive_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)

wind_turbines = None
if show_wind_turbines:
    try:
        wind_turbines, _ = _cached_wind_layers(str(repo_root), 30000)
    except Exception:
        st.sidebar.warning("Kunde inte lasa in vindkraftverk i deployment.")
        show_wind_turbines = False

# Compatibility guard for older build_map implementations that always derive map center from `sty`.
if sty is None or len(sty) == 0:
    if lan_boundary is not None and len(lan_boundary) > 0:
        sty = gpd.GeoDataFrame(geometry=lan_boundary.geometry.copy(), crs=lan_boundary.crs)
    elif kommuner is not None and len(kommuner) > 0:
        sty = gpd.GeoDataFrame(geometry=kommuner.geometry.copy(), crs=kommuner.crs)
    elif kommungrupper is not None and len(kommungrupper) > 0:
        sty = gpd.GeoDataFrame(geometry=kommungrupper.geometry.copy(), crs=kommungrupper.crs)
    else:
        sty = _fallback_center_layer()

m = _build_map_compat(
    sty=sty,
    kar=kar,
    sty_field=sty_field,
    kar_field=kar_field,
    show_sty=show_sty,
    show_kar=show_kar,
    lan_boundary=lan_boundary,
    show_lan_boundary=show_lan_boundary,
    theme_layers=theme_layers,
    theme_visibility={k: True for k in theme_layers.keys()},
    kommuner=kommuner,
    kommungrupper=kommungrupper,
    show_kommuner=show_kommuner,
    show_kommungrupper=show_kommungrupper,
    plats1_points=plats1_points,
    plats2_points=plats2_points,
    show_plats1_points=show_plats1_points,
    show_plats2_points=show_plats2_points,
    sensitive_points=sensitive_points,
    non_sensitive_points=non_sensitive_points,
    show_sensitive_points=show_sensitive_points,
    show_non_sensitive_points=show_non_sensitive_points,
    sensitive_buffer_m=point_buffer_m,
    sty_opacity=0.6,
    show_landscape_colored_points=False,
    show_landscape_aggregated_points=False,
    wind_turbines=wind_turbines,
    show_wind_turbines=show_wind_turbines,
)

if analysis_enabled:
    analysis_pts = _analysis_points(
        show_plats1_points,
        show_plats2_points,
        show_sensitive_points,
        show_non_sensitive_points,
        plats1_points,
        plats2_points,
        sensitive_points,
        non_sensitive_points,
    )
    lst_active_layers: list[tuple[str, gpd.GeoDataFrame, str | None]] = []
    if show_sty and sty is not None and len(sty) > 0:
        lst_active_layers.append(("sty", sty, sty_field))
    if show_kar and kar is not None and len(kar) > 0:
        lst_active_layers.append(("kar", kar, kar_field))
    for key in ["rorligt_friluftsliv", "utbyggnad_vindkraft", "nature_reserve", "kulturmiljovard"]:
        if key in theme_layers:
            layer = theme_layers[key]
            lst_active_layers.append((key, layer, choose_default_field(layer)))

    selected_lst_layer = None
    analysis_blocked_multi_lst = False
    with right_col:
        if len(lst_active_layers) > 1:
            st.warning("Analysen stoder max ett LST-lager at gangen. Slack till ett lager for maskad analys.")
            analysis_blocked_multi_lst = True
        elif len(lst_active_layers) == 1:
            st.caption("Analyslage: arbetsomrade + 1 aktivt LST-lager.")
            selected_key, selected_lst_layer, selected_field = lst_active_layers[0]
            if selected_field is not None and selected_field in selected_lst_layer.columns:
                vals = (
                    selected_lst_layer[selected_field]
                    .dropna()
                    .astype(str)
                    .str.strip()
                )
                uniq = sorted([v for v in vals.unique().tolist() if v != ""])
                if len(uniq) > 0:
                    selected_cat = st.selectbox(
                        "Kategori i valt LST-lager (valfritt)",
                        ["Alla kategorier"] + uniq,
                        index=0,
                        key=f"lst_cat_{selected_key}",
                    )
                    if selected_cat != "Alla kategorier":
                        selected_lst_layer = selected_lst_layer[
                            selected_lst_layer[selected_field].astype(str).str.strip() == selected_cat
                        ].copy()
                        st.caption(f"Kategori: {selected_cat}")
        else:
            st.caption("Analyslage: endast arbetsomrade (inget aktivt LST-lager).")

    if analysis_blocked_multi_lst:
        q_suffix = " utan LST-mask (flera LST-lager ar tanda)"
    elif selected_lst_layer is None:
        q_suffix = ""
    else:
        q_suffix = " inom valt LST-lager"
    st.caption(f"Fragga: Hur manga {q_points.lower()} finns i {q_area}{q_suffix}? Svaret visas i kartan.")

    if analysis_blocked_multi_lst:
        summary = gpd.GeoDataFrame(columns=["kategori", "n", "geometry"], geometry="geometry", crs=4326)
        lst_zone = None
        bubbles_drawn = 0
    else:
        analysis_pts, lst_zone = _apply_single_lst_mask(analysis_pts, selected_lst_layer, analysis_near_m)
        units, unit_col = _analysis_units(area_kind, area_value, lan_boundary, kommuner, kommungrupper)
        summary = _analysis_summary(analysis_pts, units, unit_col, analysis_metric)
        _add_lst_zone_overlay(m, lst_zone)
        bubbles_drawn = _add_analysis_bubbles(m, summary)
    with right_col:
        if analysis_blocked_multi_lst:
            st.caption("Analys pausad: valj hogst ett LST-lager.")
        elif summary is not None and len(summary) > 0:
            st.caption(f"Analysen visar {analysis_metric.lower()} i {len(summary)} arbetsomrade(n). Summa n: {int(summary['n'].sum())}.")
            if bubbles_drawn == 0:
                st.warning("Analysresultat finns men bubblor kunde inte ritas (geometriproblem).")
        else:
            st.caption("Ingen traff i analysen med nuvarande val.")

with main_col:
    try:
        folium_static(m, width=1200, height=920)
    except Exception:
        components.html(m.get_root().render(), height=920, scrolling=False)
