from __future__ import annotations

from pathlib import Path
import inspect
import importlib.util
import json
import re
import sys
import unicodedata

import folium
import geopandas as gpd
import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image
from streamlit_folium import folium_static
from shapely.geometry import Point

_mf_path = Path(__file__).resolve().parent / "scripts" / "map_factory.py"
try:
    import scripts.map_factory as map_factory
    imported_path = Path(getattr(map_factory, "__file__", "")).resolve()
    if imported_path != _mf_path.resolve():
        raise ImportError(f"Resolved unexpected scripts.map_factory path: {imported_path}")
except Exception:
    # Streamlit Cloud may resolve "scripts" to a non-local namespace.
    # Force loading local scripts/map_factory.py when that happens.
    if str(_mf_path.parent.parent) not in sys.path:
        sys.path.insert(0, str(_mf_path.parent.parent))
    _spec = importlib.util.spec_from_file_location("local_map_factory", _mf_path)
    if _spec is None or _spec.loader is None:
        raise ImportError(f"Could not load local map_factory at {_mf_path}")
    map_factory = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(map_factory)

build_map = map_factory.build_map
choose_default_field = map_factory.choose_default_field
load_wind_turbines_dalarna_buffer = map_factory.load_wind_turbines_dalarna_buffer


st.set_page_config(page_title="Energiomställning i Dalarna", layout="wide")
st.title("Energiomställning i Dalarna")

repo_root = Path(__file__).resolve().parent
cloud_dir = repo_root / "data" / "cloud"

LST_BUNDLE_GPKG = "lst_layers.gpkg"
ADMIN_BUNDLE_GPKG = "admin_boundaries.gpkg"
LOCKED_POINTS_GPKG = "novus_locked_points.gpkg"
BOREAL_RASTER_OVERLAY_JSON = "tathetsanalys_3000m_procent.overlay.json"

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
CODE_TO_GROUP_ID = {
    "2084": "6",
    "2083": "6",
    "2082": "6",
    "2080": "3",
    "2081": "3",
    "2023": "1",
    "2039": "1",
    "2021": "1",
    "2062": "5",
    "2034": "5",
    "2031": "2",
    "2029": "2",
    "2026": "2",
    "2061": "4",
    "2085": "4",
}


REQUIRED_CLOUD_FILES = [
    LOCKED_POINTS_GPKG,
    ADMIN_BUNDLE_GPKG,
    LST_BUNDLE_GPKG,
]
REQUIRED_GPKG_LAYERS = {
    LOCKED_POINTS_GPKG: {"plats_1", "plats_2", "plats_3_sensitive", "plats_4_not_sensitive"},
    ADMIN_BUNDLE_GPKG: {"lan", "kommuner", "kommungrupper"},
    LST_BUNDLE_GPKG: set(LST_BUNDLE_LAYER_BY_KEY.values()),
}


def _list_vector_layers(path: Path) -> set[str]:
    if hasattr(gpd, "list_layers"):
        layers_df = gpd.list_layers(path)
        if "name" in layers_df.columns:
            return set(layers_df["name"].astype(str).tolist())
    try:
        import fiona

        return {str(name) for name in fiona.listlayers(str(path))}
    except Exception as exc:
        raise RuntimeError(exc) from exc


def _validate_cloud_foundation() -> list[str]:
    problems: list[str] = []
    for file_name in REQUIRED_CLOUD_FILES:
        path = cloud_dir / file_name
        if not path.exists():
            problems.append(f"Missing file: data/cloud/{file_name}")
            continue

        try:
            available_layers = _list_vector_layers(path)
        except Exception as exc:
            problems.append(f"Could not read layers in data/cloud/{file_name}: {exc}")
            continue

        required_layers = REQUIRED_GPKG_LAYERS.get(file_name, set())
        missing_layers = sorted(required_layers - available_layers)
        if missing_layers:
            problems.append(
                f"Missing layers in data/cloud/{file_name}: {', '.join(missing_layers)}"
            )
    return problems


foundation_errors = _validate_cloud_foundation()
if foundation_errors:
    st.error("Cloud foundation is incomplete. App requires exactly 3 GPKG bundles.")
    for msg in foundation_errors:
        st.error(msg)
    st.stop()


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


def _load_admin_layers_local(repo: Path) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    admin_bundle = repo / "data" / "cloud" / ADMIN_BUNDLE_GPKG
    kommuner = _read_vector_4326(admin_bundle, layer="kommuner", default_crs=3006)
    kommungrupper = _read_vector_4326(admin_bundle, layer="kommungrupper", default_crs=3006)
    kommuner = _normalize_kommuner_schema(kommuner)
    kommungrupper = _normalize_kommungrupper_schema(kommungrupper)
    return kommuner, kommungrupper


@st.cache_data(show_spinner=False, ttl=300)
def _cached_admin_layers(repo_root_str: str):
    repo = Path(repo_root_str)
    return _load_admin_layers_local(repo)


@st.cache_data(show_spinner=False, ttl=300)
def _cached_lan_boundary():
    admin_bundle = cloud_dir / ADMIN_BUNDLE_GPKG
    return _normalize_lan_boundary_schema(
        _read_vector_4326(admin_bundle, layer="lan", default_crs=3006)
    )


@st.cache_data(show_spinner=False, ttl=300)
def _cached_base_layers(repo_root_str: str):
    repo = Path(repo_root_str)
    lst_bundle = repo / "data" / "cloud" / LST_BUNDLE_GPKG
    return (
        _read_vector_4326(lst_bundle, layer=LST_BUNDLE_LAYER_BY_KEY["landskapstyp"], default_crs=3006),
        _read_vector_4326(lst_bundle, layer=LST_BUNDLE_LAYER_BY_KEY["landskapskaraktar"], default_crs=3006),
    )


@st.cache_data(show_spinner=False, ttl=300)
def _cached_theme_layer(repo_root_str: str, key: str):
    repo = Path(repo_root_str)
    lst_bundle = repo / "data" / "cloud" / LST_BUNDLE_GPKG
    if key not in LST_BUNDLE_LAYER_BY_KEY:
        raise KeyError(f"Unknown layer key: {key}")
    return _read_vector_4326(lst_bundle, layer=LST_BUNDLE_LAYER_BY_KEY[key], default_crs=3006)


def _locked_points_gpkg_path(repo_root_str: str) -> Path:
    return Path(repo_root_str) / "data" / "cloud" / LOCKED_POINTS_GPKG


def _file_cache_token(path: Path) -> str:
    if not path.exists():
        return f"{path}:missing"
    stp = path.stat()
    return f"{path}:{stp.st_mtime_ns}:{stp.st_size}"


def _locked_points_cache_token(repo_root_str: str) -> str:
    return _file_cache_token(_locked_points_gpkg_path(repo_root_str))


@st.cache_data(show_spinner=False, ttl=300)
def _cached_locked_point_layers(repo_root_str: str, cache_token: str):
    _ = cache_token
    gpkg = _locked_points_gpkg_path(repo_root_str)
    admin_bundle = Path(repo_root_str) / "data" / "cloud" / ADMIN_BUNDLE_GPKG

    code_to_name: dict[str, str] = {}
    code_to_gid: dict[str, str] = {}
    try:
        km = _read_vector_4326(admin_bundle, layer="kommuner", default_crs=3006)
        if "kommunkod" in km.columns and "kommunnamn" in km.columns:
            code_to_name = dict(
                zip(
                    _numkey(km["kommunkod"]),
                    km["kommunnamn"].astype("string").fillna("").astype(str),
                )
            )
        if "kommunkod" in km.columns and "kommungrupp_id" in km.columns:
            code_to_gid = dict(zip(_numkey(km["kommunkod"]), _numkey(km["kommungrupp_id"])))
    except Exception:
        code_to_name = {}
        code_to_gid = {}
    if not code_to_gid:
        code_to_gid = CODE_TO_GROUP_ID

    plats1 = gpd.read_file(gpkg, layer="plats_1").to_crs(4326)
    plats2 = gpd.read_file(gpkg, layer="plats_2").to_crs(4326)
    sensitive = gpd.read_file(gpkg, layer="plats_3_sensitive").to_crs(4326)
    non_sensitive = gpd.read_file(gpkg, layer="plats_4_not_sensitive").to_crs(4326)

    def _pick_string_series(out: gpd.GeoDataFrame, candidates: list[str]) -> pd.Series:
        for c in candidates:
            if c in out.columns:
                return out[c].astype("string")
        return pd.Series([pd.NA] * len(out), index=out.index, dtype="string")

    def _normalize_point_schema(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if gdf is None or len(gdf) == 0:
            return gdf
        out = gdf.copy()

        resp_code = _numkey(
            _pick_string_series(out, ["resp_kom", "home_kommunkod", "Q1", "q1", "hemvist_q1", "hemvist_kommunkod"])
        )
        coord_code = _numkey(_pick_string_series(out, ["coord_kom", "kommunkod", "admin_2_kod"]))

        out["resp_kom"] = resp_code.where(resp_code.ne(""), pd.NA)
        out["coord_kom"] = coord_code.where(coord_code.ne(""), pd.NA)
        out["home_kommunkod"] = out["resp_kom"]
        out["Q1"] = out["resp_kom"]
        out["kommunkod"] = out["coord_kom"]

        resp_gid_from_col = _numkey(_pick_string_series(out, ["resp_komgrp", "home_kommungrupp_id_current", "home_kommungrupp_id"]))
        derived_home_gid = _numkey(out["home_kommunkod"]).map(code_to_gid)
        home_gid = resp_gid_from_col.where(resp_gid_from_col.ne(""), derived_home_gid)
        out["home_kommungrupp_id_current"] = home_gid.where(home_gid.ne(""), pd.NA)

        derived_home_gname = _numkey(out["home_kommunkod"]).map(CODE_TO_GROUP_NAME)
        out["home_kommungrupp_current"] = derived_home_gname.where(derived_home_gname.ne(""), pd.NA)
        out["home_kommungrupp"] = out["home_kommungrupp_current"]

        coord_gid = _numkey(out["coord_kom"]).map(code_to_gid)
        out["coord_kommungrupp_id_current"] = coord_gid.where(coord_gid.ne(""), pd.NA)
        coord_gname = _numkey(out["coord_kom"]).map(CODE_TO_GROUP_NAME)
        out["coord_kommungrupp_current"] = coord_gname.where(coord_gname.ne(""), pd.NA)

        if "admin_2" in out.columns:
            old_name = out["admin_2"].astype("string")
        else:
            old_name = pd.Series([pd.NA] * len(out), index=out.index, dtype="string")
        mapped_name = _numkey(out["kommunkod"]).map(code_to_name)
        out["admin_2"] = old_name.where(old_name.fillna("").str.strip() != "", mapped_name)

        if "kommungrupp" in out.columns:
            old_grp = out["kommungrupp"].astype("string")
        else:
            old_grp = pd.Series([pd.NA] * len(out), index=out.index, dtype="string")
        out["kommungrupp"] = old_grp.where(old_grp.fillna("").str.strip() != "", out["home_kommungrupp_id_current"])

        for c in ["plats_nr", "plats_fritext", "record", "respid", "lat", "lon"]:
            if c not in out.columns:
                out[c] = pd.NA

        return out

    plats1 = _normalize_point_schema(plats1)
    plats2 = _normalize_point_schema(plats2)
    sensitive = _normalize_point_schema(sensitive)
    non_sensitive = _normalize_point_schema(non_sensitive)

    return plats1, plats2, sensitive, non_sensitive

@st.cache_data(show_spinner=False, ttl=600)
def _cached_wind_layers(repo_root_str: str, buffer_m: int):
    return load_wind_turbines_dalarna_buffer(Path(repo_root_str), buffer_m=buffer_m)


@st.cache_data(show_spinner=False, ttl=300)
def _cached_raster_overlay(repo_root_str: str, overlay_json: str):
    cfg_path = Path(repo_root_str) / "data" / "cloud" / overlay_json
    if not cfg_path.exists():
        return None

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    image_name = str(cfg.get("image", "")).strip()
    if not image_name:
        raise RuntimeError(f"{overlay_json}: missing 'image'")

    image_path = (cfg_path.parent / image_name).resolve()
    if not image_path.exists():
        raise FileNotFoundError(f"Missing raster image: {image_path}")

    bounds_raw = cfg.get("bounds_4326")
    if not isinstance(bounds_raw, list) or len(bounds_raw) != 2:
        raise RuntimeError(f"{overlay_json}: 'bounds_4326' must be [[south, west], [north, east]]")

    try:
        bounds = [
            [float(bounds_raw[0][0]), float(bounds_raw[0][1])],
            [float(bounds_raw[1][0]), float(bounds_raw[1][1])],
        ]
    except Exception as exc:
        raise RuntimeError(f"{overlay_json}: invalid coordinate values in 'bounds_4326'") from exc

    return {
        "name": str(cfg.get("name", "Tathetsanalys boreal region")),
        "image": str(image_path),
        "bounds": bounds,
        "opacity": float(cfg.get("opacity", 1.0)),
        "zindex": int(cfg.get("zindex", 5)),
        "raster_min": int(cfg.get("raster_min", 1)),
        "raster_max": int(cfg.get("raster_max", 94)),
        "sample_image": str((cfg_path.parent / str(cfg.get("sample_image", image_name))).resolve()),
    }


@st.cache_data(show_spinner=False, ttl=300)
def _cached_raster_sampler(repo_root_str: str, overlay_json: str):
    cfg_path = Path(repo_root_str) / "data" / "cloud" / overlay_json
    if not cfg_path.exists():
        return None
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    bounds_raw = cfg.get("bounds_4326")
    if not isinstance(bounds_raw, list) or len(bounds_raw) != 2:
        return None

    sample_name = str(cfg.get("sample_image", cfg.get("image", ""))).strip()
    if not sample_name:
        return None
    sample_path = (cfg_path.parent / sample_name).resolve()
    if not sample_path.exists():
        return None

    with Image.open(sample_path) as img:
        arr = np.asarray(img)
    if arr.ndim == 2:
        values = arr.astype(np.float32)
        alpha = None
    elif arr.ndim == 3 and arr.shape[2] == 2:
        values = arr[:, :, 0].astype(np.float32)
        alpha = arr[:, :, 1].astype(np.uint8)
    elif arr.ndim == 3 and arr.shape[2] >= 3:
        values = arr[:, :, 0].astype(np.float32)
        alpha = arr[:, :, 3].astype(np.uint8) if arr.shape[2] >= 4 else None
    else:
        return None

    bounds = [
        [float(bounds_raw[0][0]), float(bounds_raw[0][1])],
        [float(bounds_raw[1][0]), float(bounds_raw[1][1])],
    ]
    rmin = int(cfg.get("raster_min", 1))
    rmax = int(cfg.get("raster_max", 94))
    return {
        "values": values,
        "alpha": alpha,
        "bounds": bounds,
        "raster_min": rmin,
        "raster_max": rmax,
        "sample_path": str(sample_path),
    }


def _attach_raster_values(gdf: gpd.GeoDataFrame | None, sampler: dict | None, out_col: str = "skoglig_vardekarna") -> gpd.GeoDataFrame | None:
    if gdf is None or len(gdf) == 0 or sampler is None:
        return gdf
    if gdf.crs is None:
        pts = gdf.set_crs(4326)
    else:
        pts = gdf.to_crs(4326) if int(gdf.crs.to_epsg() or 0) != 4326 else gdf

    vals = sampler["values"]
    alpha = sampler["alpha"]
    h, w = vals.shape
    south, west = sampler["bounds"][0]
    north, east = sampler["bounds"][1]
    dx = max(1e-12, (east - west))
    dy = max(1e-12, (north - south))

    xy = np.array([(geom.x, geom.y) if geom is not None else (np.nan, np.nan) for geom in pts.geometry], dtype=np.float64)
    lon = xy[:, 0]
    lat = xy[:, 1]
    inside = np.isfinite(lon) & np.isfinite(lat) & (lon >= west) & (lon <= east) & (lat >= south) & (lat <= north)

    out_vals = np.full(len(pts), np.nan, dtype=np.float64)
    if inside.any():
        cols = np.rint(((lon[inside] - west) / dx) * (w - 1)).astype(np.int64)
        rows = np.rint(((north - lat[inside]) / dy) * (h - 1)).astype(np.int64)
        cols = np.clip(cols, 0, w - 1)
        rows = np.clip(rows, 0, h - 1)
        sampled = vals[rows, cols].astype(np.float64)
        if alpha is not None:
            a = alpha[rows, cols]
            sampled[a == 0] = np.nan
        out_vals[inside] = sampled

    out = pts.copy()
    out[out_col] = out_vals
    return out


def _filter_points_by_raster_range(
    gdf: gpd.GeoDataFrame | None,
    sampler: dict | None,
    vmin: int,
    vmax: int,
    value_col: str = "skoglig_vardekarna",
) -> gpd.GeoDataFrame | None:
    if gdf is None or len(gdf) == 0:
        return gdf
    with_vals = _attach_raster_values(gdf, sampler, out_col=value_col)
    if with_vals is None or len(with_vals) == 0:
        return with_vals
    keep = with_vals[value_col].between(float(vmin), float(vmax), inclusive="both")
    return with_vals[keep.fillna(False)].copy()


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
    out = (
        series.astype("string")
        .str.replace(".0", "", regex=False)
        .str.replace(",0", "", regex=False)
        .str.strip()
    )
    return out.fillna("")


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


def _norm_group_name_safe(value: str) -> str:
    s = unicodedata.normalize("NFKD", str(value))
    s = s.encode("ascii", "ignore").decode("ascii").strip().lower()
    return re.sub(r"[^a-z0-9]+", "", s)


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

    def _first_existing_col(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in gdf.columns:
                return c
        return None

    code_to_gid: dict[str, str] = {}
    if (
        kommuner is not None
        and len(kommuner) > 0
        and "kommunkod" in kommuner.columns
        and "kommungrupp_id" in kommuner.columns
    ):
        km = kommuner[["kommunkod", "kommungrupp_id"]].dropna().drop_duplicates().copy()
        km["kommunkod_norm"] = _numkey(km["kommunkod"])
        km["kommungrupp_id_norm"] = _numkey(km["kommungrupp_id"])
        code_to_gid = dict(zip(km["kommunkod_norm"], km["kommungrupp_id_norm"]))
    if not code_to_gid:
        code_to_gid = CODE_TO_GROUP_ID

    # Spatial filter mode: filter by coordinate municipality code (coord_kom), not polygon clip.
    if filter_mode == "Koordinatläge (spatialt)":
        if area_kind == "kommun":
            code = kommun_code_by_name.get(area_value)
            if code is None:
                return gdf.iloc[0:0].copy()
            coord_col = _first_existing_col(["coord_kom", "kommunkod"])
            if coord_col is None:
                return gdf.iloc[0:0].copy()
            return gdf[_numkey(gdf[coord_col]) == str(code)]

        if area_kind == "kommungrupp":
            gid = group_id_by_name.get(area_value)
            if gid is None:
                return gdf.iloc[0:0].copy()
            gid_norm = str(gid)

            if "coord_kommungrupp_id_current" in gdf.columns:
                coord_gid = _numkey(gdf["coord_kommungrupp_id_current"])
                if (coord_gid.str.len() > 0).any():
                    return gdf[coord_gid == gid_norm]

            coord_col = _first_existing_col(["coord_kom", "kommunkod"])
            if coord_col is None:
                return gdf.iloc[0:0].copy()
            derived_gid = _numkey(gdf[coord_col]).map(code_to_gid)
            return gdf[derived_gid.astype(str) == gid_norm]

        return gdf.iloc[0:0].copy()

    # Hemvist (QI): filter by respondent home (resp_kom), not point location.
    if area_kind == "kommun":
        code = kommun_code_by_name.get(area_value)
        if code is None:
            return gdf.iloc[0:0].copy()
        home_col = _first_existing_col(["resp_kom", "home_kommunkod", "Q1", "q1", "hemvist_q1", "hemvist_kommunkod"])
        if home_col is None:
            return gdf.iloc[0:0].copy()
        return gdf[_numkey(gdf[home_col]) == str(code)]

    if area_kind == "kommungrupp":
        gid = group_id_by_name.get(area_value)
        if gid is None:
            return gdf.iloc[0:0].copy()
        gid_norm = str(gid)

        # Primary: explicit current group-id field on points.
        if "home_kommungrupp_id_current" in gdf.columns:
            gid_col = _numkey(gdf["home_kommungrupp_id_current"])
            if (gid_col.str.len() > 0).any():
                return gdf[gid_col == gid_norm]

        # Secondary: derive kommungrupp from resp_kom/home_kommunkod.
        home_code_col = _first_existing_col(["resp_kom", "home_kommunkod", "Q1", "q1", "hemvist_q1", "hemvist_kommunkod"])
        if home_code_col is not None:
            derived_gid = _numkey(gdf[home_code_col]).map(code_to_gid)
            out = gdf[derived_gid.astype(str) == gid_norm]
            if len(out) > 0 or derived_gid.notna().any():
                return out

        # Secondary: use current group-name field when present.
        if "home_kommungrupp_current" in gdf.columns:
            wanted = _norm_group_name_safe(area_value)
            curr = gdf["home_kommungrupp_current"].fillna("").astype(str).map(_norm_group_name_safe)
            if (curr.str.len() > 0).any():
                return gdf[curr == wanted]

        return gdf.iloc[0:0].copy()

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
        "Extra känsliga": (sensitive_points, show_sensitive_points),
        "Inte extra känsliga": (non_sensitive_points, show_non_sensitive_points),
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
    # Keep one stable bubble position per analysis unit (arbetsomrade),
    # regardless of active LST mask. LST changes count `n`, not bubble placement.
    out["geometry"] = gpd.GeoSeries(out.geometry, crs=3006).representative_point()
    return gpd.GeoDataFrame(out, geometry="geometry", crs=3006).to_crs(4326)


def _add_analysis_bubbles(m: folium.Map, summary: gpd.GeoDataFrame) -> int:
    if summary is None or len(summary) == 0:
        return 0
    max_n = max(1, int(summary["n"].max()))
    palette = ["#ef4444", "#3b82f6", "#10b981", "#f59e0b", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16"]
    cats = [str(x) for x in summary["kategori"].astype(str).unique().tolist()]
    cat_color = {c: palette[i % len(palette)] for i, c in enumerate(cats)}

    # Keep kommungrupp bubble colors consistent with point/group palette when category labels match.
    group_palette = getattr(
        map_factory,
        "GROUP_PALETTE",
        {1: "#4e79a7", 2: "#f28e2b", 3: "#59a14f", 4: "#e15759", 5: "#76b7b2", 6: "#af7aa1"},
    )
    gid_by_name = globals().get("group_id_by_name", {})
    gid_by_name_norm = {_norm_group_name_safe(str(k)): str(v) for k, v in gid_by_name.items()}

    def _bubble_color(label: str) -> str:
        gid = gid_by_name_norm.get(_norm_group_name_safe(label))
        if gid:
            try:
                return group_palette.get(int(float(gid)), cat_color.get(label, "#ef4444"))
            except Exception:
                pass
        return cat_color.get(label, "#ef4444")

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
        fill_color = _bubble_color(label)
        radius = 20 + (36 * ((n / max_n) ** 0.5))
        folium.CircleMarker(
            location=[pt.y, pt.x],
            radius=radius,
            color="#9ca3af",
            weight=1.2,
            fill=True,
            fill_color=fill_color,
            fill_opacity=0.85,
            tooltip=f"{label}: {n}",
            popup=folium.Popup(f"{label}<br>Antal: {n}", max_width=320),
        ).add_to(m)
        folium.Marker(
            location=[pt.y, pt.x],
            icon=folium.DivIcon(
                icon_size=(1, 1),
                icon_anchor=(0, 0),
                html=(
                    "<div style=\""
                    "display:inline-block;"
                    "font-size:12px;font-weight:700;color:#111827;"
                    "background:rgba(255,255,255,0.92);"
                    "border:1px solid rgba(17,24,39,0.35);border-radius:8px;"
                    "padding:3px 7px;line-height:1.15;text-align:center;"
                    "box-shadow:0 2px 4px rgba(0,0,0,0.20);"
                    "white-space:nowrap;"
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


def _render_boreal_density_legend() -> None:
    legend_items = [
        ("1-30", "#1b5e20", "Morkgron"),
        ("31-60", "#8bc34a", "Ljusgron"),
        ("61-70", "#fdd835", "Gul"),
        ("71-80", "#fb8c00", "Orange"),
        ("81-90", "#e53935", "Rod"),
        ("91-94", "#7f0000", "Morkrod"),
    ]
    rows = []
    for value_range, color, label in legend_items:
        rows.append(
            (
                "<div style=\"display:flex;align-items:center;gap:8px;margin:0 0 4px 0;\">"
                f"<span style=\"display:inline-block;width:14px;height:14px;border-radius:3px;"
                f"background:{color};border:1px solid rgba(0,0,0,0.18);\"></span>"
                f"<span><strong>{value_range}</strong> {label}</span>"
                "</div>"
            )
        )
    st.caption("Farglegend: skoglig vardekarna")
    st.markdown(
        (
            "<div style=\"font-size:0.85rem;line-height:1.2;margin:-0.15rem 0 0.35rem 0;\">"
            + "".join(rows)
            + "</div>"
        ),
        unsafe_allow_html=True,
    )


area_mode_options = ["Hela länet", "Samtliga kommuner", "Samtliga kommungrupper"]
kommun_code_by_name: dict[str, str] = {}
group_id_by_name: dict[str, str] = {}
_k, _kg = _cached_admin_layers(str(repo_root))
kp = _k[["kommunnamn", "kommunkod"]].dropna().drop_duplicates().sort_values("kommunnamn")
gp = _kg[["kommungrupp_namn", "kommungrupp_id"]].dropna().drop_duplicates().sort_values("kommungrupp_namn")
kommun_code_by_name = {str(r["kommunnamn"]): _numkey(pd.Series([r["kommunkod"]])).iloc[0] for _, r in kp.iterrows()}
group_id_by_name = {str(r["kommungrupp_namn"]): _numkey(pd.Series([r["kommungrupp_id"]])).iloc[0] for _, r in gp.iterrows()}
area_mode_options += [f"Kommun: {x}" for x in kommun_code_by_name.keys()]
area_mode_options += [f"Kommungrupp: {x}" for x in group_id_by_name.keys()]

with st.sidebar:
    st.header("Kartinställningar")
    selected_area = st.selectbox("Arbetsområde", area_mode_options, index=0)
    filter_mode = st.selectbox("Filtergrund", ["Hemvist (QI)", "Koordinatläge (spatialt)"], index=0)
    if filter_mode == "Hemvist (QI)":
        st.caption(
            "Hemvist (QI): För kommun och kommungrupp visas punkter från respondenter som bor i valt arbetsområde (resp_kom). "
            "Punkterna kan ligga var som helst i länet."
        )

    st.subheader("Bakgrund")
    show_lan_boundary = st.checkbox("Visa länsgräns", value=False)
    show_kommungrupper = st.checkbox("Visa kommungrupper", value=False)
    show_kommuner = st.checkbox("Visa kommungräns", value=False)
    if hasattr(st, "toggle"):
        satellite_base = st.toggle("Satellitbakgrund", value=False)
    else:
        satellite_base = st.checkbox("Satellitbakgrund", value=False)

    st.subheader("Lager från Länsstyrelsens geodatakatalog")
    show_sty = st.checkbox("Landskapstyper.lst", value=False)
    show_kar = st.checkbox("Landskapskaraktärsområden.lst", value=False)
    show_rorligt_friluftsliv = st.checkbox("Rörligt friluftsliv.lst", value=False)
    show_utbyggnad_vindkraft = st.checkbox("Utbyggnad av vindkraft.lst", value=False)
    show_nature_reserve = st.checkbox("Naturreservat.osm", value=False)
    show_kulturmiljovard = st.checkbox("Kulturmiljövård.lst", value=False)
    show_boreal_density = st.checkbox("Tathetsanalys boreal region (raster)", value=False)
    filter_points_by_boreal = st.checkbox("Filtrera alla punktlager med skoglig värdekärna", value=False)
    boreal_min_val, boreal_max_val = 1, 94
    if show_boreal_density or filter_points_by_boreal:
        try:
            boreal_meta = _cached_raster_overlay(str(repo_root), BOREAL_RASTER_OVERLAY_JSON)
            if boreal_meta is not None:
                boreal_min_val = int(boreal_meta.get("raster_min", 1))
                boreal_max_val = int(boreal_meta.get("raster_max", 94))
        except Exception:
            pass
    if filter_points_by_boreal:
        boreal_value_range = st.slider(
            f"Täthetsvärde ({boreal_min_val}-{boreal_max_val})",
            boreal_min_val,
            boreal_max_val,
            (boreal_min_val, boreal_max_val),
            1,
        )
    if show_boreal_density or filter_points_by_boreal:
        _render_boreal_density_legend()
    else:
        boreal_value_range = (boreal_min_val, boreal_max_val)

    st.subheader("Betydelsefulla platser")
    st.caption("Färg visar kommungrupp.")
    show_plats1_points = st.checkbox("Vald plats 1", value=True)
    show_plats2_points = st.checkbox("Vald plats 2", value=False)
    show_sensitive_points = st.checkbox("Valda platser som är extra känsliga för ny infrastruktur", value=False)
    show_non_sensitive_points = st.checkbox("Valda platser som INTE är känsliga för ny infrastruktur", value=False)

    st.subheader("Vind")
    st.caption("Vindlager är avstängt i cloud-only-läge.")
    show_wind_turbines = False

main_col, right_col = st.columns([4.8, 1.2], gap="medium")
with right_col:
    st.subheader("Punktbuffert")
    point_buffer_m = st.slider("Buffert runt tända punktlager (meter)", 0, 3000, 0, 100, key="point_buffer_right")
    st.subheader("Analys")
    analysis_enabled = st.checkbox("Aktivera analys", value=False)
    analysis_metric = st.selectbox("Mått", ["Punkter", "Unika respondenter"], index=0)
    analysis_near_m = st.slider("Närhetszon runt valt LST-lager (meter)", 0, 3000, 0, 50)

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
    active_point_labels.append("extra känsliga")
if show_non_sensitive_points:
    active_point_labels.append("inte extra känsliga")
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
    sty, kar = _cached_base_layers(str(repo_root))
    sty_field, kar_field = choose_default_field(sty), choose_default_field(kar)

theme_layers: dict[str, gpd.GeoDataFrame] = {}
for key, on in [
    ("rorligt_friluftsliv", show_rorligt_friluftsliv),
    ("utbyggnad_vindkraft", show_utbyggnad_vindkraft),
    ("nature_reserve", show_nature_reserve),
    ("kulturmiljovard", show_kulturmiljovard),
]:
    if on:
        theme_layers[key] = _cached_theme_layer(str(repo_root), key)

kommuner, kommungrupper, lan_boundary = None, None, None
if show_kommuner or show_kommungrupper or analysis_enabled or area_kind in {"kommun", "kommungrupp", "all_kommuner", "all_kommungrupper"}:
    kommuner, kommungrupper = _cached_admin_layers(str(repo_root))
if show_lan_boundary or analysis_enabled or area_kind == "lan":
    lan_boundary = _cached_lan_boundary()
    if lan_boundary is None or len(lan_boundary) == 0:
        raise RuntimeError("admin_boundaries.gpkg: lan layer is empty or invalid.")

point_filter_stats_rows: list[dict[str, object]] | None = None
point_filter_totals: dict[str, int | float] | None = None
plats1_points = plats2_points = sensitive_points = non_sensitive_points = None
if show_plats1_points or show_plats2_points or show_sensitive_points or show_non_sensitive_points or analysis_enabled:
    locked_cache_token = _locked_points_cache_token(str(repo_root))
    locked_layers = _cached_locked_point_layers(str(repo_root), locked_cache_token)
    plats1_points, plats2_points, sensitive_points, non_sensitive_points = locked_layers

    def _has_home_values(gdf: gpd.GeoDataFrame | None) -> bool:
        if gdf is None or len(gdf) == 0:
            return False
        for c in ["resp_kom", "home_kommunkod", "Q1", "q1", "hemvist_q1", "hemvist_kommunkod", "home_kommungrupp_id_current"]:
            if c in gdf.columns:
                vals = gdf[c].astype("string").str.strip().fillna("")
                if vals.ne("").any():
                    return True
        return False

    if filter_mode == "Hemvist (QI)" and area_kind in {"kommun", "kommungrupp"}:
        has_home = any(
            _has_home_values(g)
            for g in [plats1_points, plats2_points, sensitive_points, non_sensitive_points]
        )
        if not has_home:
            st.warning(
                "Hemvist (QI) saknar hemvistfält i punktlagret. "
                "Bygg om `novus_locked_points.gpkg` med `resp_kom` och helst "
                "`home_kommungrupp_id_current`."
            )

    plats1_points = _apply_area_filter(plats1_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    plats2_points = _apply_area_filter(plats2_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    sensitive_points = _apply_area_filter(sensitive_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    non_sensitive_points = _apply_area_filter(non_sensitive_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    before_counts = {
        "Vald plats 1": len(plats1_points) if plats1_points is not None else 0,
        "Vald plats 2": len(plats2_points) if plats2_points is not None else 0,
        "Extra känsliga": len(sensitive_points) if sensitive_points is not None else 0,
        "Inte extra känsliga": len(non_sensitive_points) if non_sensitive_points is not None else 0,
    }

    if filter_points_by_boreal:
        sampler = _cached_raster_sampler(str(repo_root), BOREAL_RASTER_OVERLAY_JSON)
        if sampler is None:
            st.sidebar.warning("Skoglig raster för filtrering saknas. Bygg overlay först.")
        else:
            vmin, vmax = int(boreal_value_range[0]), int(boreal_value_range[1])
            plats1_points = _filter_points_by_raster_range(plats1_points, sampler, vmin, vmax)
            plats2_points = _filter_points_by_raster_range(plats2_points, sampler, vmin, vmax)
            sensitive_points = _filter_points_by_raster_range(sensitive_points, sampler, vmin, vmax)
            non_sensitive_points = _filter_points_by_raster_range(non_sensitive_points, sampler, vmin, vmax)
            with right_col:
                st.caption(f"Rasterfilter aktivt: skoglig värdekärna {vmin}-{vmax}.")

    after_counts = {
        "Vald plats 1": len(plats1_points) if plats1_points is not None else 0,
        "Vald plats 2": len(plats2_points) if plats2_points is not None else 0,
        "Extra känsliga": len(sensitive_points) if sensitive_points is not None else 0,
        "Inte extra känsliga": len(non_sensitive_points) if non_sensitive_points is not None else 0,
    }
    vis_by_layer = {
        "Vald plats 1": bool(show_plats1_points),
        "Vald plats 2": bool(show_plats2_points),
        "Extra känsliga": bool(show_sensitive_points),
        "Inte extra känsliga": bool(show_non_sensitive_points),
    }
    point_filter_stats_rows = []
    for label in ["Vald plats 1", "Vald plats 2", "Extra känsliga", "Inte extra känsliga"]:
        b = int(before_counts.get(label, 0))
        a = int(after_counts.get(label, 0))
        keep_pct = 100.0 if b == 0 else (100.0 * a / b)
        point_filter_stats_rows.append(
            {
                "Punktlager": label,
                "Visas i karta": "Ja" if vis_by_layer.get(label, False) else "Nej",
                "Före rasterfilter": b,
                "Efter rasterfilter": a,
                "Behållna %": round(keep_pct, 1),
            }
        )

    total_before_visible = int(sum(before_counts.get(lbl, 0) for lbl, on in vis_by_layer.items() if on))
    total_after_visible = int(sum(after_counts.get(lbl, 0) for lbl, on in vis_by_layer.items() if on))
    total_keep_visible = 100.0 if total_before_visible == 0 else (100.0 * total_after_visible / total_before_visible)
    total_before_all = int(sum(before_counts.values()))
    total_after_all = int(sum(after_counts.values()))
    total_keep_all = 100.0 if total_before_all == 0 else (100.0 * total_after_all / total_before_all)
    point_filter_totals = {
        "before_visible": total_before_visible,
        "after_visible": total_after_visible,
        "keep_visible_pct": round(total_keep_visible, 1),
        "before_all": total_before_all,
        "after_all": total_after_all,
        "keep_all_pct": round(total_keep_all, 1),
    }
    point_filter_stats_rows.append(
        {
            "Punktlager": "SUMMA (visas i karta)",
            "Visas i karta": "Ja",
            "Före rasterfilter": total_before_visible,
            "Efter rasterfilter": total_after_visible,
            "Behållna %": round(total_keep_visible, 1),
        }
    )
    point_filter_stats_rows.append(
        {
            "Punktlager": "SUMMA (alla punktlager)",
            "Visas i karta": "-",
            "Före rasterfilter": total_before_all,
            "Efter rasterfilter": total_after_all,
            "Behållna %": round(total_keep_all, 1),
        }
    )

    if filter_mode == "Hemvist (QI)" and area_kind in {"kommun", "kommungrupp"}:
        active_counts = []
        if show_plats1_points:
            active_counts.append(len(plats1_points) if plats1_points is not None else 0)
        if show_plats2_points:
            active_counts.append(len(plats2_points) if plats2_points is not None else 0)
        if show_sensitive_points:
            active_counts.append(len(sensitive_points) if sensitive_points is not None else 0)
        if show_non_sensitive_points:
            active_counts.append(len(non_sensitive_points) if non_sensitive_points is not None else 0)
        if active_counts and sum(active_counts) == 0:
            st.warning(
                "Hemvist-filter gav 0 träffar för valt arbetsområde. "
                "Om du nyss byggt om `novus_locked_points.gpkg`, starta om appen eller vänta 5 minuter så cache uppdateras."
            )

if filter_points_by_boreal:
    st.sidebar.subheader("Rasterfilter: före/efter")
    vmin, vmax = int(boreal_value_range[0]), int(boreal_value_range[1])
    st.sidebar.caption(
        f"Arbetsområde: {_analysis_scope_label(area_kind, area_value)} | Värdeintervall: {vmin}-{vmax}"
    )
    st.sidebar.caption("Före/efter avser rasterfiltret, efter arbetsområdesfilter.")
    if point_filter_totals is not None:
        c1, c2 = st.sidebar.columns(2)
        c1.metric("Före (visas)", int(point_filter_totals["before_visible"]))
        c2.metric("Efter (visas)", int(point_filter_totals["after_visible"]))
        st.sidebar.caption(f"Andel kvar (visas): {float(point_filter_totals['keep_visible_pct']):.1f}%")
        c3, c4 = st.sidebar.columns(2)
        c3.metric("Före (alla)", int(point_filter_totals["before_all"]))
        c4.metric("Efter (alla)", int(point_filter_totals["after_all"]))
        st.sidebar.caption(f"Andel kvar (alla): {float(point_filter_totals['keep_all_pct']):.1f}%")
    if point_filter_stats_rows:
        st.sidebar.dataframe(
            pd.DataFrame(point_filter_stats_rows),
            use_container_width=True,
            hide_index=True,
            height=220,
        )
    else:
        st.sidebar.info("Inga punktlager är inlästa för valt arbetsområde.")

wind_turbines = None
if show_wind_turbines:
    try:
        wind_turbines, _ = _cached_wind_layers(str(repo_root), 30000)
    except Exception:
        st.sidebar.warning("Kunde inte läsa in vindkraftverk i deployment.")
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

extra_image_overlays: list[dict[str, object]] = []
if show_boreal_density:
    try:
        boreal_overlay = _cached_raster_overlay(str(repo_root), BOREAL_RASTER_OVERLAY_JSON)
        if boreal_overlay is None:
            st.sidebar.info(
                "Raster overlay saknas. Kor scripts/11_prepare_raster_overlay.py for att skapa en komprimerad overlay."
            )
        else:
            extra_image_overlays.append(boreal_overlay)
    except Exception as exc:
        st.sidebar.warning(f"Kunde inte lasa rasteroverlay: {exc}")

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
    satellite_base=satellite_base,
    extra_image_overlays=extra_image_overlays,
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
            st.warning("Analysen stöder max ett LST-lager åt gången. Släck till ett lager för maskad analys.")
            analysis_blocked_multi_lst = True
        elif len(lst_active_layers) == 1:
            st.caption("Analysläge: arbetsområde + 1 aktivt LST-lager.")
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
            st.caption("Analysläge: endast arbetsområde (inget aktivt LST-lager).")

    if analysis_blocked_multi_lst:
        q_suffix = " utan LST-mask (flera LST-lager är tända)"
    elif selected_lst_layer is None:
        q_suffix = ""
    else:
        q_suffix = " inom valt LST-lager"
    st.caption(f"Fråga: Hur många {q_points.lower()} finns i {q_area}{q_suffix}? Svaret visas i kartan.")

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
            st.caption("Analys pausad: välj högst ett LST-lager.")
        elif summary is not None and len(summary) > 0:
            st.caption(f"Analysen visar {analysis_metric.lower()} i {len(summary)} arbetsområde(n). Summa n: {int(summary['n'].sum())}.")
            if bubbles_drawn == 0:
                st.warning("Analysresultat finns men bubblor kunde inte ritas (geometriproblem).")
        else:
            st.caption("Ingen träff i analysen med nuvarande val.")

with main_col:
    try:
        folium_static(m, width=1200, height=920)
    except Exception:
        components.html(m.get_root().render(), height=920, scrolling=False)
