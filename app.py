from __future__ import annotations

from pathlib import Path
import html
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
from PIL import Image
from pyproj import Transformer
from streamlit_folium import st_folium
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

LAYER_LABELS = {
    "landskapstyp": "Landskapstyper",
    "landskapskaraktar": "Landskapskaraktärsområden",
    "rorligt_friluftsliv": "Rörligt friluftsliv",
    "utbyggnad_vindkraft": "Utbyggnad av vindkraft",
    "nature_reserve": "Naturvårdsområden",
    "kulturmiljovard": "Kulturmiljövård",
    "boreal_density": "Skoglig värdekärna",
}
THEME_LAYER_STYLES = {
    "rorligt_friluftsliv": "#0891b2",
    "utbyggnad_vindkraft": "#22c55e",
    "nature_reserve": "#d946ef",
    "kulturmiljovard": "#f59e0b",
}
THEME_LAYER_SHAPES = {}
ADMIN_LAYER_STYLES = {
    "lan_boundary": ("Länsgräns", "#b91c1c"),
    "kommuner": ("Kommungräns", "#4b5563"),
    "kommungrupper": ("Kommungrupper", "#1d4ed8"),
}
BOREAL_LEGEND_ITEMS = [
    ("1-30", "linear-gradient(90deg, rgba(27,94,32,0.08) 0%, rgba(27,94,32,0.60) 100%)", "Transparent mörkgrön"),
    ("31-60", "#8bc34a", "Ljusgrön"),
    ("61-70", "#fdd835", "Gul"),
    ("71-80", "#fb8c00", "Orange"),
    ("81-90", "#e53935", "Röd"),
    ("91-94", "#7f0000", "Mörkröd"),
]

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
    "2084": "Avesta, Hedemora, Säter",
    "2083": "Avesta, Hedemora, Säter",
    "2082": "Avesta, Hedemora, Säter",
    "2080": "Falun, Borlänge",
    "2081": "Falun, Borlänge",
    "2023": "Malung-Sälen, Älvdalen, Vansbro",
    "2039": "Malung-Sälen, Älvdalen, Vansbro",
    "2021": "Malung-Sälen, Älvdalen, Vansbro",
    "2062": "Mora, Orsa",
    "2034": "Mora, Orsa",
    "2031": "Rättvik, Leksand, Gagnef",
    "2029": "Rättvik, Leksand, Gagnef",
    "2026": "Rättvik, Leksand, Gagnef",
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

GROUP_NAME_BY_ID: dict[str, str] = {}
for _code, _group_id in CODE_TO_GROUP_ID.items():
    _group_name = CODE_TO_GROUP_NAME.get(_code, "")
    if _group_id and _group_name and _group_id not in GROUP_NAME_BY_ID:
        GROUP_NAME_BY_ID[_group_id] = _group_name


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
def _cached_base_layers(repo_root_str: str, cache_token: str):
    _ = cache_token
    repo = Path(repo_root_str)
    lst_bundle = repo / "data" / "cloud" / LST_BUNDLE_GPKG
    return (
        _read_vector_4326(lst_bundle, layer=LST_BUNDLE_LAYER_BY_KEY["landskapstyp"], default_crs=3006),
        _read_vector_4326(lst_bundle, layer=LST_BUNDLE_LAYER_BY_KEY["landskapskaraktar"], default_crs=3006),
    )


def _clip_to_dalarna(gdf: gpd.GeoDataFrame, dalarna: gpd.GeoDataFrame | None) -> gpd.GeoDataFrame:
    if gdf is None or len(gdf) == 0 or dalarna is None or len(dalarna) == 0:
        return gdf
    try:
        return gpd.clip(gdf, dalarna)
    except Exception:
        mask = dalarna.geometry.union_all() if hasattr(dalarna.geometry, "union_all") else dalarna.geometry.unary_union
        return gdf[gdf.geometry.intersects(mask)].copy()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_theme_layer(repo_root_str: str, key: str, cache_token: str):
    _ = cache_token
    repo = Path(repo_root_str)
    lst_bundle = repo / "data" / "cloud" / LST_BUNDLE_GPKG
    if key not in LST_BUNDLE_LAYER_BY_KEY:
        raise KeyError(f"Unknown layer key: {key}")
    out = _read_vector_4326(lst_bundle, layer=LST_BUNDLE_LAYER_BY_KEY[key], default_crs=3006)
    if key in {"rorligt_friluftsliv", "kulturmiljovard"}:
        out = _clip_to_dalarna(out, _cached_lan_boundary())
    return out


@st.cache_data(show_spinner=False, ttl=300)
def _cached_respondent_metadata(repo_root_str: str) -> pd.DataFrame:
    csv_path = Path(repo_root_str) / "data" / "cloud" / "novus_full_dataframe.csv"
    if not csv_path.exists():
        return pd.DataFrame(columns=["record", "respid", "respondent_alder", "respondent_q1"])

    df = pd.read_csv(
        csv_path,
        usecols=["Record", "respid", "Alder", "Q1"],
        dtype={"Record": "string", "respid": "string", "Alder": "Int64", "Q1": "string"},
    ).rename(
        columns={
            "Record": "record",
            "Alder": "respondent_alder",
            "Q1": "respondent_q1",
        }
    )
    df["record"] = _numkey(df["record"])
    df["respid"] = df["respid"].astype("string").str.strip()
    df["respondent_q1"] = df["respondent_q1"].astype("string").str.strip()
    df["respondent_alder"] = _numkey(df["respondent_alder"])
    return df.drop_duplicates(subset=["record", "respid"], keep="first")


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
    respondent_meta = _cached_respondent_metadata(repo_root_str)

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
        out["record"] = _numkey(out["record"]) if "record" in out.columns else pd.Series([pd.NA] * len(out), index=out.index, dtype="string")
        out["respid"] = out["respid"].astype("string").str.strip() if "respid" in out.columns else pd.Series([pd.NA] * len(out), index=out.index, dtype="string")

        if not respondent_meta.empty:
            out = out.merge(respondent_meta, on=["record", "respid"], how="left")

        resp_code = _numkey(
            _pick_string_series(out, ["resp_kom", "home_kommunkod", "Q1", "respondent_q1", "q1", "hemvist_q1", "hemvist_kommunkod"])
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
        out["home_kommunnamn"] = _numkey(out["home_kommunkod"]).map(code_to_name)
        out["respondent_hemvist"] = (
            out["home_kommunnamn"].fillna("").astype(str).str.strip()
            + out["home_kommunkod"].fillna("").astype(str).map(lambda value: f" ({value})" if value else "")
        ).str.strip()
        out["respondent_hemvist"] = out["respondent_hemvist"].replace("", pd.NA)

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
        if "respondent_alder" in out.columns:
            out["respondent_alder"] = _numkey(out["respondent_alder"])
        if "plats_nr" in out.columns:
            out["plats_nr"] = _numkey(out["plats_nr"])

        for c in ["plats_nr", "plats_fritext", "record", "respid", "lat", "lon", "respondent_alder", "home_kommunnamn", "respondent_hemvist"]:
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


def _parse_world_file(path: Path) -> tuple[float, float, float, float, float, float]:
    lines = [ln.strip() for ln in path.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
    if len(lines) < 6:
        raise RuntimeError(f"World file must contain 6 numeric rows: {path}")
    vals = [float(lines[i]) for i in range(6)]
    return vals[0], vals[1], vals[2], vals[3], vals[4], vals[5]


def _resolve_overlay_path(cfg_path: Path, repo_root: Path, raw_value: str) -> Path:
    raw = str(raw_value).strip()
    normalized = raw.replace("\\", "/")
    candidates: list[Path] = []
    for value in [normalized, raw]:
        if not value:
            continue
        p = Path(value)
        if p not in candidates:
            candidates.append(p)

    resolved: list[Path] = []
    for candidate in candidates:
        if candidate.is_absolute():
            resolved.append(candidate)
        else:
            resolved.append((repo_root / candidate).resolve())
            resolved.append((cfg_path.parent / candidate).resolve())

    seen: set[str] = set()
    for candidate in resolved:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate

    return resolved[0] if resolved else Path(raw)


def _current_world_params(
    image_width: int,
    image_height: int,
    source_window: tuple[float, float, float, float],
    world_params: tuple[float, float, float, float, float, float],
) -> tuple[float, float, float, float, float, float]:
    left, top, right, bottom = source_window
    sx = (right - left) / float(image_width)
    sy = (bottom - top) / float(image_height)
    ox = (left - 0.5) + (0.5 * sx)
    oy = (top - 0.5) + (0.5 * sy)
    a, d, b, e, c, f = world_params
    return (
        a * sx,
        d * sx,
        b * sy,
        e * sy,
        (a * ox) + (b * oy) + c,
        (d * ox) + (e * oy) + f,
    )


def _world_to_pixel_np(
    x,
    y,
    a: float,
    d: float,
    b: float,
    e: float,
    c: float,
    f: float,
):
    det = (a * e) - (b * d)
    if det == 0:
        raise RuntimeError("Invalid geotransform: determinant is zero.")
    dx = x - c
    dy = y - f
    col = ((e * dx) - (b * dy)) / det
    row = ((-d * dx) + (a * dy)) / det
    return col, row


@st.cache_data(show_spinner=False, ttl=300)
def _cached_raster_sampler(repo_root_str: str, overlay_json: str):
    repo_root = Path(repo_root_str)
    cfg_path = Path(repo_root_str) / "data" / "cloud" / overlay_json
    if not cfg_path.exists():
        return None
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

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

    tfw_raw = str(cfg.get("source_tfw", "")).strip()
    if not tfw_raw:
        return None
    tfw_path = _resolve_overlay_path(cfg_path, repo_root, tfw_raw)
    if not tfw_path.exists():
        return None

    source_window_raw = cfg.get("source_window_px")
    if not isinstance(source_window_raw, list) or len(source_window_raw) != 4:
        return None

    sample_crs = str(cfg.get("sample_crs", cfg.get("source_crs", "EPSG:3006"))).strip() or "EPSG:3006"
    world_params = _parse_world_file(tfw_path)
    sample_world_params = _current_world_params(
        image_width=values.shape[1],
        image_height=values.shape[0],
        source_window=(
            float(source_window_raw[0]),
            float(source_window_raw[1]),
            float(source_window_raw[2]),
            float(source_window_raw[3]),
        ),
        world_params=world_params,
    )
    rmin = int(cfg.get("raster_min", 1))
    rmax = int(cfg.get("raster_max", 94))
    return {
        "values": values,
        "alpha": alpha,
        "sample_crs": sample_crs,
        "sample_world_params": sample_world_params,
        "raster_min": rmin,
        "raster_max": rmax,
        "sample_path": str(sample_path),
        "source_tfw": str(tfw_path),
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
    sample_crs = str(sampler.get("sample_crs", "EPSG:3006"))
    sample_world_params = sampler["sample_world_params"]
    h, w = vals.shape

    xy = np.array([(geom.x, geom.y) if geom is not None else (np.nan, np.nan) for geom in pts.geometry], dtype=np.float64)
    lon = xy[:, 0]
    lat = xy[:, 1]
    valid_xy = np.isfinite(lon) & np.isfinite(lat)
    if not valid_xy.any():
        out = pts.copy()
        out[out_col] = np.nan
        return out

    if sample_crs == "EPSG:4326":
        x_s = lon
        y_s = lat
    else:
        transformer = Transformer.from_crs(4326, sample_crs, always_xy=True)
        x_s, y_s = transformer.transform(lon, lat)

    out_vals = np.full(len(pts), np.nan, dtype=np.float64)
    cols_f, rows_f = _world_to_pixel_np(x_s, y_s, *sample_world_params)
    inside = (
        valid_xy
        & np.isfinite(cols_f)
        & np.isfinite(rows_f)
        & (cols_f >= -0.5)
        & (cols_f <= (w - 0.5))
        & (rows_f >= -0.5)
        & (rows_f <= (h - 0.5))
    )
    if inside.any():
        cols = np.clip(np.rint(cols_f[inside]).astype(np.int64), 0, w - 1)
        rows = np.clip(np.rint(rows_f[inside]).astype(np.int64), 0, h - 1)
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


def _geometry_union(geoms: gpd.GeoSeries):
    if hasattr(geoms, "union_all"):
        return geoms.union_all()
    return geoms.unary_union


def _internal_kommun_boundary_layer(kommuner: gpd.GeoDataFrame | None) -> gpd.GeoDataFrame | None:
    if kommuner is None or len(kommuner) == 0:
        return kommuner

    try:
        source = kommuner.copy()
        if source.crs is None:
            source = source.set_crs(4326)
        source = source.to_crs(3006)

        polygonal = source[source.geometry.geom_type.astype(str).isin(["Polygon", "MultiPolygon"])].copy()
        if len(polygonal) == 0:
            return kommuner

        try:
            fixed_geometry = polygonal.geometry.make_valid()
        except Exception:
            fixed_geometry = polygonal.geometry.buffer(0)

        fixed = gpd.GeoSeries(fixed_geometry, crs=source.crs)
        fixed = fixed[fixed.notna() & (~fixed.is_empty)]
        if len(fixed) == 0:
            return kommuner

        all_boundaries = _geometry_union(fixed.boundary)
        dissolved = _geometry_union(fixed)
        if all_boundaries is None or dissolved is None:
            return kommuner

        outer_boundary = dissolved.boundary
        internal = all_boundaries.difference(outer_boundary)
        if internal is None or getattr(internal, "is_empty", True):
            internal = all_boundaries.difference(outer_boundary.buffer(2.0))
        if internal is None or getattr(internal, "is_empty", True):
            return kommuner

        return gpd.GeoDataFrame(
            {
                "kommunnamn": [ADMIN_LAYER_STYLES["kommuner"][0]],
                "kommunkod": [""],
            },
            geometry=[internal],
            crs=source.crs,
        ).to_crs(4326)
    except Exception:
        return kommuner


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

    def _prepared_mask(layer: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        g = layer.to_crs(3006).copy()
        g = g[g.geometry.notna() & (~g.geometry.is_empty)].copy()
        if len(g) == 0:
            return g
        # Repair invalid polygons before union to avoid TopologyException.
        try:
            g["geometry"] = g.geometry.make_valid()
        except Exception:
            pass
        g["geometry"] = g.geometry.buffer(0)
        return g[g.geometry.notna() & (~g.geometry.is_empty)].copy()

    mask_gdf = _prepared_mask(mask_layer)
    if len(mask_gdf) == 0:
        return pts.to_crs(4326), None

    if near_m > 0:
        mask_gdf = mask_gdf.copy()
        mask_gdf["geometry"] = mask_gdf.geometry.buffer(float(near_m))

    joined = gpd.sjoin(
        pts,
        mask_gdf[[mask_gdf.geometry.name]].rename(columns={mask_gdf.geometry.name: "geometry"}),
        how="inner",
        predicate="intersects",
    )
    filtered_pts = pts.loc[joined.index.unique()].copy().to_crs(4326)

    mask_zone = None
    if near_m > 0:
        try:
            zone_geom = mask_gdf.geometry.union_all()
        except Exception:
            zone_geom = mask_gdf.geometry.unary_union
        if zone_geom is not None and not getattr(zone_geom, "is_empty", True):
            try:
                zone_geom = zone_geom.simplify(max(25.0, float(near_m) * 0.08), preserve_topology=True)
            except Exception:
                pass
            mask_zone = gpd.GeoDataFrame({"geometry": [zone_geom]}, geometry="geometry", crs=3006).to_crs(4326)

    return filtered_pts, mask_zone


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


def _attach_leaflet_render_styles(m: folium.Map) -> None:
    m.get_root().header.add_child(
        folium.Element(
            """
            <style>
            .leaflet-container {
              overflow: hidden;
            }
            .leaflet-image-layer {
              image-rendering: crisp-edges;
              image-rendering: pixelated;
            }
            .leaflet-control-attribution {
              margin: 0 8px 8px 0;
            }
            </style>
            """
        )
    )


MAP_COMPONENT_KEY = "energidalarna_main_map"
RIGHT_PANEL_OPEN_KEY = "right_panel_open"


def _stable_streamlit_map_shell(
    source_map: folium.Map,
    satellite_base: bool,
) -> folium.Map:
    base = folium.Map(
        location=list(getattr(source_map, "location", [61.0, 14.5])),
        zoom_start=int(getattr(source_map, "options", {}).get("zoom", 8)),
        tiles=None,
    )
    folium.TileLayer(
        tiles="CartoDB positron",
        name="Bakgrund: Ljus karta",
        overlay=False,
        control=True,
        show=not satellite_base,
    ).add_to(base)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles (c) Esri, Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community",
        name="Bakgrund: Satellit",
        overlay=False,
        control=True,
        show=satellite_base,
        max_zoom=19,
    ).add_to(base)
    _attach_leaflet_render_styles(base)
    return base


def _dynamic_feature_groups(source_map: folium.Map) -> list[folium.FeatureGroup]:
    groups: list[folium.FeatureGroup] = []
    skip_types = (
        folium.raster_layers.TileLayer,
        folium.map.LayerControl,
    )
    for idx, child in enumerate(list(source_map._children.values())):
        if isinstance(child, skip_types):
            continue
        if isinstance(child, folium.FeatureGroup):
            groups.append(child)
            continue
        layer_name = getattr(child, "layer_name", None) or getattr(child, "_name", None)
        group = folium.FeatureGroup(
            name=str(layer_name or f"Kartlager {idx + 1}"),
            overlay=True,
            control=bool(getattr(child, "control", True)),
            show=bool(getattr(child, "show", True)),
        )
        group.add_child(child)
        groups.append(group)
    return groups


def _normalize_landscape_label(value: object) -> str:
    normalize = getattr(map_factory, "_normalize_landscape_type", None)
    if callable(normalize):
        return normalize(str(value))
    return str(value)


def _palette_map_safe(values: list[str]) -> dict[str, str]:
    palette_map = getattr(map_factory, "_palette_map", None)
    if callable(palette_map):
        return palette_map(values)
    fallback = ["#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e", "#17becf"]
    uniq = sorted({str(v) for v in values})
    return {value: fallback[i % len(fallback)] for i, value in enumerate(uniq)}


def _legend_swatch_html(color: str, shape: str = "box") -> str:
    if shape == "line":
        return (
            "<span style=\"display:inline-block;width:18px;height:0;"
            f"border-top:3px solid {color};margin-right:8px;vertical-align:middle;\"></span>"
        )
    if shape == "circle":
        return (
            "<span style=\"display:inline-block;width:14px;height:14px;border-radius:999px;"
            f"background:{color};border:1px solid rgba(0,0,0,0.18);margin-right:8px;"
            "vertical-align:middle;\"></span>"
        )
    return (
        "<span style=\"display:inline-block;width:14px;height:14px;border-radius:4px;"
        f"background:{color};border:1px solid rgba(0,0,0,0.18);margin-right:8px;"
        "vertical-align:middle;\"></span>"
    )


def _render_legend_card(title: str, items: list[dict[str, str]], caption: str | None = None, footer: str | None = None) -> None:
    rows = []
    for item in items:
        label = html.escape(str(item.get("label", "")))
        note = str(item.get("note", "")).strip()
        note_html = f" <span style=\"color:#6b7280;\">{html.escape(note)}</span>" if note else ""
        rows.append(
            (
                "<div style=\"display:flex;align-items:flex-start;gap:0;margin:0 0 6px 0;\">"
                f"{_legend_swatch_html(str(item.get('color', '#9ca3af')), str(item.get('shape', 'box')))}"
                f"<span style=\"line-height:1.25;\">{label}{note_html}</span>"
                "</div>"
            )
        )
    body_style = "max-height:220px;overflow-y:auto;padding-right:4px;" if len(items) > 9 else ""
    caption_html = (
        f"<div style=\"font-size:0.82rem;color:#4b5563;margin:0 0 8px 0;\">{html.escape(caption)}</div>"
        if caption
        else ""
    )
    footer_html = (
        f"<div style=\"font-size:0.82rem;color:#4b5563;margin:8px 0 0 0;\">{html.escape(footer)}</div>"
        if footer
        else ""
    )
    st.markdown(
        (
            "<div style=\"background:#f8fafc;border:1px solid rgba(148,163,184,0.35);"
            "border-radius:12px;padding:12px 14px;margin:0 0 12px 0;\">"
            f"<div style=\"font-weight:700;color:#0f172a;margin:0 0 6px 0;\">{html.escape(title)}</div>"
            f"{caption_html}"
            f"<div style=\"font-size:0.85rem;line-height:1.2;{body_style}\">"
            + "".join(rows)
            + "</div>"
            f"{footer_html}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_active_legends(
    *,
    show_lan_boundary: bool,
    show_kommuner: bool,
    show_kommungrupper: bool,
    show_sty: bool,
    sty: gpd.GeoDataFrame | None,
    show_kar: bool,
    kar: gpd.GeoDataFrame | None,
    show_rorligt_friluftsliv: bool,
    show_utbyggnad_vindkraft: bool,
    show_nature_reserve: bool,
    show_kulturmiljovard: bool,
    show_boreal_density: bool,
    filter_points_by_boreal: bool,
    boreal_value_range: tuple[int, int],
    show_plats1_points: bool,
    show_plats2_points: bool,
    show_sensitive_points: bool,
    show_non_sensitive_points: bool,
    point_buffer_m: int,
    analysis_enabled: bool,
    analysis_metric: str,
    selected_lst_layer: gpd.GeoDataFrame | None,
    analysis_blocked_multi_lst: bool,
) -> None:
    cards: list[dict[str, object]] = []

    admin_items: list[dict[str, str]] = []
    if show_lan_boundary:
        label, color = ADMIN_LAYER_STYLES["lan_boundary"]
        admin_items.append({"label": label, "color": color, "shape": "line"})
    if show_kommuner:
        label, color = ADMIN_LAYER_STYLES["kommuner"]
        admin_items.append({"label": label, "color": color, "shape": "line"})
    if show_kommungrupper:
        label, color = ADMIN_LAYER_STYLES["kommungrupper"]
        admin_items.append({"label": label, "color": color, "shape": "line"})
    if admin_items:
        cards.append(
            {
                "title": "Gränser",
                "items": admin_items,
                "caption": "Visar administrativa gränser som stöd i kartläsningen.",
            }
        )

    if show_sty and sty is not None and len(sty) > 0:
        field = choose_default_field(sty)
        values = sty[field].fillna("(saknas)").astype(str).map(_normalize_landscape_label).tolist()
        colors = _palette_map_safe(values)
        items = [{"label": label, "color": color} for label, color in colors.items()]
        cards.append(
            {
                "title": LAYER_LABELS["landskapstyp"],
                "items": items,
                "caption": "Färg visar landskapstyp i ytorna.",
            }
        )

    if show_kar and kar is not None and len(kar) > 0:
        field = choose_default_field(kar)
        values = kar[field].fillna("(saknas)").astype(str).tolist()
        colors = _palette_map_safe(values)
        items = [{"label": label, "color": color} for label, color in colors.items()]
        cards.append(
            {
                "title": LAYER_LABELS["landskapskaraktar"],
                "items": items,
                "caption": "Områdena visas transparent ovanpå baskartan.",
            }
        )

    thematic_items: list[dict[str, str]] = []
    for key, is_active in [
        ("rorligt_friluftsliv", show_rorligt_friluftsliv),
        ("utbyggnad_vindkraft", show_utbyggnad_vindkraft),
        ("nature_reserve", show_nature_reserve),
        ("kulturmiljovard", show_kulturmiljovard),
    ]:
        if is_active:
            thematic_items.append(
                {
                    "label": LAYER_LABELS[key],
                    "color": THEME_LAYER_STYLES[key],
                    "shape": THEME_LAYER_SHAPES.get(key, "box"),
                }
            )
    if thematic_items:
        cards.append(
            {
                "title": "Tematiska lager",
                "items": thematic_items,
                "caption": "Aktiva lager från externa datakällor.",
            }
        )

    point_layers_active = any([show_plats1_points, show_plats2_points, show_sensitive_points, show_non_sensitive_points])
    if point_layers_active:
        point_items = []
        active_point_layer_labels: list[str] = []
        if show_plats1_points:
            active_point_layer_labels.append("Vald plats 1")
        if show_plats2_points:
            active_point_layer_labels.append("Vald plats 2")
        if show_sensitive_points:
            active_point_layer_labels.append("Extra känsliga platser")
        if show_non_sensitive_points:
            active_point_layer_labels.append("Inte extra känsliga platser")
        for group_id in sorted(GROUP_NAME_BY_ID.keys(), key=lambda value: int(value)):
            point_items.append(
                {
                    "label": GROUP_NAME_BY_ID[group_id],
                    "color": getattr(map_factory, "GROUP_PALETTE", {}).get(int(group_id), "#9ca3af"),
                    "shape": "circle",
                }
            )
        if point_buffer_m > 0:
            point_items.append(
                {
                    "label": f"Punktbuffert ({point_buffer_m} m)",
                    "color": "#ef4444",
                }
            )
        cards.append(
            {
                "title": "Betydelsefulla platser",
                "items": point_items,
                "caption": "Färg visar kommungrupp för aktiva punktlager.",
                "footer": f"Aktiva lager: {', '.join(active_point_layer_labels)}.",
            }
        )

    if show_boreal_density or filter_points_by_boreal:
        items = [
            {"label": value_range, "note": label, "color": color}
            for value_range, color, label in BOREAL_LEGEND_ITEMS
        ]
        footer = "Lagret visas transparent ovanpå kartan." if show_boreal_density else "Filtret använder samma färgskala som lagret."
        if filter_points_by_boreal:
            footer = f"{footer} Aktivt filter: {int(boreal_value_range[0])}-{int(boreal_value_range[1])}."
        cards.append(
            {
                "title": LAYER_LABELS["boreal_density"],
                "items": items,
                "caption": "Färgskalan visar täthetsvärde för skoglig värdekärna. Låga värden visas mer transparent.",
                "footer": footer,
            }
        )

    if analysis_enabled:
        analysis_items = [
            {
                "label": f"Analysbubblor: antal {analysis_metric.lower()}",
                "color": "#3b82f6",
                "shape": "circle",
            }
        ]
        if selected_lst_layer is not None and not analysis_blocked_multi_lst:
            analysis_items.append({"label": "Närhetszon runt valt kartlager", "color": "#0ea5e9"})
        cards.append(
            {
                "title": "Punktanalys",
                "items": analysis_items,
                "caption": "Visas när punktanalysen är aktiverad.",
            }
        )

    if not cards:
        return

    st.markdown("**Legender**")
    cols = st.columns(2)
    for idx, card in enumerate(cards):
        with cols[idx % 2]:
            _render_legend_card(
                str(card.get("title", "")),
                list(card.get("items", [])),
                str(card["caption"]) if card.get("caption") else None,
                str(card["footer"]) if card.get("footer") else None,
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

    st.subheader("Kartlager")
    show_sty = st.checkbox(LAYER_LABELS["landskapstyp"], value=False)
    show_kar = st.checkbox(LAYER_LABELS["landskapskaraktar"], value=False)
    show_rorligt_friluftsliv = st.checkbox(LAYER_LABELS["rorligt_friluftsliv"], value=False)
    show_utbyggnad_vindkraft = st.checkbox(LAYER_LABELS["utbyggnad_vindkraft"], value=False)
    show_nature_reserve = st.checkbox(LAYER_LABELS["nature_reserve"], value=False)
    show_kulturmiljovard = st.checkbox(LAYER_LABELS["kulturmiljovard"], value=False)
    show_boreal_density = st.checkbox(LAYER_LABELS["boreal_density"], value=False)
    kartlager_opacity_pct = st.slider("Opacitet kartlager (%)", 5, 80, 35, 5)
    kartlager_opacity = kartlager_opacity_pct / 100.0

    st.subheader("Betydelsefulla platser")
    st.caption("Färg visar kommungrupp.")
    show_plats1_points = st.checkbox("Vald plats 1", value=True)
    show_plats2_points = st.checkbox("Vald plats 2", value=False)
    show_sensitive_points = st.checkbox("Valda platser som är extra känsliga för ny infrastruktur", value=False)
    show_non_sensitive_points = st.checkbox("Valda platser som INTE är känsliga för ny infrastruktur", value=False)

    st.subheader("Vind")
    st.caption("Vindlager är avstängt i cloud-only-läge.")
    show_wind_turbines = False

boreal_min_val, boreal_max_val = 1, 94
try:
    boreal_meta = _cached_raster_overlay(str(repo_root), BOREAL_RASTER_OVERLAY_JSON)
    if boreal_meta is not None:
        boreal_min_val = int(boreal_meta.get("raster_min", 1))
        boreal_max_val = int(boreal_meta.get("raster_max", 94))
except Exception:
    pass

def _session_int(key: str, default: int, min_value: int, max_value: int) -> int:
    try:
        value = int(st.session_state.get(key, default))
    except Exception:
        value = default
    return max(min_value, min(max_value, value))


def _session_range(key: str, default: tuple[int, int], min_value: int, max_value: int) -> tuple[int, int]:
    raw = st.session_state.get(key, default)
    try:
        lo, hi = int(raw[0]), int(raw[1])
    except Exception:
        lo, hi = default
    lo = max(min_value, min(max_value, lo))
    hi = max(min_value, min(max_value, hi))
    if lo > hi:
        lo, hi = hi, lo
    return lo, hi


if RIGHT_PANEL_OPEN_KEY not in st.session_state:
    st.session_state[RIGHT_PANEL_OPEN_KEY] = True

show_right_panel = bool(st.session_state.get(RIGHT_PANEL_OPEN_KEY, True))

if show_right_panel:
    main_col, right_toggle_col, right_col = st.columns([4.75, 0.08, 1.2], gap="small")
else:
    main_col, right_toggle_col = st.columns([5.95, 0.08], gap="small")
    right_col = None

right_panel_width_css = "min(22rem, 34vw)"
right_toggle_right_css = f"calc({right_panel_width_css} + 0.45rem)" if show_right_panel else "0.65rem"
right_panel_drawer_css = ""
if show_right_panel:
    right_panel_drawer_css = f"""
        div[data-testid="column"]:has(#right-panel-content-anchor) {{
          position: fixed !important;
          top: 0;
          right: 0;
          bottom: 0;
          width: {right_panel_width_css} !important;
          min-width: {right_panel_width_css} !important;
          max-width: {right_panel_width_css} !important;
          flex: 0 0 {right_panel_width_css} !important;
          z-index: 999;
          overflow-y: auto;
          overflow-x: hidden;
          background: rgb(240, 242, 246);
          border-left: 1px solid rgba(49, 51, 63, 0.16);
          padding: 3.35rem 1.15rem 2rem 1.15rem !important;
        }}
        div[data-testid="column"]:has(#right-panel-content-anchor) #right-panel-content-anchor {{
          display: none;
        }}
        div[data-testid="stAppViewContainer"] section.main .block-container,
        div[data-testid="stAppViewContainer"] .main .block-container {{
          padding-right: calc({right_panel_width_css} + 1.25rem);
        }}
        """

with right_toggle_col:
    right_toggle_css = """
        <span id="right-panel-toggle-anchor"></span>
        <style>
        div[data-testid="column"]:has(#right-panel-toggle-anchor) {
          min-width: 1.75rem !important;
          width: 1.75rem !important;
          max-width: 1.75rem !important;
          flex: 0 0 1.75rem !important;
          padding-left: 0 !important;
          padding-right: 0 !important;
        }
        div[data-testid="column"]:has(#right-panel-toggle-anchor) div[data-testid="stButton"] {
          position: fixed;
          top: 0.65rem;
          right: __RIGHT_TOGGLE_RIGHT__;
          z-index: 1001;
          width: 1.75rem;
          margin: 0 !important;
          padding: 0 !important;
        }
        div[data-testid="column"]:has(#right-panel-toggle-anchor) div[data-testid="stButton"] button {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 1.75rem;
          min-width: 1.75rem;
          height: 1.75rem;
          min-height: 1.75rem;
          padding: 0;
          border: 0;
          border-radius: 0.25rem;
          background: transparent;
          box-shadow: none;
          color: rgba(49, 51, 63, 0.55);
          font-size: 1rem;
          font-weight: 600;
          line-height: 1;
        }
        div[data-testid="column"]:has(#right-panel-toggle-anchor) div[data-testid="stButton"] button:hover {
          background: rgba(49, 51, 63, 0.08);
          color: rgba(49, 51, 63, 0.8);
        }
        div[data-testid="column"]:has(#right-panel-toggle-anchor) div[data-testid="stButton"] button:focus {
          outline: none;
          box-shadow: none;
        }
        div[data-testid="column"]:has(#right-panel-toggle-anchor) div[data-testid="stButton"] p {
          margin: 0;
          line-height: 1;
        }
        __RIGHT_PANEL_DRAWER_CSS__
        </style>
        """
    st.markdown(
        right_toggle_css.replace("__RIGHT_TOGGLE_RIGHT__", right_toggle_right_css).replace(
            "__RIGHT_PANEL_DRAWER_CSS__", right_panel_drawer_css
        ),
        unsafe_allow_html=True,
    )
    toggle_label = "»" if show_right_panel else "«"
    toggle_help = "Fäll in högerpanelen" if show_right_panel else "Visa högerpanelen"
    if st.button(toggle_label, key="right_panel_edge_toggle", help=toggle_help):
        st.session_state[RIGHT_PANEL_OPEN_KEY] = not show_right_panel
        st.rerun()

analysis_metric_options = ["Punkter", "Unika respondenter"]
if right_col is not None:
    point_buffer_default = _session_int("right_panel_point_buffer_m", 0, 0, 3000)
    analysis_enabled_default = bool(st.session_state.get("right_panel_analysis_enabled", False))
    analysis_metric_default = str(st.session_state.get("right_panel_analysis_metric", "Punkter"))
    if analysis_metric_default not in analysis_metric_options:
        analysis_metric_default = "Punkter"
    analysis_near_default = _session_int("right_panel_analysis_near_m", 0, 0, 3000)
    filter_boreal_default = bool(st.session_state.get("right_panel_filter_boreal", False))
    boreal_range_default = _session_range(
        "right_panel_boreal_range",
        (boreal_min_val, boreal_max_val),
        boreal_min_val,
        boreal_max_val,
    )
    with right_col:
        st.markdown('<span id="right-panel-content-anchor"></span>', unsafe_allow_html=True)
        st.subheader("Punktbuffert")
        point_buffer_m = st.slider("Buffert runt tända punktlager (meter)", 0, 3000, point_buffer_default, 100, key="point_buffer_right")
        st.subheader("Punktanalys")
        analysis_enabled = st.checkbox("Visa antal punkter i valt kartlager", value=analysis_enabled_default, key="analysis_enabled_right")
        analysis_metric = "Punkter"
        analysis_near_m = 0
        if analysis_enabled:
            analysis_metric = st.selectbox(
                "Mått",
                analysis_metric_options,
                index=analysis_metric_options.index(analysis_metric_default),
                key="analysis_metric_right",
            )
            analysis_near_m = st.slider("Närhetszon runt valt kartlager (meter)", 0, 3000, analysis_near_default, 50, key="analysis_near_m_right")
        else:
            st.caption("Aktivera analysen för att välja mått och närhetszon.")

        st.subheader(LAYER_LABELS["boreal_density"])
        st.markdown(
            "[Vill du veta mer om skogliga värdekärnor? Klicka här](https://geodata.naturvardsverket.se/nedladdning/Skog/Slutrapport_Landskapsanalys_av_skogliga_vardekarnor_i_boreal_region.pdf)"
        )
        filter_points_by_boreal = st.checkbox(
            "Filtrera alla punktlager med skoglig värdekärna",
            value=filter_boreal_default,
            key="filter_points_by_boreal_right",
        )
        if show_boreal_density:
            st.caption("Lagret visas på kartan. Legend visas under kartan.")
        if filter_points_by_boreal:
            boreal_value_range = st.slider(
                f"Täthetsvärde ({boreal_min_val}-{boreal_max_val})",
                boreal_min_val,
                boreal_max_val,
                boreal_range_default,
                1,
                key="boreal_value_range_right",
            )
        else:
            boreal_value_range = (boreal_min_val, boreal_max_val)
    st.session_state["right_panel_point_buffer_m"] = int(point_buffer_m)
    st.session_state["right_panel_analysis_enabled"] = bool(analysis_enabled)
    st.session_state["right_panel_analysis_metric"] = str(analysis_metric)
    st.session_state["right_panel_analysis_near_m"] = int(analysis_near_m)
    st.session_state["right_panel_filter_boreal"] = bool(filter_points_by_boreal)
    st.session_state["right_panel_boreal_range"] = tuple(int(v) for v in boreal_value_range)
else:
    point_buffer_m = _session_int("right_panel_point_buffer_m", 0, 0, 3000)
    analysis_enabled = bool(st.session_state.get("right_panel_analysis_enabled", False))
    analysis_metric_raw = str(st.session_state.get("right_panel_analysis_metric", "Punkter"))
    analysis_metric = analysis_metric_raw if analysis_metric_raw in analysis_metric_options else "Punkter"
    analysis_near_m = _session_int("right_panel_analysis_near_m", 0, 0, 3000) if analysis_enabled else 0
    filter_points_by_boreal = bool(st.session_state.get("right_panel_filter_boreal", False))
    boreal_value_range = (
        _session_range(
            "right_panel_boreal_range",
            (boreal_min_val, boreal_max_val),
            boreal_min_val,
            boreal_max_val,
        )
        if filter_points_by_boreal
        else (boreal_min_val, boreal_max_val)
    )

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
    if mismatch and right_col is not None:
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

lst_bundle_cache_token = _file_cache_token(repo_root / "data" / "cloud" / LST_BUNDLE_GPKG)

sty, kar = _empty_gdf(), _empty_gdf()
sty_field, kar_field = "geometry", "geometry"
if show_sty or show_kar:
    sty, kar = _cached_base_layers(str(repo_root), lst_bundle_cache_token)
    sty_field, kar_field = choose_default_field(sty), choose_default_field(kar)

theme_layers: dict[str, gpd.GeoDataFrame] = {}
for key, on in [
    ("rorligt_friluftsliv", show_rorligt_friluftsliv),
    ("utbyggnad_vindkraft", show_utbyggnad_vindkraft),
    ("nature_reserve", show_nature_reserve),
    ("kulturmiljovard", show_kulturmiljovard),
]:
    if on:
        theme_layers[key] = _cached_theme_layer(str(repo_root), key, lst_bundle_cache_token)

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
            if right_col is not None:
                with right_col:
                    st.warning("Rasterunderlag för skoglig värdekärna saknas. Bygg overlay först.")
            else:
                st.warning("Rasterunderlag för skoglig värdekärna saknas. Bygg overlay först.")
        else:
            vmin, vmax = int(boreal_value_range[0]), int(boreal_value_range[1])
            plats1_points = _filter_points_by_raster_range(plats1_points, sampler, vmin, vmax)
            plats2_points = _filter_points_by_raster_range(plats2_points, sampler, vmin, vmax)
            sensitive_points = _filter_points_by_raster_range(sensitive_points, sampler, vmin, vmax)
            non_sensitive_points = _filter_points_by_raster_range(non_sensitive_points, sampler, vmin, vmax)
            if right_col is not None:
                with right_col:
                    st.caption(f"Filter aktivt: skoglig värdekärna {vmin}-{vmax}.")
            else:
                st.caption(f"Filter aktivt: skoglig värdekärna {vmin}-{vmax}.")

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

if filter_points_by_boreal and right_col is not None:
    with right_col:
        st.subheader("Filterresultat")
        vmin, vmax = int(boreal_value_range[0]), int(boreal_value_range[1])
        st.caption(
            f"Arbetsområde: {_analysis_scope_label(area_kind, area_value)} | Värdeintervall: {vmin}-{vmax}"
        )
        st.caption("Före/efter avser rasterfiltret, efter arbetsområdesfilter.")
        if point_filter_totals is not None:
            c1, c2 = st.columns(2)
            c1.metric("Före", int(point_filter_totals["before_visible"]))
            c2.metric("Efter", int(point_filter_totals["after_visible"]))
            st.caption(f"Andel kvar i kartan: {float(point_filter_totals['keep_visible_pct']):.1f}%")
            if point_filter_stats_rows:
                with st.expander("Visa före/efter per punktlager"):
                    st.dataframe(
                        pd.DataFrame(point_filter_stats_rows),
                        use_container_width=True,
                        hide_index=True,
                        height=220,
                    )
        else:
            st.info("Inga punktlager är inlästa för valt arbetsområde.")

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
            boreal_overlay = dict(boreal_overlay)
            boreal_overlay["name"] = LAYER_LABELS["boreal_density"]
            boreal_overlay["opacity"] = kartlager_opacity
            extra_image_overlays.append(boreal_overlay)
    except Exception as exc:
        st.sidebar.warning(f"Kunde inte lasa rasteroverlay: {exc}")

kommuner_for_map = _internal_kommun_boundary_layer(kommuner) if show_kommuner else kommuner

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
    kommuner=kommuner_for_map,
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
    layer_opacity=kartlager_opacity,
    point_radius=3.5,
    show_landscape_colored_points=False,
    show_landscape_aggregated_points=False,
    wind_turbines=wind_turbines,
    show_wind_turbines=show_wind_turbines,
    satellite_base=satellite_base,
    extra_image_overlays=extra_image_overlays,
    show_map_legend=False,
    initial_center=None,
    initial_zoom=None,
)

selected_lst_layer = None
analysis_blocked_multi_lst = False
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

    if len(lst_active_layers) > 1:
        analysis_blocked_multi_lst = True
        if right_col is not None:
            with right_col:
                st.warning("Punktanalysen stöder ett aktivt kartlager åt gången. Släck till ett lager för maskad analys.")
    elif len(lst_active_layers) == 1:
        selected_key, selected_lst_layer, selected_field = lst_active_layers[0]
        if right_col is not None:
            with right_col:
                st.caption("Punktanalys: arbetsområde + ett aktivt kartlager.")
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
        elif selected_field is not None and selected_field in selected_lst_layer.columns:
            vals = (
                selected_lst_layer[selected_field]
                .dropna()
                .astype(str)
                .str.strip()
            )
            uniq = sorted([v for v in vals.unique().tolist() if v != ""])
            selected_cat = str(st.session_state.get(f"lst_cat_{selected_key}", "Alla kategorier"))
            if selected_cat in uniq:
                selected_lst_layer = selected_lst_layer[
                    selected_lst_layer[selected_field].astype(str).str.strip() == selected_cat
                ].copy()
    elif right_col is not None:
        with right_col:
            st.caption("Punktanalys: endast arbetsområde.")

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
    if right_col is not None:
        with right_col:
            if analysis_blocked_multi_lst:
                st.caption("Punktanalysen är pausad: välj högst ett kartlager.")
            elif summary is not None and len(summary) > 0:
                st.caption(f"Punktanalysen visar {analysis_metric.lower()} i {len(summary)} arbetsområde(n). Summa n: {int(summary['n'].sum())}.")
                if bubbles_drawn == 0:
                    st.warning("Analysresultat finns men bubblor kunde inte ritas (geometriproblem).")
            else:
                st.caption("Ingen träff i punktanalysen med nuvarande val.")

_attach_leaflet_render_styles(m)
map_shell = _stable_streamlit_map_shell(m, satellite_base)
dynamic_feature_groups = _dynamic_feature_groups(m)

with main_col:
    st_folium(
        map_shell,
        key=MAP_COMPONENT_KEY,
        height=920,
        width=None,
        returned_objects=[],
        feature_group_to_add=dynamic_feature_groups,
        layer_control=folium.LayerControl(collapsed=False),
        use_container_width=True,
        pixelated=True,
    )
    _render_active_legends(
        show_lan_boundary=show_lan_boundary,
        show_kommuner=show_kommuner,
        show_kommungrupper=show_kommungrupper,
        show_sty=show_sty,
        sty=sty if show_sty else None,
        show_kar=show_kar,
        kar=kar if show_kar else None,
        show_rorligt_friluftsliv=show_rorligt_friluftsliv,
        show_utbyggnad_vindkraft=show_utbyggnad_vindkraft,
        show_nature_reserve=show_nature_reserve,
        show_kulturmiljovard=show_kulturmiljovard,
        show_boreal_density=show_boreal_density,
        filter_points_by_boreal=filter_points_by_boreal,
        boreal_value_range=(int(boreal_value_range[0]), int(boreal_value_range[1])),
        show_plats1_points=show_plats1_points,
        show_plats2_points=show_plats2_points,
        show_sensitive_points=show_sensitive_points,
        show_non_sensitive_points=show_non_sensitive_points,
        point_buffer_m=point_buffer_m,
        analysis_enabled=analysis_enabled,
        analysis_metric=analysis_metric,
        selected_lst_layer=selected_lst_layer,
        analysis_blocked_multi_lst=analysis_blocked_multi_lst,
    )
