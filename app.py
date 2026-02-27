from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from streamlit_folium import folium_static

from scripts.map_factory import (
    build_map,
    choose_default_field,
    load_admin_layers_from_db,
    load_dalarna_boundary_from_db,
    load_layers,
    load_theme_layer,
    load_wind_turbines_dalarna_buffer,
    load_plats_layers_from_db,
    load_sensitivity_layers_from_db,
)


st.set_page_config(page_title="Energiomstallning i Dalarna", layout="wide")
st.title("Energiomstallning i Dalarna")

repo_root = Path(__file__).resolve().parent


@st.cache_data(show_spinner=False, ttl=300)
def _cached_admin_layers():
    return load_admin_layers_from_db()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_lan_boundary():
    return load_dalarna_boundary_from_db()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_base_layers(repo_root_str: str):
    return load_layers(Path(repo_root_str))


@st.cache_data(show_spinner=False, ttl=300)
def _cached_theme_layer(repo_root_str: str, key: str):
    return load_theme_layer(Path(repo_root_str), key)


@st.cache_data(show_spinner=False, ttl=300)
def _cached_plats_layers():
    return load_plats_layers_from_db()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_sensitivity_layers():
    return load_sensitivity_layers_from_db()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_locked_point_layers(repo_root_str: str):
    gpkg = Path(repo_root_str) / "data" / "processed" / "locked_layers" / "novus_locked_points.gpkg"
    if not gpkg.exists():
        return None
    plats1 = gpd.read_file(gpkg, layer="plats_1").to_crs(4326)
    plats2 = gpd.read_file(gpkg, layer="plats_2").to_crs(4326)
    sensitive = gpd.read_file(gpkg, layer="plats_3_sensitive").to_crs(4326)
    non_sensitive = gpd.read_file(gpkg, layer="plats_4_not_sensitive").to_crs(4326)
    return plats1, plats2, sensitive, non_sensitive


@st.cache_data(show_spinner=False, ttl=600)
def _cached_wind_layers(repo_root_str: str, buffer_m: int):
    return load_wind_turbines_dalarna_buffer(Path(repo_root_str), buffer_m=buffer_m)


def _numkey(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace(".0", "", regex=False).str.strip()


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

    if filter_mode == "Koordinatlage (spatialt)":
        target = None
        if area_kind == "kommun" and kommuner is not None and len(kommuner) > 0:
            target = kommuner[kommuner["kommunnamn"].astype(str) == str(area_value)]
        elif area_kind == "kommungrupp" and kommungrupper is not None and len(kommungrupper) > 0:
            target = kommungrupper[kommungrupper["kommungrupp_namn"].astype(str) == str(area_value)]
        if target is None or len(target) == 0:
            return gdf.iloc[0:0].copy()
        return gdf[gdf.geometry.intersects(target.geometry.unary_union)]

    if area_kind == "kommun":
        code = kommun_code_by_name.get(area_value)
        if code is None:
            return gdf.iloc[0:0].copy()
        col = "home_kommunkod" if "home_kommunkod" in gdf.columns else ("kommunkod" if "kommunkod" in gdf.columns else None)
        if col is None:
            return gdf.iloc[0:0].copy()
        return gdf[_numkey(gdf[col]) == str(code)]

    if area_kind == "kommungrupp":
        gid = group_id_by_name.get(area_value)
        if gid is None:
            return gdf.iloc[0:0].copy()
        col = "home_kommungrupp" if "home_kommungrupp" in gdf.columns else "kommungrupp"
        return gdf[_numkey(gdf[col]) == str(gid)]

    return gdf


area_mode_options = ["Hela lanet", "Samtliga kommuner", "Samtliga kommungrupper"]
kommun_code_by_name: dict[str, str] = {}
group_id_by_name: dict[str, str] = {}
try:
    _k, _kg = _cached_admin_layers()
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

    st.subheader("Bakgrund")
    show_lan_boundary = st.checkbox("Visa lansgrans", value=False)
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
    show_plats1_points = st.checkbox("Visa Plats 1-punkter (farg efter kommungrupp)", value=False)
    show_plats2_points = st.checkbox("Visa Plats 2-punkter (farg efter kommungrupp)", value=False)
    show_sensitive_points = st.checkbox("Visa extra kansliga punkter", value=False)
    show_non_sensitive_points = st.checkbox("Visa inte extra kansliga punkter", value=False)
    sensitive_buffer_m = st.slider("Buffert runt extra kansliga punkter (meter)", 0, 3000, 0, 100)

    st.subheader("Vind")
    show_wind_turbines = st.checkbox("Visa vindkraftverk (Dalarna + 30 km)", value=False)

if selected_area == "Hela lanet":
    area_kind, area_value = "lan", ""
elif selected_area == "Samtliga kommuner":
    area_kind, area_value = "all_kommuner", ""
elif selected_area == "Samtliga kommungrupper":
    area_kind, area_value = "all_kommungrupper", ""
elif selected_area.startswith("Kommun: "):
    area_kind, area_value = "kommun", selected_area.replace("Kommun: ", "", 1)
else:
    area_kind, area_value = "kommungrupp", selected_area.replace("Kommungrupp: ", "", 1)

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
if show_kommuner or show_kommungrupper or area_kind in {"kommun", "kommungrupp", "all_kommuner", "all_kommungrupper"}:
    kommuner, kommungrupper = _cached_admin_layers()
if show_lan_boundary or area_kind == "lan":
    lan_boundary = _cached_lan_boundary()

plats1_points = plats2_points = sensitive_points = non_sensitive_points = None
if show_plats1_points or show_plats2_points or show_sensitive_points or show_non_sensitive_points:
    locked_layers = _cached_locked_point_layers(str(repo_root))
    if locked_layers is not None:
        plats1_points, plats2_points, sensitive_points, non_sensitive_points = locked_layers
    else:
        plats1_points, plats2_points = _cached_plats_layers()
        sensitive_points, non_sensitive_points = _cached_sensitivity_layers()

    plats1_points = _apply_area_filter(plats1_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    plats2_points = _apply_area_filter(plats2_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    sensitive_points = _apply_area_filter(sensitive_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)
    non_sensitive_points = _apply_area_filter(non_sensitive_points, filter_mode, area_kind, area_value, kommun_code_by_name, group_id_by_name, kommuner, kommungrupper)

wind_turbines = None
if show_wind_turbines:
    wind_turbines, _ = _cached_wind_layers(str(repo_root), 30000)

m = build_map(
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
    sensitive_buffer_m=sensitive_buffer_m,
    sty_opacity=0.6,
    show_landscape_colored_points=False,
    show_landscape_aggregated_points=False,
    wind_turbines=wind_turbines,
    show_wind_turbines=show_wind_turbines,
)

try:
    folium_static(m, width=1400, height=920)
except Exception:
    components.html(m.get_root().render(), height=920, scrolling=False)
