from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from scripts.map_factory import (
    build_map,
    choose_default_field,
    load_admin_layers_from_db,
    load_layers,
    load_plats_layers_from_db,
    load_sensitivity_layers_from_db,
    load_wind_turbines_dalarna_buffer,
)


st.set_page_config(page_title="Landskapstyper Dalarna", layout="wide")
st.title("Landskapstyper i Dalarna")

repo_root = Path(__file__).resolve().parent


@st.cache_data(show_spinner=False)
def _cached_layers(repo_root_str: str):
    return load_layers(Path(repo_root_str))


@st.cache_data(show_spinner=False, ttl=300)
def _cached_admin_layers():
    return load_admin_layers_from_db()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_plats_layers():
    return load_plats_layers_from_db()


@st.cache_data(show_spinner=False, ttl=300)
def _cached_sensitivity_layers():
    return load_sensitivity_layers_from_db()


@st.cache_data(show_spinner=False, ttl=600)
def _cached_wind_layers(repo_root_str: str, buffer_m: int):
    return load_wind_turbines_dalarna_buffer(Path(repo_root_str), buffer_m=buffer_m)


def _numkey(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace(".0", "", regex=False).str.strip()


def _filter_points_by_area(
    gdf,
    area_kind: str,
    area_value: str,
    kommun_code_by_name: dict[str, str],
    group_id_by_name: dict[str, str],
):
    if gdf is None or len(gdf) == 0 or area_kind == "lan":
        return gdf

    if area_kind == "kommun":
        code = kommun_code_by_name.get(area_value)
        if code is None:
            return gdf.iloc[0:0].copy()
        return gdf[_numkey(gdf["kommunkod"]) == str(code)]

    if area_kind == "kommungrupp":
        gid = group_id_by_name.get(area_value)
        if gid is None:
            return gdf.iloc[0:0].copy()
        return gdf[_numkey(gdf["kommungrupp"]) == str(gid)]

    return gdf


sty, kar = _cached_layers(str(repo_root))
sty_field = choose_default_field(sty)
kar_field = choose_default_field(kar)

area_mode_options = ["Hela lanet"]
kommun_code_by_name: dict[str, str] = {}
group_id_by_name: dict[str, str] = {}

try:
    _kommuner_all, _kommungrupper_all = _cached_admin_layers()
    kommun_pairs = (
        _kommuner_all[["kommunnamn", "kommunkod"]]
        .dropna()
        .drop_duplicates()
        .sort_values("kommunnamn")
    )
    group_pairs = (
        _kommungrupper_all[["kommungrupp_namn", "kommungrupp_id"]]
        .dropna()
        .drop_duplicates()
        .sort_values("kommungrupp_namn")
    )
    kommun_code_by_name = {
        str(r["kommunnamn"]): _numkey(pd.Series([r["kommunkod"]])).iloc[0]
        for _, r in kommun_pairs.iterrows()
    }
    group_id_by_name = {
        str(r["kommungrupp_namn"]): _numkey(pd.Series([r["kommungrupp_id"]])).iloc[0]
        for _, r in group_pairs.iterrows()
    }
    area_mode_options += [f"Kommun: {name}" for name in kommun_code_by_name.keys()]
    area_mode_options += [f"Kommungrupp: {name}" for name in group_id_by_name.keys()]
except Exception:
    pass

default_area = "Kommun: Smedjebacken"
default_idx = area_mode_options.index(default_area) if default_area in area_mode_options else 0
selected_area = st.selectbox("Arbetsomrade", area_mode_options, index=default_idx)

if selected_area == "Hela lanet":
    area_kind = "lan"
    area_value = ""
elif selected_area.startswith("Kommun: "):
    area_kind = "kommun"
    area_value = selected_area.replace("Kommun: ", "", 1)
else:
    area_kind = "kommungrupp"
    area_value = selected_area.replace("Kommungrupp: ", "", 1)

show_kar = st.checkbox("Visa Landskapskaraktar (konturer)", value=False)
show_kommuner = st.checkbox("Visa kommunpolygoner", value=True)
show_kommungrupper = st.checkbox("Visa kommungrupper", value=False)
show_plats1_points = st.checkbox("Visa Plats 1-punkter (farg efter kommungrupp)", value=False)
show_plats2_points = st.checkbox("Visa Plats 2-punkter (farg efter kommungrupp)", value=False)
show_sensitive_points = st.checkbox("Visa extra kansliga punkter", value=False)
show_non_sensitive_points = st.checkbox("Visa inte extra kansliga punkter", value=False)
sensitive_buffer_m = st.slider("Buffer runt extra kansliga punkter (meter)", min_value=0, max_value=3000, value=0, step=100)
sty_opacity = st.slider("Opacitet landskapstyper", min_value=0.0, max_value=1.0, value=0.6, step=0.05)
show_landscape_colored_points = st.checkbox("Visa punkter kategoriserade efter landskapstyp", value=False)
show_landscape_aggregated_points = st.checkbox("Visa aggregerade punktantal per landskapstyp", value=False)
show_wind_turbines = st.checkbox("Visa vindkraftverk (Dalarna + 30 km)", value=False)
show_wind_buffer = st.checkbox("Visa 30 km buffer for vindkrafturval", value=False)

kommuner = None
kommungrupper = None
plats1_points = None
plats2_points = None
sensitive_points = None
non_sensitive_points = None
wind_turbines = None
wind_buffer = None

if show_kommuner or show_kommungrupper:
    try:
        kommuner, kommungrupper = _cached_admin_layers()
    except Exception as exc:
        st.warning(f"Kunde inte ladda adm_indelning-lager fran databasen: {exc}")

if show_plats1_points or show_plats2_points:
    try:
        plats1_points, plats2_points = _cached_plats_layers()
        plats1_points = _filter_points_by_area(plats1_points, area_kind, area_value, kommun_code_by_name, group_id_by_name)
        plats2_points = _filter_points_by_area(plats2_points, area_kind, area_value, kommun_code_by_name, group_id_by_name)
    except Exception as exc:
        st.warning(f"Kunde inte ladda Novus punktlager fran databasen: {exc}")

if show_sensitive_points or show_non_sensitive_points:
    try:
        sensitive_points, non_sensitive_points = _cached_sensitivity_layers()
        sensitive_points = _filter_points_by_area(sensitive_points, area_kind, area_value, kommun_code_by_name, group_id_by_name)
        non_sensitive_points = _filter_points_by_area(non_sensitive_points, area_kind, area_value, kommun_code_by_name, group_id_by_name)
    except Exception as exc:
        st.warning(f"Kunde inte ladda kanslighetslager fran databasen: {exc}")

if show_landscape_colored_points or show_landscape_aggregated_points:
    try:
        if plats1_points is None or plats2_points is None:
            plats1_points, plats2_points = _cached_plats_layers()
            plats1_points = _filter_points_by_area(plats1_points, area_kind, area_value, kommun_code_by_name, group_id_by_name)
            plats2_points = _filter_points_by_area(plats2_points, area_kind, area_value, kommun_code_by_name, group_id_by_name)
        if sensitive_points is None:
            sensitive_points, non_sensitive_points = _cached_sensitivity_layers()
            sensitive_points = _filter_points_by_area(sensitive_points, area_kind, area_value, kommun_code_by_name, group_id_by_name)
            non_sensitive_points = _filter_points_by_area(non_sensitive_points, area_kind, area_value, kommun_code_by_name, group_id_by_name)
    except Exception as exc:
        st.warning(f"Kunde inte forbereda landskapstyp-klassade punktlager: {exc}")

if show_wind_turbines or show_wind_buffer:
    try:
        wind_turbines, wind_buffer = _cached_wind_layers(str(repo_root), 30000)
    except Exception as exc:
        st.warning(f"Kunde inte ladda vindkraftslager: {exc}")

m = build_map(
    sty=sty,
    kar=kar,
    sty_field=sty_field,
    kar_field=kar_field,
    show_kar=show_kar,
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
    sty_opacity=sty_opacity,
    show_landscape_colored_points=show_landscape_colored_points,
    show_landscape_aggregated_points=show_landscape_aggregated_points,
    wind_turbines=wind_turbines,
    show_wind_turbines=show_wind_turbines,
    wind_buffer=wind_buffer,
    show_wind_buffer=show_wind_buffer,
)
st_folium(m, width=None, height=760)
