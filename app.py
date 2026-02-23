from __future__ import annotations

from pathlib import Path

import streamlit as st
from streamlit_folium import st_folium

from scripts.map_factory import (
    build_map,
    choose_default_field,
    load_admin_layers_from_db,
    load_layers,
)


st.set_page_config(page_title="Landskapstyper Dalarna", layout="wide")
st.title("Landskapstyper i Dalarna")

repo_root = Path(__file__).resolve().parent

sty, kar = load_layers(repo_root)
sty_field = choose_default_field(sty)
kar_field = choose_default_field(kar)

show_kar = st.checkbox("Visa Landskapskaraktar (konturer)", value=False)
show_kommuner = st.checkbox("Visa kommunpolygoner", value=True)
show_kommungrupper = st.checkbox("Visa kommungrupper", value=False)

dalarna_lan = None
kommuner = None
kommungrupper = None

if show_kommuner or show_kommungrupper:
    try:
        dalarna_lan, kommuner, kommungrupper = load_admin_layers_from_db()
    except Exception as exc:
        st.warning(f"Kunde inte ladda adm_indelning-lager från databasen: {exc}")

m = build_map(
    sty=sty,
    kar=kar,
    sty_field=sty_field,
    kar_field=kar_field,
    show_kar=show_kar,
    dalarna_lan=dalarna_lan,
    kommuner=kommuner,
    kommungrupper=kommungrupper,
    show_kommuner=show_kommuner,
    show_kommungrupper=show_kommungrupper,
)
st_folium(m, width=None, height=760)

