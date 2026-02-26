from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

try:
    import plotly.graph_objects as go
except Exception:
    go = None

try:
    from pyvis.network import Network
except Exception:
    Network = None


st.set_page_config(page_title="Word x Kommun", layout="wide")

st.markdown(
    """
<style>
html, body, [class*="css"]  {
  font-family: "Segoe UI", "Calibri", "Arial", sans-serif;
}
h1, h2, h3 {
  font-family: "Segoe UI Semibold", "Segoe UI", "Calibri", sans-serif;
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("Ordanalys per kommun")
st.info(
    "Quick guide: 1) Choose focus word (or hem* definition), 2) review top kommuner/words, "
    "3) switch Sankey mode, 4) inspect Network 1 (kommun-context) and Network 2 (kommun-focus words)."
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = REPO_ROOT / "data" / "interim" / "hem_kommun_network"
HEM_SENTINEL = "hem*"
HEM_CORE_FORMS = {
    "hem",
    "hemma",
    "hemmet",
    "hemmets",
    "hemifran",
    "hemifrån",
    "hemort",
    "hemmavid",
    "hemkommun",
    "hemkomun",
}
HOME_THEME_PREFIXES = ("hem", "stug", "fritidshus", "bostad", "hus", "boend")
FOCUS_PREFIX_BUNDLES: dict[str, tuple[str, ...]] = {
    "skog*": ("skog",),
    "stuga*": ("stug",),
    "fäbod*": ("fäbod",),
    "fjäll*": ("fjäll",),
    "sommarstuga*": ("sommarstug",),
    "jaktmark*": ("jaktmark",),
    "utsikt*": ("utsikt",),
    "strand*": ("strand",),
}
FOCUS_EXACT_BUNDLES: dict[str, set[str]] = {
    "natur*": {"natur", "naturen"},
}
REQUIRED_ARTIFACTS = [
    "edges.csv",
    "nodes.csv",
    "hem_sankey.html",
    "hem_by_kommun.csv",
    "hem_forms_frequency.csv",
    "response_tokens.csv",
]


def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def missing_committed_artifacts(base: Path) -> list[str]:
    missing: list[str] = []

    for name in REQUIRED_ARTIFACTS:
        if not (base / name).exists():
            missing.append(name)

    if not list(base.glob("hem_context_*.csv")):
        missing.append("hem_context_*.csv")
    if not list(base.glob("word_frequency*.csv")):
        missing.append("word_frequency*.csv")

    sankey_assets = base / "hem_sankey_files"
    if not sankey_assets.exists():
        missing.append("hem_sankey_files/")

    return missing


@st.cache_data(show_spinner=False)
def load_data(out_dir: str) -> dict[str, pd.DataFrame]:
    base = Path(out_dir)
    return {
        "tokens": read_csv_safe(base / "response_tokens.csv"),
        "word_freq": read_csv_safe(base / "word_frequency.csv"),
    }


def normalize_word(x: str) -> str:
    return (x or "").strip().lower()


def make_token_mask(token_series: pd.Series, focus_item: str, match_mode: str, hem_definition: str) -> pd.Series:
    token_series = token_series.astype(str).str.lower()

    if focus_item in FOCUS_PREFIX_BUNDLES:
        return token_series.str.startswith(FOCUS_PREFIX_BUNDLES[focus_item])

    if focus_item in FOCUS_EXACT_BUNDLES:
        return token_series.isin(FOCUS_EXACT_BUNDLES[focus_item])

    if focus_item != HEM_SENTINEL:
        if match_mode == "exact token":
            return token_series == focus_item
        return token_series.str.startswith(focus_item)

    if hem_definition == "hem-core (narrow)":
        return token_series.isin(HEM_CORE_FORMS)
    if hem_definition == "home-theme (hem+stuga+...)":
        return token_series.str.startswith(HOME_THEME_PREFIXES)
    return token_series.str.startswith("hem")


def focus_rule_text(focus_item: str, match_mode: str, hem_definition: str) -> str:
    if focus_item in FOCUS_PREFIX_BUNDLES:
        prefixes = "|".join(FOCUS_PREFIX_BUNDLES[focus_item])
        return f"token starts with {prefixes}"

    if focus_item in FOCUS_EXACT_BUNDLES:
        forms = ", ".join(sorted(FOCUS_EXACT_BUNDLES[focus_item]))
        return f"token in {{{forms}}}"

    if focus_item != HEM_SENTINEL:
        if match_mode == "exact token":
            return f"token equals '{focus_item}'"
        return f"token starts with '{focus_item}'"

    if hem_definition == "hem-core (narrow)":
        return "token in {hem, hemma, hemmet, hemifran, hemort, ...}"
    if hem_definition == "home-theme (hem+stuga+...)":
        return "token starts with hem|stug|fritidshus|bostad|hus|boend"
    return "token starts with 'hem' (wide hem*)"


def compute_focus_outputs(
    tokens: pd.DataFrame, focus_item: str, match_mode: str, hem_definition: str
) -> dict[str, pd.DataFrame | int | float]:
    t = tokens.copy()
    t["token"] = t["token"].astype(str).str.lower()
    t["kommun"] = t["kommun"].astype(str)
    t["is_focus_token"] = make_token_mask(t["token"], focus_item, match_mode, hem_definition)

    responses = t[["response_id", "kommun"]].drop_duplicates()
    n_responses = len(responses)

    focus_response_ids = set(t.loc[t["is_focus_token"], "response_id"].astype(str).tolist())
    response_flags = responses.copy()
    response_flags["has_focus"] = response_flags["response_id"].astype(str).isin(focus_response_ids)

    by_kommun = (
        response_flags.groupby("kommun", as_index=False)
        .agg(
            responses=("response_id", "count"),
            focus_count=("has_focus", "sum"),
        )
        .sort_values(["focus_count", "responses"], ascending=False)
    )
    by_kommun["focus_share"] = by_kommun["focus_count"] / by_kommun["responses"].clip(lower=1)

    t_focus = t[t["response_id"].astype(str).isin(focus_response_ids)].copy()
    t_focus = t_focus[~t_focus["is_focus_token"]]

    # Context words aggregated across all municipalities.
    context = (
        t_focus[["response_id", "kommun", "token"]]
        .drop_duplicates()
        .groupby("token", as_index=False)
        .size()
        .rename(columns={"token": "context_word", "size": "n"})
        .sort_values("n", ascending=False)
    )

    # Municipality -> context word edges (no focus->context link).
    kommun_word_edges = (
        t_focus[["response_id", "kommun", "token"]]
        .drop_duplicates()
        .groupby(["kommun", "token"], as_index=False)
        .size()
        .rename(columns={"token": "word", "size": "weight"})
    )

    word_focus_counts = (
        t_focus[["response_id", "token"]]
        .drop_duplicates()
        .groupby("token", as_index=False)
        .size()
        .rename(columns={"token": "word", "size": "focus_count"})
    )

    return {
        "responses_total": int(n_responses),
        "focus_count": int(response_flags["has_focus"].sum()),
        "focus_share": float(response_flags["has_focus"].mean()) if n_responses else 0.0,
        "by_kommun": by_kommun,
        "context": context,
        "kommun_word_edges": kommun_word_edges,
        "word_focus_counts": word_focus_counts,
        "focus_response_ids": pd.DataFrame({"response_id": list(focus_response_ids)}),
    }


def render_pyvis_network(
    by_kommun: pd.DataFrame,
    kommun_word_edges: pd.DataFrame,
    word_focus_counts: pd.DataFrame,
    min_edge_weight: int,
    gravity: float,
    central_gravity: float,
    spring_length: int,
    spring_strength: float,
    damping: float,
    node_font_size: int,
) -> str:
    edges = kommun_word_edges.copy()
    edges = edges[edges["weight"] >= min_edge_weight]
    if edges.empty:
        return ""

    net = Network(height="760px", width="100%", bgcolor="#ffffff", font_color="#1f2933", directed=False)
    net.barnes_hut(
        gravity=gravity,
        central_gravity=central_gravity,
        spring_length=spring_length,
        spring_strength=spring_strength,
        damping=damping,
    )

    kommun_map = by_kommun.set_index("kommun")
    word_map = word_focus_counts.set_index("word")

    for kommun in sorted(edges["kommun"].unique().tolist()):
        responses = int(kommun_map.loc[kommun, "responses"]) if kommun in kommun_map.index else 1
        focus_count = int(kommun_map.loc[kommun, "focus_count"]) if kommun in kommun_map.index else 0
        node_size = 10 + min(55, responses ** 0.5 * 1.8)
        net.add_node(
            n_id=f"k::{kommun}",
            label=kommun,
            title=f"<b>Kommun</b><br>{kommun}<br>responses: {responses}<br>focus_count: {focus_count}",
            color="#4c78a8",
            size=node_size,
            shape="dot",
            group="kommun",
        )

    for word in sorted(edges["word"].unique().tolist()):
        focus_count = int(word_map.loc[word, "focus_count"]) if word in word_map.index else 1
        node_size = 8 + min(45, focus_count ** 0.5 * 3.0)
        net.add_node(
            n_id=f"w::{word}",
            label=word,
            title=f"<b>Word</b><br>{word}<br>focus_count: {focus_count}",
            color="#54a24b",
            size=node_size,
            shape="dot",
            group="word",
        )

    max_w = max(1, int(edges["weight"].max()))
    for _, row in edges.iterrows():
        width = 1 + (8 * (float(row["weight"]) / max_w))
        net.add_edge(
            source=f"k::{row['kommun']}",
            to=f"w::{row['word']}",
            value=float(row["weight"]),
            width=width,
            title=f"{row['kommun']} -> {row['word']} | count={int(row['weight'])}",
            color="rgba(120,120,120,0.5)",
        )

    net.set_options(
        """
var options = {
  "nodes": {
    "font": { "size": """
        + str(node_font_size)
        + """, "face": "Segoe UI", "strokeWidth": 4, "strokeColor": "#ffffff" }
  },
  "edges": {
    "smooth": false
  },
  "physics": {
    "enabled": true,
    "stabilization": { "enabled": true, "iterations": 1500, "updateInterval": 25 }
  },
  "interaction": {
    "hover": true,
    "navigationButtons": true
  }
}
"""
    )
    return net.generate_html()


def build_focus_word_edges(
    tokens: pd.DataFrame,
    focus_words: list[str],
    hem_definition: str,
    focus_word_match_mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    t = tokens.copy()
    t["token"] = t["token"].astype(str).str.lower()
    t["kommun"] = t["kommun"].astype(str)

    edges_parts: list[pd.DataFrame] = []
    counts_parts: list[pd.DataFrame] = []

    for fw in focus_words:
        fw_norm = normalize_word(fw)
        if not fw_norm:
            continue

        if fw_norm == HEM_SENTINEL:
            mask = make_token_mask(t["token"], HEM_SENTINEL, "prefix (word*)", hem_definition)
        elif focus_word_match_mode == "prefix (word*)":
            mask = t["token"].str.startswith(fw_norm)
        else:
            mask = t["token"] == fw_norm

        hits = t[mask][["response_id", "kommun"]].drop_duplicates()
        if hits.empty:
            continue

        edge = hits.groupby("kommun", as_index=False).size().rename(columns={"size": "weight"})
        edge["word"] = fw_norm
        edges_parts.append(edge)

        count_df = (
            hits.groupby("response_id", as_index=False)
            .size()
            .assign(word=fw_norm)
            .groupby("word", as_index=False)
            .size()
            .rename(columns={"size": "focus_count"})
        )
        counts_parts.append(count_df)

    if not edges_parts:
        return pd.DataFrame(columns=["kommun", "word", "weight"]), pd.DataFrame(columns=["word", "focus_count"])

    edges = pd.concat(edges_parts, ignore_index=True)[["kommun", "word", "weight"]]
    counts = pd.concat(counts_parts, ignore_index=True).groupby("word", as_index=False)["focus_count"].sum()
    return edges, counts


def build_focus_word_context_edges(
    tokens: pd.DataFrame,
    focus_words: list[str],
    hem_definition: str,
    focus_word_match_mode: str,
    top_context_per_focus: int,
) -> pd.DataFrame:
    t = tokens.copy()
    t["token"] = t["token"].astype(str).str.lower()
    t["kommun"] = t["kommun"].astype(str)

    out_parts: list[pd.DataFrame] = []

    for fw in focus_words:
        fw_norm = normalize_word(fw)
        if not fw_norm:
            continue

        if fw_norm == HEM_SENTINEL:
            mask = make_token_mask(t["token"], HEM_SENTINEL, "prefix (word*)", hem_definition)
        elif focus_word_match_mode == "prefix (word*)":
            mask = t["token"].str.startswith(fw_norm)
        else:
            mask = t["token"] == fw_norm

        focus_response_ids = set(t.loc[mask, "response_id"].astype(str).tolist())
        if not focus_response_ids:
            continue

        t_focus = t[t["response_id"].astype(str).isin(focus_response_ids)].copy()
        t_focus = t_focus[~mask.reindex(t_focus.index, fill_value=False)]
        if t_focus.empty:
            continue

        ctx = (
            t_focus[["response_id", "token"]]
            .drop_duplicates()
            .groupby("token", as_index=False)
            .size()
            .rename(columns={"token": "context_word", "size": "weight"})
            .sort_values("weight", ascending=False)
        )
        if top_context_per_focus > 0:
            ctx = ctx.head(top_context_per_focus)
        if ctx.empty:
            continue

        ctx["focus_word"] = fw_norm
        out_parts.append(ctx[["focus_word", "context_word", "weight"]])

    if not out_parts:
        return pd.DataFrame(columns=["focus_word", "context_word", "weight"])

    return pd.concat(out_parts, ignore_index=True)


def render_pyvis_focus_word_network(
    by_kommun: pd.DataFrame,
    edges: pd.DataFrame,
    word_counts: pd.DataFrame,
    min_edge_weight: int,
    gravity: float,
    central_gravity: float,
    spring_length: int,
    spring_strength: float,
    damping: float,
    node_font_size: int,
) -> str:
    if edges.empty:
        return ""

    use_edges = edges[edges["weight"] >= min_edge_weight].copy()
    if use_edges.empty:
        return ""

    net = Network(height="760px", width="100%", bgcolor="#ffffff", font_color="#1f2933", directed=False)
    net.barnes_hut(
        gravity=gravity,
        central_gravity=central_gravity,
        spring_length=spring_length,
        spring_strength=spring_strength,
        damping=damping,
    )

    kommun_map = by_kommun.set_index("kommun")
    word_map = word_counts.set_index("word") if not word_counts.empty else pd.DataFrame()

    for kommun in sorted(use_edges["kommun"].unique().tolist()):
        responses = int(kommun_map.loc[kommun, "responses"]) if kommun in kommun_map.index else 1
        node_size = 10 + min(55, responses ** 0.5 * 1.8)
        net.add_node(
            n_id=f"k::{kommun}",
            label=kommun,
            title=f"<b>Kommun</b><br>{kommun}<br>responses: {responses}",
            color="#4c78a8",
            size=node_size,
            shape="dot",
            group="kommun",
        )

    for word in sorted(use_edges["word"].unique().tolist()):
        focus_count = int(word_map.loc[word, "focus_count"]) if (not word_map.empty and word in word_map.index) else 1
        node_size = 9 + min(42, focus_count ** 0.5 * 3.0)
        net.add_node(
            n_id=f"f::{word}",
            label=word,
            title=f"<b>Fokusord</b><br>{word}<br>focus_count: {focus_count}",
            color="#f58518",
            size=node_size,
            shape="dot",
            group="focus_word",
        )

    max_w = max(1, int(use_edges["weight"].max()))
    for _, row in use_edges.iterrows():
        width = 1 + (8 * (float(row["weight"]) / max_w))
        net.add_edge(
            source=f"k::{row['kommun']}",
            to=f"f::{row['word']}",
            value=float(row["weight"]),
            width=width,
            title=f"{row['kommun']} -> {row['word']} | count={int(row['weight'])}",
            color="rgba(120,120,120,0.5)",
        )

    net.set_options(
        """
var options = {
  "nodes": {
    "font": { "size": """
        + str(node_font_size)
        + """, "face": "Segoe UI", "strokeWidth": 4, "strokeColor": "#ffffff" }
  },
  "edges": {
    "smooth": false
  },
  "physics": {
    "enabled": true,
    "stabilization": { "enabled": true, "iterations": 1500, "updateInterval": 25 }
  },
  "interaction": {
    "hover": true,
    "navigationButtons": true
  }
}
"""
    )
    return net.generate_html()


with st.sidebar:
    st.header("Settings")
    out_dir = str(DEFAULT_OUT)
    with st.expander("Advanced: custom output folder"):
        out_dir = st.text_input("Output folder from R script", value=out_dir)
    match_mode = st.radio("Match mode (for normal words)", ["exact token", "prefix (word*)"], index=1)
    top_n_kommun = st.slider("Top-N kommuner (tables/sankey)", min_value=5, max_value=20, value=5, step=1)
    top_m_context = st.slider("Top-M words (table/sankey)", min_value=5, max_value=60, value=5, step=1)
    network_min_edge = st.slider("Network min edge count", min_value=1, max_value=10, value=5, step=1)
    focus_net_n_words = st.slider("Network2 focus words (top N)", min_value=5, max_value=80, value=20, step=5)
    focus_net_match_mode = st.radio("Network2 match", ["exact token", "prefix (word*)"], index=0)
    st.markdown("### Sankey")
    sankey_mode = st.radio(
        "Sankey mode",
        ["kommun -> contextord", "kommun -> fokusord -> contextord"],
        index=0,
    )
    sankey_n_focus_words = st.slider("Sankey focus words (top N)", min_value=3, max_value=40, value=12, step=1)
    sankey_context_per_focus = st.slider("Sankey context per focus", min_value=3, max_value=20, value=8, step=1)
    st.markdown("### Network layout")
    physics_preset = st.selectbox("Physics preset", ["Spread out", "Balanced", "Compact"], index=0)
    node_font_size = st.slider("Node label size", min_value=12, max_value=24, value=18, step=1)

if physics_preset == "Spread out":
    physics_gravity = -55000
    physics_central_gravity = 0.08
    physics_spring_length = 280
    physics_spring_strength = 0.006
    physics_damping = 0.60
elif physics_preset == "Compact":
    physics_gravity = -12000
    physics_central_gravity = 0.30
    physics_spring_length = 120
    physics_spring_strength = 0.02
    physics_damping = 0.35
else:
    physics_gravity = -25000
    physics_central_gravity = 0.18
    physics_spring_length = 180
    physics_spring_strength = 0.01
    physics_damping = 0.50

frames = load_data(out_dir)
tokens = frames["tokens"].copy()
word_freq = frames["word_freq"].copy()

missing_bundle = missing_committed_artifacts(Path(out_dir))
if missing_bundle:
    st.error(
        "Missing committed artifacts: data/interim/hem_kommun_network. "
        "Run scripts/build_hem_kommun_network.py locally and commit the outputs."
    )
    st.caption("Missing files/patterns: " + ", ".join(missing_bundle))
    st.stop()

if tokens.empty:
    st.error(
        "Missing committed artifacts: data/interim/hem_kommun_network. "
        "Run scripts/build_hem_kommun_network.py locally and commit the outputs."
    )
    st.stop()

if "token" not in word_freq.columns:
    common_words = sorted(tokens["token"].astype(str).str.lower().value_counts().head(200).index.tolist())
else:
    common_words = word_freq["token"].astype(str).str.lower().head(200).tolist()

bundle_options = [HEM_SENTINEL] + sorted(
    list(FOCUS_PREFIX_BUNDLES.keys()) + list(FOCUS_EXACT_BUNDLES.keys())
)
focus_options = bundle_options + [w for w in common_words if w not in bundle_options]

with st.sidebar:
    st.markdown("---")
    focus_select = st.selectbox("Focus word", options=focus_options, index=0)
    focus_custom = st.text_input("Or type another word", value="")
    st.caption("Bundled focus options: " + ", ".join(bundle_options))

    hem_definition = "hem* (wide)"
    if (not normalize_word(focus_custom) and focus_select == HEM_SENTINEL) or normalize_word(focus_custom) == HEM_SENTINEL:
        st.markdown("### Definition of hem")
        hem_definition = st.radio(
            "Used when focus is hem*",
            ["hem* (wide)", "hem-core (narrow)", "home-theme (hem+stuga+...)"],
            index=0,
        )
        st.caption(
            "hem* (wide): starts with 'hem'. "
            "hem-core: home-related whitelist. "
            "home-theme: hem/stuga/fritidshus/bostad/hus/boende."
        )

focus_item = normalize_word(focus_custom) if normalize_word(focus_custom) else normalize_word(focus_select)
if not focus_item:
    st.error("Choose a focus word.")
    st.stop()

result = compute_focus_outputs(tokens=tokens, focus_item=focus_item, match_mode=match_mode, hem_definition=hem_definition)

st.markdown(
    f"""
<div style="padding:10px 14px;border:1px solid #d9d9d9;border-radius:10px;background:#fff8e6;">
  <span style="font-size:14px;color:#555;">Active focus word</span><br>
  <span style="font-size:28px;font-weight:700;color:#b45309;">{focus_item}</span>
</div>
""",
    unsafe_allow_html=True,
)

k1, k2, k3 = st.columns(3)
k1.metric("Responses total", f"{result['responses_total']}")
k2.metric(f"Responses with '{focus_item}'", f"{result['focus_count']}")
k3.metric("Share", f"{result['focus_share']:.1%}")

st.caption(f"Focus rule: {focus_rule_text(focus_item, match_mode, hem_definition)}")

st.subheader("Top kommuner")
top_kommun_df = result["by_kommun"].head(top_n_kommun).copy()
st.dataframe(top_kommun_df, use_container_width=True, hide_index=True)

st.subheader("Top context words")
top_ctx_df = result["context"].head(top_m_context).copy()
if top_ctx_df.empty:
    st.info("No context words found for this focus word.")
else:
    st.dataframe(top_ctx_df, use_container_width=True, hide_index=True)

st.subheader("Sankey: kommun -> contextord (inom fokus-svar)")
if go is None:
    st.warning("Plotly not installed. Install with: pip install plotly")
else:
    top_kommun = top_kommun_df["kommun"].tolist()

    if sankey_mode == "kommun -> contextord":
        edges = result["kommun_word_edges"].copy()
        top_words = top_ctx_df["context_word"].tolist()
        edges = edges[edges["kommun"].isin(top_kommun) & edges["word"].isin(top_words)]

        if edges.empty:
            st.info("No links to show.")
        else:
            labels = pd.Index(pd.unique(pd.concat([edges["kommun"], edges["word"]]))).tolist()
            idx = {name: i for i, name in enumerate(labels)}
            source = edges["kommun"].map(idx).tolist()
            target = edges["word"].map(idx).tolist()
            value = edges["weight"].astype(float).tolist()

            node_colors = ["#4c78a8" if name in top_kommun else "#54a24b" for name in labels]
            fig = go.Figure(
                data=[
                    go.Sankey(
                        node=dict(label=labels, pad=12, thickness=16, color=node_colors),
                        link=dict(source=source, target=target, value=value, color="rgba(120,120,120,0.45)"),
                        textfont=dict(size=16, family="Segoe UI"),
                    )
                ]
            )
            fig.update_layout(
                height=680,
                margin=dict(l=10, r=10, t=20, b=10),
                font=dict(family="Segoe UI, Calibri, Arial, sans-serif", size=16, color="#1f2933"),
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        focus_word_list = focus_options[:sankey_n_focus_words]
        fw_edges, _fw_counts = build_focus_word_edges(
            tokens=tokens,
            focus_words=focus_word_list,
            hem_definition=hem_definition,
            focus_word_match_mode=focus_net_match_mode,
        )
        fw_edges = fw_edges[fw_edges["kommun"].isin(top_kommun)]

        ctx_edges = build_focus_word_context_edges(
            tokens=tokens,
            focus_words=focus_word_list,
            hem_definition=hem_definition,
            focus_word_match_mode=focus_net_match_mode,
            top_context_per_focus=sankey_context_per_focus,
        )

        if fw_edges.empty or ctx_edges.empty:
            st.info("No links to show in three-step Sankey.")
        else:
            links_left = fw_edges.rename(columns={"kommun": "from", "word": "to"})[["from", "to", "weight"]]
            links_right = ctx_edges.rename(columns={"focus_word": "from", "context_word": "to"})[["from", "to", "weight"]]
            links = pd.concat([links_left, links_right], ignore_index=True)

            labels = pd.Index(pd.unique(pd.concat([links["from"], links["to"]]))).tolist()
            idx = {name: i for i, name in enumerate(labels)}
            source = links["from"].map(idx).tolist()
            target = links["to"].map(idx).tolist()
            value = links["weight"].astype(float).tolist()

            focus_set = set(focus_word_list)
            context_set = set(ctx_edges["context_word"].tolist())
            node_colors = []
            for name in labels:
                if name in top_kommun:
                    node_colors.append("#4c78a8")
                elif name in focus_set:
                    node_colors.append("#f58518")
                elif name in context_set:
                    node_colors.append("#54a24b")
                else:
                    node_colors.append("#bab0ac")

            fig = go.Figure(
                data=[
                    go.Sankey(
                        node=dict(label=labels, pad=12, thickness=16, color=node_colors),
                        link=dict(source=source, target=target, value=value, color="rgba(120,120,120,0.42)"),
                        textfont=dict(size=16, family="Segoe UI"),
                    )
                ]
            )
            fig.update_layout(
                height=720,
                margin=dict(l=10, r=10, t=20, b=10),
                font=dict(family="Segoe UI, Calibri, Arial, sans-serif", size=16, color="#1f2933"),
            )
            st.plotly_chart(fig, use_container_width=True)

st.subheader("Free network: kommun <-> ord (utan focus->context-lager)")
st.caption(
    "Detta natverk visar kommun -> kontextord inom svar som matchar valt fokusord. "
    "Det ar en kontextordlista, inte listan av fokusord."
)
if Network is None:
    st.warning("PyVis not installed. Install with: pip install pyvis")
else:
    html = render_pyvis_network(
        by_kommun=result["by_kommun"],
        kommun_word_edges=result["kommun_word_edges"],
        word_focus_counts=result["word_focus_counts"],
        min_edge_weight=network_min_edge,
        gravity=physics_gravity,
        central_gravity=physics_central_gravity,
        spring_length=physics_spring_length,
        spring_strength=physics_spring_strength,
        damping=physics_damping,
        node_font_size=node_font_size,
    )
    if html:
        components.html(html, height=790, scrolling=False)
    else:
        st.info("No network edges after current filters.")

st.subheader("Network 2: kommun <-> fokusord (separat ordlista)")
st.caption(
    "Detta natverk anvander en annan ordlista: toppfokusord fran ord-frekvensen (inklusive hem*). "
    "Det visar relationen mellan kommuner och fokusord direkt."
)
if Network is None:
    st.warning("PyVis not installed. Install with: pip install pyvis")
else:
    focus_word_list = focus_options[:focus_net_n_words]
    fw_edges, fw_counts = build_focus_word_edges(
        tokens=tokens,
        focus_words=focus_word_list,
        hem_definition=hem_definition,
        focus_word_match_mode=focus_net_match_mode,
    )
    fw_html = render_pyvis_focus_word_network(
        by_kommun=result["by_kommun"],
        edges=fw_edges,
        word_counts=fw_counts,
        min_edge_weight=network_min_edge,
        gravity=physics_gravity,
        central_gravity=physics_central_gravity,
        spring_length=physics_spring_length,
        spring_strength=physics_spring_strength,
        damping=physics_damping,
        node_font_size=node_font_size,
    )
    if fw_html:
        components.html(fw_html, height=790, scrolling=False)
    else:
        st.info("No edges in focus-word network with current filters.")

with st.expander("How matching and visuals work"):
    st.markdown(
        """
- Sankey now shows **municipality -> context word** links directly.
- Sankey can also be switched to **municipality -> focus word -> context word**.
- Weight = number of responses in that municipality where the word appears, within responses that match current focus.
- Network shows the same municipality-word relation (no focus->context layer).
- Network 2 shows municipality -> focus-word relations from a separate focus-word list.
- Edge width = count.
- Municipality node size = responses.
- Word node size = focus_count.
"""
    )
