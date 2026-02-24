from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

try:
    import plotly.graph_objects as go
except Exception:
    go = None


st.set_page_config(page_title="Word x Kommun", layout="wide")
st.title("Ordanalys per kommun")

DEFAULT_OUT = Path("data/interim/hem_kommun_network")


def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


@st.cache_data(show_spinner=False)
def load_data(out_dir: str) -> dict[str, pd.DataFrame]:
    base = Path(out_dir)
    return {
        "tokens": read_csv_safe(base / "response_tokens.csv"),
        "word_freq": read_csv_safe(base / "word_frequency.csv"),
    }


def normalize_word(x: str) -> str:
    return (x or "").strip().lower()


def compute_focus_outputs(tokens: pd.DataFrame, focus_word: str, use_prefix: bool) -> dict[str, pd.DataFrame | int | float]:
    t = tokens.copy()
    t["token"] = t["token"].astype(str).str.lower()
    t["kommun"] = t["kommun"].astype(str)

    responses = t[["response_id", "kommun"]].drop_duplicates()
    n_responses = len(responses)

    if use_prefix:
        mask_focus = t["token"].str.startswith(focus_word)
    else:
        mask_focus = t["token"] == focus_word

    focus_hits = t[mask_focus]
    focus_responses = focus_hits[["response_id"]].drop_duplicates()
    focus_response_ids = set(focus_responses["response_id"].tolist())

    response_flags = responses.copy()
    response_flags["has_focus"] = response_flags["response_id"].isin(focus_response_ids)

    by_kommun = (
        response_flags.groupby("kommun", as_index=False)
        .agg(
            responses=("response_id", "count"),
            focus_count=("has_focus", "sum"),
        )
    )
    by_kommun["focus_share"] = by_kommun["focus_count"] / by_kommun["responses"].clip(lower=1)
    by_kommun = by_kommun.sort_values(["focus_count", "focus_share"], ascending=False)

    t_focus = t[t["response_id"].isin(focus_response_ids)].copy()
    if use_prefix:
        t_focus = t_focus[~t_focus["token"].str.startswith(focus_word)]
    else:
        t_focus = t_focus[t_focus["token"] != focus_word]

    context = (
        t_focus[["response_id", "kommun", "token"]]
        .drop_duplicates()
        .groupby("token", as_index=False)
        .size()
        .rename(columns={"token": "context_word", "size": "n"})
        .sort_values("n", ascending=False)
    )

    edge_kommun_focus = (
        response_flags[response_flags["has_focus"]]
        .groupby("kommun", as_index=False)
        .size()
        .rename(columns={"size": "weight"})
    )
    edge_kommun_focus["from"] = edge_kommun_focus["kommun"]
    edge_kommun_focus["to"] = focus_word
    edge_kommun_focus["relation"] = "kommun_to_focus"
    edge_kommun_focus = edge_kommun_focus[["from", "to", "weight", "relation"]]

    edge_focus_context = context.rename(columns={"context_word": "to", "n": "weight"}).copy()
    edge_focus_context["from"] = focus_word
    edge_focus_context["relation"] = "focus_to_context"
    edge_focus_context = edge_focus_context[["from", "to", "weight", "relation"]]

    return {
        "responses_total": int(n_responses),
        "focus_count": int(response_flags["has_focus"].sum()),
        "focus_share": float(response_flags["has_focus"].mean()) if n_responses else 0.0,
        "by_kommun": by_kommun,
        "context": context,
        "edge_kommun_focus": edge_kommun_focus,
        "edge_focus_context": edge_focus_context,
    }


with st.sidebar:
    st.header("Settings")
    out_dir = st.text_input("Output folder from R script", value=str(DEFAULT_OUT))
    match_mode = st.radio("Match mode", ["exact token", "prefix (word*)"], index=1)
    top_n_kommun = st.slider("Top-N kommuner", min_value=5, max_value=20, value=10, step=1)
    top_m_context = st.slider("Top-M context words", min_value=10, max_value=50, value=20, step=1)

frames = load_data(out_dir)

tokens = frames["tokens"].copy()
word_freq = frames["word_freq"].copy()

if tokens.empty:
    st.error("Missing response_tokens.csv. Run: Rscript scripts/hem_kommun_network.R")
    st.stop()

if "token" not in word_freq.columns:
    common_words = sorted(tokens["token"].astype(str).str.lower().value_counts().head(200).index.tolist())
else:
    common_words = word_freq["token"].astype(str).str.lower().head(200).tolist()

with st.sidebar:
    focus_select = st.selectbox("Focus word (from top list)", options=common_words, index=common_words.index("hem") if "hem" in common_words else 0)
    focus_custom = st.text_input("Or type another word", value="")

focus_word = normalize_word(focus_custom) if normalize_word(focus_custom) else normalize_word(focus_select)
use_prefix = match_mode.startswith("prefix")

if not focus_word:
    st.error("Choose a focus word.")
    st.stop()

result = compute_focus_outputs(tokens, focus_word=focus_word, use_prefix=use_prefix)

k1, k2, k3 = st.columns(3)
k1.metric("Responses total", f"{result['responses_total']}")
k2.metric(f"Responses with '{focus_word}'", f"{result['focus_count']}")
k3.metric("Share", f"{result['focus_share']:.1%}")

st.caption(
    f"Focus rule: {'token starts with' if use_prefix else 'token equals'} '{focus_word}'"
)

st.subheader("Top kommuner")
top_kommun_df = result["by_kommun"].head(top_n_kommun).copy()
st.dataframe(top_kommun_df, use_container_width=True, hide_index=True)

st.subheader("Top context words")
top_ctx_df = result["context"].head(top_m_context).copy()
if top_ctx_df.empty:
    st.info("No context words found for this focus word.")
else:
    st.dataframe(top_ctx_df, use_container_width=True, hide_index=True)

st.subheader("Sankey: kommun -> focus -> context")
if go is None:
    st.warning("Plotly not installed. Install with: pip install plotly")
else:
    komm_edges = result["edge_kommun_focus"].sort_values("weight", ascending=False).head(top_n_kommun)
    ctx_edges = result["edge_focus_context"].sort_values("weight", ascending=False).head(top_m_context)
    sankey_edges = pd.concat([komm_edges, ctx_edges], ignore_index=True)

    if sankey_edges.empty:
        st.info("No links to show.")
    else:
        labels = pd.Index(pd.unique(pd.concat([sankey_edges["from"], sankey_edges["to"]]))).tolist()
        idx = {name: i for i, name in enumerate(labels)}
        source = sankey_edges["from"].map(idx).tolist()
        target = sankey_edges["to"].map(idx).tolist()
        value = sankey_edges["weight"].astype(float).tolist()

        kommun_set = set(komm_edges["from"].tolist())
        context_set = set(ctx_edges["to"].tolist())
        colors = []
        for name in labels:
            if name == focus_word:
                colors.append("#f58518")
            elif name in kommun_set:
                colors.append("#4c78a8")
            elif name in context_set:
                colors.append("#54a24b")
            else:
                colors.append("#bab0ac")

        fig = go.Figure(
            data=[
                go.Sankey(
                    node=dict(label=labels, pad=12, thickness=16, color=colors),
                    link=dict(source=source, target=target, value=value),
                )
            ]
        )
        fig.update_layout(height=620, margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(fig, use_container_width=True)

st.subheader("Simple network view")
if go is None:
    st.info("Network view requires plotly.")
else:
    komm_edges = result["edge_kommun_focus"].sort_values("weight", ascending=False).head(top_n_kommun)
    ctx_edges = result["edge_focus_context"].sort_values("weight", ascending=False).head(top_m_context)
    net_edges = pd.concat([komm_edges, ctx_edges], ignore_index=True)

    if net_edges.empty:
        st.info("No network links to show.")
    else:
        komm_nodes = sorted(komm_edges["from"].unique().tolist())
        ctx_nodes = sorted(ctx_edges["to"].unique().tolist())

        pos = {}
        if komm_nodes:
            for i, node in enumerate(komm_nodes):
                pos[node] = (0.0, i / max(1, len(komm_nodes) - 1))
        pos[focus_word] = (0.5, 0.5)
        if ctx_nodes:
            for i, node in enumerate(ctx_nodes):
                pos[node] = (1.0, i / max(1, len(ctx_nodes) - 1))

        fig2 = go.Figure()
        max_w = max(1, float(net_edges["weight"].max()))

        for _, row in net_edges.iterrows():
            x0, y0 = pos.get(row["from"], (0.0, 0.0))
            x1, y1 = pos.get(row["to"], (1.0, 1.0))
            fig2.add_trace(
                go.Scatter(
                    x=[x0, x1],
                    y=[y0, y1],
                    mode="lines",
                    line=dict(width=1 + 8 * (float(row["weight"]) / max_w), color="rgba(120,120,120,0.4)"),
                    hoverinfo="text",
                    text=f"{row['from']} -> {row['to']} | weight={int(row['weight'])}",
                    showlegend=False,
                )
            )

        node_names = list(pos.keys())
        node_x = [pos[n][0] for n in node_names]
        node_y = [pos[n][1] for n in node_names]
        node_colors = [
            "#f58518" if n == focus_word else ("#4c78a8" if n in komm_nodes else "#54a24b")
            for n in node_names
        ]

        fig2.add_trace(
            go.Scatter(
                x=node_x,
                y=node_y,
                mode="markers+text",
                text=node_names,
                textposition="middle right",
                marker=dict(size=12, color=node_colors, line=dict(width=0.5, color="#333")),
                showlegend=False,
            )
        )
        fig2.update_layout(
            height=620,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            margin=dict(l=10, r=10, t=20, b=10),
        )
        st.plotly_chart(fig2, use_container_width=True)

with st.expander("What does prefix mode mean?"):
    st.markdown(
        f"""
- `exact token`: token must equal `{focus_word}`.
- `prefix (word*)`: token must start with `{focus_word}`.
- Example for `hem` in prefix mode: `hem`, `hemma`, `hemmet`, `hemifrån`.
- Prefix mode can include unrelated words that share the same start.
"""
    )

