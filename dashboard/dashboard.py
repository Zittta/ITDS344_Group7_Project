"""
Seattle Crime Intelligence Dashboard
ITDS344 Group 7 — Warm Editorial Redesign

Collections (gold DB):
  fact_crime_events          → offense_category, neighborhood, report_datetime, …
  fact_911_calls             → call_datetime, event_type, …
  agg_crime_by_offense_category → offense_category, year, month, crime_count
  agg_crime_per_capita       → neighborhood_name, total_crimes, crime_rate_per_10k
"""

import os
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.graph_objects as go
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from datetime import datetime
from pathlib import Path

# ─── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Seattle Crime Intelligence",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={},   # empty → collapses the ⋮ menu, removes Deploy prompt
)

# ─── Load external CSS ────────────────────────────────────────
css_path = Path(__file__).parent / "assets" / "style.css"
with open(css_path) as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Inject fonts via @import inside <style> (loads synchronously, beats Streamlit render)
# + JS font-ready: force repaint once Playfair Display is actually available
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,600;0,700;1,400;1,700&family=IBM+Plex+Mono:wght@400;500&family=Instrument+Sans:wght@300;400;500&display=swap');

/* Belt-and-suspenders: re-declare on every selector that must use Playfair */
.pg-title,
.pg-title *,
.sb-wordmark,
.sb-wordmark * {
    font-family: 'Playfair Display', Georgia, serif !important;
    font-weight: 700 !important;
}
.kpi-val,
.kpi-val * {
    font-family: 'Playfair Display', Georgia, serif !important;
    font-weight: 400 !important;
}
.sec-title,
.sec-title * {
    font-family: 'Playfair Display', Georgia, serif !important;
    font-weight: 400 !important;
}
.kpi-lbl,
.kpi-hint,
.kpi-lbl *,
.kpi-hint *,
.pg-eyebrow,
.pg-eyebrow *,
.pg-sub,
.pg-sub *,
.pg-time-label,
.pg-time-label *,
.pg-time-value,
.pg-time-value * {
    font-family: 'IBM Plex Mono', 'Courier New', monospace !important;
}
</style>
<script>
// Wait until Playfair Display is loaded, then force a repaint
// so Streamlit doesn't show Georgia fallback on first render
document.fonts.load("700 1em 'Playfair Display'").then(function() {
    // pg-title and sb-wordmark stay bold (700)
    ['.pg-title', '.sb-wordmark'].forEach(function(sel) {
        document.querySelectorAll(sel).forEach(function(el) {
            el.style.fontFamily = "'Playfair Display', Georgia, serif";
            el.style.fontWeight = "700";
        });
    });
    // kpi-val uses regular (400) for elegant editorial look
    document.querySelectorAll('.kpi-val').forEach(function(el) {
        el.style.fontFamily = "'Playfair Display', Georgia, serif";
        el.style.fontWeight = "400";
    });
    // sec-title uses regular (400) for elegant editorial look
    document.querySelectorAll('.sec-title').forEach(function(el) {
        el.style.fontFamily = "'Playfair Display', Georgia, serif";
        el.style.fontWeight = "400";
    });
    var monoSels = ['.kpi-lbl', '.kpi-hint', '.pg-eyebrow', '.pg-sub',
                    '.pg-time-label', '.pg-time-value'];
    monoSels.forEach(function(sel) {
        document.querySelectorAll(sel).forEach(function(el) {
            el.style.fontFamily = "'IBM Plex Mono', 'Courier New', monospace";
        });
    });
});
</script>
""", unsafe_allow_html=True)

# Inject Google Fonts into the parent document <head> via postMessage trick
# This is the only reliable way to get fonts before Streamlit's first paint
components.html("""
<script>
(function() {
    // Send font link to parent window so it gets injected into <head>
    var fonts = [
        "https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,600;0,700;1,400;1,700&family=IBM+Plex+Mono:wght@400;500&family=Instrument+Sans:wght@300;400;500&display=swap"
    ];
    fonts.forEach(function(href) {
        var msg = { type: "streamlit:injectFont", href: href };
        window.parent.postMessage(msg, "*");
        // Also inject directly into parent document if same-origin
        try {
            var link = window.parent.document.createElement("link");
            link.rel = "stylesheet";
            link.href = href;
            window.parent.document.head.appendChild(link);
        } catch(e) {}
    });
})();
</script>
""", height=0)


# ─── Plotly theme ─────────────────────────────────────────────
def _layout_base(**extra):
    return go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Mono, monospace", color="#9e9488", size=11),
        colorway=["#b84c1e", "#2d6a4f", "#3b5268", "#b07d2b", "#7c5c48", "#4a7c6f"],
        xaxis=dict(
            gridcolor="rgba(28,24,20,0.06)",
            linecolor="rgba(28,24,20,0.12)",
            tickcolor="rgba(28,24,20,0.12)",
            zeroline=False,
            tickfont=dict(family="IBM Plex Mono, monospace", size=10, color="#9e9488"),
        ),
        yaxis=dict(
            gridcolor="rgba(28,24,20,0.06)",
            linecolor="rgba(28,24,20,0.12)",
            tickcolor="rgba(28,24,20,0.12)",
            zeroline=False,
            tickfont=dict(family="IBM Plex Mono, monospace", size=10, color="#9e9488"),
        ),
        margin=dict(l=8, r=8, t=8, b=8),
        hoverlabel=dict(
            bgcolor="#ede8de",
            bordercolor="rgba(28,24,20,0.15)",
            font=dict(family="IBM Plex Mono, monospace", size=11, color="#1c1814"),
        ),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
        **extra,
    )


def bar_chart_h(df, label_col, value_col, color="#b84c1e", height=380):
    """Horizontal bar chart — sorted largest first."""
    df_sorted = df.sort_values(value_col, ascending=True).tail(20)
    fig = go.Figure(go.Bar(
        x=df_sorted[value_col],
        y=df_sorted[label_col],
        orientation="h",
        marker=dict(
            color=df_sorted[value_col],
            colorscale=[[0, "#f0c4b0"], [1, color]],
            showscale=False,
            line=dict(width=0),
        ),
        text=df_sorted[value_col].apply(lambda v: f"{v:,}"),
        textposition="outside",
        textfont=dict(size=10, family="IBM Plex Mono, monospace"),
        hovertemplate="<b>%{y}</b><br>%{x:,}<extra></extra>",
    ))
    fig.update_layout(_layout_base(height=height))
    fig.update_xaxes(showgrid=False)
    return fig


def bar_chart_v(df, label_col, value_col, color="#b84c1e", height=380):
    """Vertical bar chart."""
    fig = go.Figure(go.Bar(
        x=df[label_col],
        y=df[value_col],
        marker=dict(color=color, opacity=0.85, line=dict(width=0)),
        hovertemplate="<b>%{x}</b><br>%{y:,}<extra></extra>",
    ))
    fig.update_layout(_layout_base(height=height))
    fig.update_xaxes(tickangle=-30)
    return fig


def donut_chart(df, names_col, values_col, height=380):
    """Donut chart — values_col must be numeric."""
    colors = ["#b84c1e", "#2d6a4f", "#3b5268", "#b07d2b",
              "#7c5c48", "#4a7c6f", "#c47a5a", "#5d7a60"]
    # Ensure numeric — coerce anything that isn't
    df = df.copy()
    df[values_col] = pd.to_numeric(df[values_col], errors="coerce").fillna(0)
    total = int(df[values_col].sum())
    fig = go.Figure(go.Pie(
        labels=df[names_col],
        values=df[values_col],
        hole=0.60,
        marker=dict(colors=colors, line=dict(color="#f5f0e8", width=2)),
        textfont=dict(family="IBM Plex Mono, monospace", size=10),
        hovertemplate="<b>%{label}</b><br>%{value:,} · %{percent}<extra></extra>",
    ))
    fig.update_layout(_layout_base(
        height=height,
        annotations=[dict(
            text=f"<span style='font-family:\"IBM Plex Mono\",monospace;font-size:18px;font-variant-numeric:tabular-nums;letter-spacing:-0.02em'>{total:,}</span><br><span style='font-family:\"IBM Plex Mono\",monospace;font-size:11px;letter-spacing:0.08em'>total</span>",
            x=0.5, y=0.5,
            font=dict(family="IBM Plex Mono, monospace", size=16, color="#1c1814"),
            showarrow=False,
        )],
    ))
    return fig


def area_chart(dates, values, height=300):
    """Area / timeline chart."""
    fig = go.Figure(go.Scatter(
        x=dates, y=values, mode="lines",
        fill="tozeroy",
        fillcolor="rgba(184,76,30,0.08)",
        line=dict(color="#b84c1e", width=1.8),
        hovertemplate="<b>%{x|%b %Y}</b><br>%{y:,} calls<extra></extra>",
    ))
    fig.update_layout(_layout_base(height=height))
    return fig


# ─── MongoDB connection ───────────────────────────────────────
@st.cache_resource(ttl=60)
def connect_mongo():
    """
    Try URIs in order — no auth because docker-compose.yml does NOT
    set MONGO_INITDB_ROOT_USERNAME / PASSWORD.
    """
    candidates = [
        os.environ.get("MONGO_URI", ""),
        "mongodb://mongo:27017",
        "mongodb://localhost:27017",
        "mongodb://127.0.0.1:27017",
    ]
    for uri in candidates:
        if not uri:
            continue
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.server_info()
            return client["gold"], True, uri
        except (ServerSelectionTimeoutError, Exception):
            continue
    return None, False, None


def load(db, collection):
    if db is None:
        return pd.DataFrame()
    try:
        return pd.DataFrame(list(db[collection].find({}, {"_id": 0})))
    except Exception as e:
        st.warning(f"Could not load `{collection}`: {e}")
        return pd.DataFrame()


# ─── Load data & derive display frames ───────────────────────
db, mongo_ok, mongo_uri = connect_mongo()

crime_raw  = load(db, "fact_crime_events")
calls_raw  = load(db, "fact_911_calls")
offense_raw = load(db, "agg_crime_by_offense_category")
capita_raw  = load(db, "agg_crime_per_capita")

# ── offense: group offense_category → sum crime_count ────────
# Schema: {offense_category, year, month, crime_count}
# Each row is one (category × month) — we need totals per category.
if not offense_raw.empty and "offense_category" in offense_raw.columns and "crime_count" in offense_raw.columns:
    offense_raw["crime_count"] = pd.to_numeric(offense_raw["crime_count"], errors="coerce").fillna(0)
    offense_agg = (
        offense_raw.groupby("offense_category", as_index=False)["crime_count"]
        .sum()
        .rename(columns={"offense_category": "Category", "crime_count": "Crimes"})
        .sort_values("Crimes", ascending=False)
    )
else:
    offense_agg = pd.DataFrame()

# ── capita: pick correct columns ─────────────────────────────
# Schema: {neighborhood_name, total_crimes, crime_rate_per_10k, total_population}
if not capita_raw.empty and "neighborhood_name" in capita_raw.columns:
    capita_raw["crime_rate_per_10k"] = pd.to_numeric(capita_raw.get("crime_rate_per_10k", pd.Series(dtype=float)), errors="coerce").fillna(0)
    capita_agg = (
        capita_raw[["neighborhood_name", "crime_rate_per_10k"]]
        .rename(columns={"neighborhood_name": "Neighborhood", "crime_rate_per_10k": "Rate per 10k"})
        .sort_values("Rate per 10k", ascending=False)
    )
else:
    capita_agg = pd.DataFrame()

# ── 911 calls: parse call_datetime for timeline ───────────────
# Schema: {call_datetime, event_type, …}
calls_daily = pd.DataFrame()
if not calls_raw.empty and "call_datetime" in calls_raw.columns:
    calls_view = calls_raw.copy()
    calls_view["_dt"] = pd.to_datetime(calls_view["call_datetime"], errors="coerce")
    calls_daily = (
        calls_view.dropna(subset=["_dt"])
        .groupby(calls_view["_dt"].dt.to_period("M"))
        .size()
        .reset_index(name="count")
    )
    calls_daily["_dt"] = calls_daily["_dt"].dt.to_timestamp()
    calls_daily = calls_daily.sort_values("_dt")

now_str  = datetime.now().strftime("%d %b %Y")
time_str = datetime.now().strftime("%H:%M")


# ─── SIDEBAR ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sb-brand">
        <div class="sb-wordmark" style="font-family:'Playfair Display',Georgia,serif !important;font-weight:700 !important;font-size:22px !important;color:#1c1814 !important;letter-spacing:-0.02em !important;line-height:1.2 !important;">Seattle Crime<br>Intelligence</div>
        <div class="sb-caption">ITDS344 · Group 7 · Data Eng.</div>
    </div>
    """, unsafe_allow_html=True)

    # ── SYSTEM ──────────────────────────────────────────────
    st.markdown('<div class="sb-section"><div class="sb-label">System</div>', unsafe_allow_html=True)
    if mongo_ok:
        st.markdown('<div class="status-pill ok"><div class="pulse"></div>MongoDB · Connected</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="font-family:var(--font-mono);font-size:9px;color:#9e9488;margin-top:6px;padding-left:2px;letter-spacing:0.06em">{mongo_uri}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="status-pill err"><div class="dot-err"></div>MongoDB · Offline</div>', unsafe_allow_html=True)
        st.markdown('<div style="font-family:var(--font-mono);font-size:9px;color:#b84c1e;margin-top:6px;padding-left:2px">Could not reach any host.<br>Check docker-compose is running.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sb-rule"></div>', unsafe_allow_html=True)

    # ── FILTER ──────────────────────────────────────────────
    st.markdown('<div class="sb-section"><div class="sb-label">Filter</div>', unsafe_allow_html=True)
    top_n = st.slider("Top Risk Areas", 5, 20, 10)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="sb-rule"></div>', unsafe_allow_html=True)

    # ── DATA SOURCES ────────────────────────────────────────
    st.markdown('<div class="sb-section"><div class="sb-label">Data Sources</div>', unsafe_allow_html=True)
    for name, interval in [
        ("Seattle Real-Time 911", "5 min"),
        ("SPD Crime Reports",     "60 min"),
        ("ACS Population",        "yearly"),
    ]:
        st.markdown(f"""
        <div class="source-item">
            <div class="source-dot"></div>
            <span>{name}</span>
            <span style="margin-left:auto;opacity:.45;font-size:9px">{interval}</span>
        </div>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ─── MAIN ─────────────────────────────────────────────────────

# Header — full-width hero banner
st.markdown(f"""
<div class="pg-hero">
    <div>
        <div class="pg-eyebrow">Real-time Crime Intelligence Platform</div>
        <div class="pg-title">Seattle <em>Crime</em><br>Dashboard</div>
        <div class="pg-sub">Powered by Apache Kafka · Airflow DAG · MongoDB Gold Layer</div>
    </div>
    <div class="pg-timestamp">
        <div class="pg-time-label">Last refreshed</div>
        <div class="pg-time-value">{now_str}<br>{time_str}</div>
    </div>
</div>
""", unsafe_allow_html=True)


# KPI row
n_categories = offense_agg["Category"].nunique() if not offense_agg.empty else 0
n_areas      = len(capita_agg) if not capita_agg.empty else 0

st.markdown(f"""
<div class="kpi-row">
    <div class="kpi-card c-accent">
        <div class="kpi-rule"></div>
        <div class="kpi-top">
            <span class="kpi-lbl" style="font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:0.18em;text-transform:uppercase;color:#968b7e">Total Crimes</span>
            <div class="kpi-icon-wrap">🔍</div>
        </div>
        <div class="kpi-val" style="font-family:'Playfair Display',Georgia,serif;font-weight:400;font-size:44px;letter-spacing:0;line-height:1;margin-bottom:6px;font-variant-numeric:tabular-nums lining-nums;font-feature-settings:'tnum' 1,'lnum' 1">{len(crime_raw):,}</div>
        <div class="kpi-hint" style="font-family:'IBM Plex Mono',monospace;font-size:9.5px;color:#968b7e;letter-spacing:0.08em">SPD incident records</div>
    </div>
    <div class="kpi-card c-green">
        <div class="kpi-rule"></div>
        <div class="kpi-top">
            <span class="kpi-lbl" style="font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:0.18em;text-transform:uppercase;color:#968b7e">911 Calls</span>
            <div class="kpi-icon-wrap">📞</div>
        </div>
        <div class="kpi-val" style="font-family:'Playfair Display',Georgia,serif;font-weight:400;font-size:44px;letter-spacing:0;line-height:1;margin-bottom:6px;font-variant-numeric:tabular-nums lining-nums;font-feature-settings:'tnum' 1,'lnum' 1">{len(calls_raw):,}</div>
        <div class="kpi-hint" style="font-family:'IBM Plex Mono',monospace;font-size:9.5px;color:#968b7e;letter-spacing:0.08em">Emergency dispatches</div>
    </div>
    <div class="kpi-card c-amber">
        <div class="kpi-rule"></div>
        <div class="kpi-top">
            <span class="kpi-lbl" style="font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:0.18em;text-transform:uppercase;color:#968b7e">Categories</span>
            <div class="kpi-icon-wrap">🏷️</div>
        </div>
        <div class="kpi-val" style="font-family:'Playfair Display',Georgia,serif;font-weight:400;font-size:44px;letter-spacing:0;line-height:1;margin-bottom:6px;font-variant-numeric:tabular-nums lining-nums;font-feature-settings:'tnum' 1,'lnum' 1">{n_categories:,}</div>
        <div class="kpi-hint" style="font-family:'IBM Plex Mono',monospace;font-size:9.5px;color:#968b7e;letter-spacing:0.08em">Offense types tracked</div>
    </div>
    <div class="kpi-card c-slate">
        <div class="kpi-rule"></div>
        <div class="kpi-top">
            <span class="kpi-lbl" style="font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:0.18em;text-transform:uppercase;color:#968b7e">Risk Areas</span>
            <div class="kpi-icon-wrap">📍</div>
        </div>
        <div class="kpi-val" style="font-family:'Playfair Display',Georgia,serif;font-weight:400;font-size:44px;letter-spacing:0;line-height:1;margin-bottom:6px;font-variant-numeric:tabular-nums lining-nums;font-feature-settings:'tnum' 1,'lnum' 1">{n_areas:,}</div>
        <div class="kpi-hint" style="font-family:'IBM Plex Mono',monospace;font-size:9.5px;color:#968b7e;letter-spacing:0.08em">Neighbourhood zones</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Row 1: Crime Categories (horizontal bar) + Highest Risk Areas ──
col_a, col_b = st.columns(2, gap="medium")

with col_a:
    st.markdown('<div class="sec-header"><span class="sec-title">Crime Categories</span><span class="sec-chip">Horizontal Bar</span></div>', unsafe_allow_html=True)
    if offense_agg.empty:
        st.markdown('<div class="empty"><div class="empty-glyph">📊</div><div class="empty-msg">No category data yet</div><div class="empty-hint">Trigger <code>spd_crime_pipeline</code> in Airflow</div></div>', unsafe_allow_html=True)
    else:
        st.plotly_chart(
            bar_chart_h(offense_agg, "Category", "Crimes", color="#b84c1e", height=400),
            use_container_width=True, config={"displayModeBar": False},
        )

with col_b:
    st.markdown(f'<div class="sec-header"><span class="sec-title">Highest Risk Areas <small style="font-size:12px;color:#9e9488">· Top {top_n}</small></span><span class="sec-chip">Per 10k Pop</span></div>', unsafe_allow_html=True)
    if capita_agg.empty:
        st.markdown('<div class="empty"><div class="empty-glyph">📍</div><div class="empty-msg">No area data yet</div><div class="empty-hint">Requires <code>dim_demographics</code> + crime data</div></div>', unsafe_allow_html=True)
    else:
        top_capita = capita_agg.head(top_n)
        st.plotly_chart(
            bar_chart_h(top_capita, "Neighborhood", "Rate per 10k", color="#2d6a4f", height=400),
            use_container_width=True, config={"displayModeBar": False},
        )

st.markdown('<div class="gap-md"></div>', unsafe_allow_html=True)

# ── Row 2: 911 Timeline + Category Mix donut ──────────────────
col_c, col_d = st.columns([1.5, 1], gap="medium")

with col_c:
    st.markdown('<div class="sec-header"><span class="sec-title">911 Call Activity</span><span class="sec-chip">Monthly Timeline</span></div>', unsafe_allow_html=True)
    if calls_daily.empty:
        st.markdown('<div class="empty"><div class="empty-glyph">📡</div><div class="empty-msg">Awaiting streaming data</div><div class="empty-hint">Kafka publishes every <code>5 min</code></div></div>', unsafe_allow_html=True)
    else:
        st.plotly_chart(
            area_chart(calls_daily["_dt"], calls_daily["count"], height=300),
            use_container_width=True, config={"displayModeBar": False},
        )

with col_d:
    st.markdown('<div class="sec-header"><span class="sec-title">Category Mix</span><span class="sec-chip">Donut</span></div>', unsafe_allow_html=True)
    if offense_agg.empty:
        st.markdown('<div class="empty"><div class="empty-glyph">🥧</div><div class="empty-msg">No distribution data</div><div class="empty-hint">Run pipeline first</div></div>', unsafe_allow_html=True)
    else:
        # Top 8 + "Other" bucket so donut is readable
        top8 = offense_agg.head(8).copy()
        rest = offense_agg.iloc[8:]
        if not rest.empty:
            other_row = pd.DataFrame([{"Category": "Other", "Crimes": rest["Crimes"].sum()}])
            top8 = pd.concat([top8, other_row], ignore_index=True)
        st.plotly_chart(
            donut_chart(top8, "Category", "Crimes", height=300),
            use_container_width=True, config={"displayModeBar": False},
        )

st.markdown('<div class="gap-md"></div>', unsafe_allow_html=True)

# ── Latest Incident Records ───────────────────────────────────
st.markdown('<div class="sec-header"><span class="sec-title">Latest Incident Records</span><span class="sec-chip">Live · Top 50</span></div>', unsafe_allow_html=True)
if crime_raw.empty:
    st.markdown('<div class="empty"><div class="empty-glyph">🗄️</div><div class="empty-msg">Pipeline not yet loaded</div><div class="empty-hint">Trigger: <code>airflow dags trigger spd_crime_pipeline</code> or open Airflow at <code>localhost:8080</code></div></div>', unsafe_allow_html=True)
else:
    # Show clean columns only
    display_cols = [c for c in ["offense_id", "report_datetime", "offense_category",
                                "offense_sub_category", "neighborhood", "is_shooting"]
                   if c in crime_raw.columns]
    st.dataframe(
        crime_raw[display_cols].head(50) if display_cols else crime_raw.head(50),
        use_container_width=True, height=340, hide_index=True,
    )

# Footer
st.markdown(f"""
<div class="pg-footer">
    <span class="footer-l">ITDS344 · Group 7 · Data Engineering</span>
    <span class="footer-r">Streamlit · Kafka · Airflow · MongoDB · {now_str}</span>
</div>
""", unsafe_allow_html=True)