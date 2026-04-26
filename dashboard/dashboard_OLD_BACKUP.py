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

# Initialize session state for performance optimization
if 'initialized' not in st.session_state:
    st.session_state.initialized = True
    st.session_state.last_top_n = 10

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
    # Optimize text generation - convert to list comprehension instead of apply
    text_values = [f"{int(v):,}" for v in df_sorted[value_col]]
    
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
        text=text_values,
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
@st.cache_resource(ttl=300)
def connect_mongo():
    """
    Try URIs in order — no auth because docker-compose.yml does NOT
    set MONGO_INITDB_ROOT_USERNAME / PASSWORD.
    Cached for 5 minutes to reuse same connection.
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
            client = MongoClient(uri, serverSelectionTimeoutMS=3000, maxPoolSize=10)
            client.server_info()
            return client, True, uri
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


def load_geo(db, collection, lat_col="latitude", lon_col="longitude", limit=4000):
    """Load lat/lon only for heatmap — fast projection, capped at `limit` rows."""
    if db is None:
        return pd.DataFrame()
    try:
        # Use aggregation pipeline for better performance
        pipeline = [
            {"$match": {
                lat_col: {"$exists": True, "$ne": None},
                lon_col: {"$exists": True, "$ne": None}
            }},
            {"$project": {"_id": 0, lat_col: 1, lon_col: 1}},
            {"$limit": limit}
        ]
        df = pd.DataFrame(list(db[collection].aggregate(pipeline)))
        if df.empty:
            return df
        df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
        df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
        # Keep only valid Seattle-area coords
        df = df.dropna(subset=[lat_col, lon_col])
        df = df[(df[lat_col].between(47.0, 48.2)) & (df[lon_col].between(-122.8, -121.8))]
        return df
    except Exception as e:
        st.warning(f"Could not load geo data from `{collection}`: {e}")
        return pd.DataFrame()


def heatmap_mapbox(df, lat_col="latitude", lon_col="longitude", weight_col=None, height=520):
    """Density heatmap on OpenStreetMap tiles — optimized for performance."""
    if df.empty:
        return go.Figure()
    
    # Prepare data efficiently
    z = df[weight_col].values if weight_col and weight_col in df.columns else None
    
    # Build customdata only if we have the columns - avoid unnecessary array operations
    customdata = None
    hovertemplate = None
    if weight_col and weight_col in df.columns and "neighborhood" in df.columns:
        # Use list comprehension instead of .values for better memory efficiency
        customdata = [[str(n), int(c)] for n, c in zip(df["neighborhood"], df[weight_col])]
        hovertemplate = "<b>%{customdata[0]}</b><br>%{customdata[1]:,} crimes<extra></extra>"
    
    fig = go.Figure(go.Densitymapbox(
        lat=df[lat_col].values,
        lon=df[lon_col].values,
        z=z,
        radius=16,  # Slightly smaller radius for better performance
        colorscale=[
            [0.0,  "rgba(253,231,159,0)"],
            [0.2,  "rgba(253,180,98,0.55)"],
            [0.5,  "rgba(220,60,40,0.80)"],
            [0.8,  "rgba(150,0,30,0.90)"],
            [1.0,  "rgba(80,0,20,1.0)"],
        ],
        showscale=False,
        hovertemplate=hovertemplate,
        customdata=customdata,
    ))
    fig.update_layout(
        mapbox_style="open-street-map",
        mapbox=dict(center=dict(lat=47.6062, lon=-122.3321), zoom=11),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=0, b=0),
        height=height,
        uirevision='constant',  # Preserve zoom/pan state on updates
    )
    return fig


def load_crime_geo(db, limit=500):
    """
    Join gold.fact_crime_events (location_id counts) with gold.dim_location (lat/lon).
    Returns top N locations by crime count — optimized for heatmap performance.
    """
    if db is None:
        return pd.DataFrame()
    try:
        # Step 1: count crimes per location_id and get top N hotspots only
        pipeline = [
            {"$match": {"location_id": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$location_id", "crime_count": {"$sum": 1}}},
            {"$sort": {"crime_count": -1}},  # Sort by crime count descending
            {"$limit": limit}  # Only get top N locations
        ]
        loc_counts = pd.DataFrame(list(db["fact_crime_events"].aggregate(pipeline)))
        if loc_counts.empty:
            return pd.DataFrame()
        loc_counts = loc_counts.rename(columns={"_id": "location_id"})

        # Step 2: pull lat/lon only for these top locations (using $in query)
        location_ids = loc_counts["location_id"].tolist()
        dim_loc = pd.DataFrame(list(db["dim_location"].find(
            {
                "location_id": {"$in": location_ids},
                "latitude": {"$ne": None}, 
                "longitude": {"$ne": None}
            },
            {"_id": 0, "location_id": 1, "latitude": 1, "longitude": 1, "neighborhood": 1},
        )))
        if dim_loc.empty:
            return pd.DataFrame()

        # Step 3: join
        merged = loc_counts.merge(dim_loc, on="location_id", how="inner")
        merged["latitude"]  = pd.to_numeric(merged["latitude"],  errors="coerce")
        merged["longitude"] = pd.to_numeric(merged["longitude"], errors="coerce")
        merged = merged.dropna(subset=["latitude", "longitude"])
        merged = merged[
            merged["latitude"].between(47.0, 48.2) &
            merged["longitude"].between(-122.8, -121.8)
        ]
        return merged
    except Exception as e:
        st.warning(f"Could not build crime geo: {e}")
        return pd.DataFrame()


# ─── Cached data loaders (TTL 5 min — avoids reload on every slider move) ────
@st.cache_data(ttl=300)
def _get_crime_raw(db_available):
    if not db_available:
        return pd.DataFrame()
    c, ok, uri = connect_mongo()
    db = c["gold"] if c else None
    return load(db, "fact_crime_events")

@st.cache_data(ttl=300)
def _get_calls_raw(db_available):
    if not db_available:
        return pd.DataFrame()
    c, ok, uri = connect_mongo()
    db = c["gold"] if c else None
    return load(db, "fact_911_calls")

@st.cache_data(ttl=300)
def _get_offense_raw(db_available):
    if not db_available:
        return pd.DataFrame()
    c, ok, uri = connect_mongo()
    db = c["gold"] if c else None
    return load(db, "agg_crime_by_offense_category")

@st.cache_data(ttl=300)
def _get_capita_raw(db_available):
    if not db_available:
        return pd.DataFrame()
    c, ok, uri = connect_mongo()
    db = c["gold"] if c else None
    return load(db, "agg_crime_per_capita")

@st.cache_data(ttl=300)
def _get_crime_geo(db_available):
    if not db_available:
        return pd.DataFrame()
    c, ok, uri = connect_mongo()
    db = c["gold"] if c else None
    # Limit to top 500 crime hotspots for heatmap performance
    return load_crime_geo(db, limit=500)

@st.cache_data(ttl=300)
def _get_calls_geo(db_available):
    if not db_available:
        return pd.DataFrame()
    c, ok, uri = connect_mongo()
    db = c["gold"] if c else None
    return load_geo(db, "fact_911_calls", limit=4000)


# ─── Load data & derive display frames ───────────────────────
# Show loading indicator while connecting and fetching data
with st.spinner('🔄 Connecting to MongoDB and loading data...'):
    _client, mongo_ok, mongo_uri = connect_mongo()

    crime_raw   = _get_crime_raw(mongo_ok)
    calls_raw   = _get_calls_raw(mongo_ok)
    offense_raw = _get_offense_raw(mongo_ok)
    capita_raw  = _get_capita_raw(mongo_ok)

    # ── Geo data for heatmaps (DISABLED FOR TESTING) ─────────────────────────────────────
    # crime_geo = _get_crime_geo(mongo_ok)
    # calls_geo = _get_calls_geo(mongo_ok)
    crime_geo = pd.DataFrame()  # Empty for testing
    calls_geo = pd.DataFrame()  # Empty for testing

# ── Cache aggregated data to avoid reprocessing on every interaction ──
@st.cache_data(ttl=300)
def _process_offense_data(offense_df):
    """Process offense category aggregations."""
    if offense_df.empty or "offense_category" not in offense_df.columns or "crime_count" not in offense_df.columns:
        return pd.DataFrame()
    
    df = offense_df.copy()
    df["crime_count"] = pd.to_numeric(df["crime_count"], errors="coerce").fillna(0)
    return (
        df.groupby("offense_category", as_index=False)["crime_count"]
        .sum()
        .rename(columns={"offense_category": "Category", "crime_count": "Crimes"})
        .sort_values("Crimes", ascending=False)
    )

@st.cache_data(ttl=300)
def _process_capita_data(capita_df):
    """Process capita rate data."""
    if capita_df.empty or "neighborhood_name" not in capita_df.columns:
        return pd.DataFrame()
    
    df = capita_df.copy()
    df["crime_rate_per_10k"] = pd.to_numeric(df.get("crime_rate_per_10k", pd.Series(dtype=float)), errors="coerce").fillna(0)
    return (
        df[["neighborhood_name", "crime_rate_per_10k"]]
        .rename(columns={"neighborhood_name": "Neighborhood", "crime_rate_per_10k": "Rate per 10k"})
        .sort_values("Rate per 10k", ascending=False)
    )

@st.cache_data(ttl=300)
def _process_calls_timeline(calls_df):
    """Process 911 calls timeline data."""
    if calls_df.empty or "call_datetime" not in calls_df.columns:
        return pd.DataFrame()
    
    df = calls_df.copy()
    df["_dt"] = pd.to_datetime(df["call_datetime"], errors="coerce")
    daily = (
        df.dropna(subset=["_dt"])
        .groupby(df["_dt"].dt.to_period("M"))
        .size()
        .reset_index(name="count")
    )
    daily["_dt"] = daily["_dt"].dt.to_timestamp()
    return daily.sort_values("_dt")

# ── offense: group offense_category → sum crime_count ────────
offense_agg = _process_offense_data(offense_raw)

# ── capita: pick correct columns ─────────────────────────────
capita_agg = _process_capita_data(capita_raw)

# ── 911 calls: parse call_datetime for timeline ───────────────
calls_daily = _process_calls_timeline(calls_raw)

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
    top_n = st.slider("Top Risk Areas", 5, 20, 10, key="top_n_slider")
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


# TESTING: Heatmap section temporarily disabled to test dashboard performance without it
st.info("🗺️ Heatmap temporarily disabled for performance testing")
st.markdown('<div class="gap-md"></div>', unsafe_allow_html=True)

# ── Row 1: Crime Categories (horizontal bar) + Highest Risk Areas ──
col_a, col_b = st.columns(2, gap="medium")

with col_a:
    st.markdown('<div class="sec-header"><span class="sec-title">Crime Categories</span><span class="sec-chip">Horizontal Bar</span></div>', unsafe_allow_html=True)
    if offense_agg.empty:
        st.markdown('<div class="empty"><div class="empty-glyph">📊</div><div class="empty-msg">No category data yet</div><div class="empty-hint">Trigger <code>spd_crime_pipeline</code> in Airflow</div></div>', unsafe_allow_html=True)
    else:
        st.plotly_chart(
            bar_chart_h(offense_agg, "Category", "Crimes", color="#b84c1e", height=400),
            use_container_width=True, 
            config={"displayModeBar": False},
            key="crime_categories_chart"
        )

with col_b:
    st.markdown(f'<div class="sec-header"><span class="sec-title">Highest Risk Areas <small style="font-size:12px;color:#9e9488">· Top {top_n}</small></span><span class="sec-chip">Per 10k Pop</span></div>', unsafe_allow_html=True)
    if capita_agg.empty:
        st.markdown('<div class="empty"><div class="empty-glyph">📍</div><div class="empty-msg">No area data yet</div><div class="empty-hint">Requires <code>dim_demographics</code> + crime data</div></div>', unsafe_allow_html=True)
    else:
        top_capita = capita_agg.head(top_n)
        st.plotly_chart(
            bar_chart_h(top_capita, "Neighborhood", "Rate per 10k", color="#2d6a4f", height=400),
            use_container_width=True, 
            config={"displayModeBar": False},
            key="risk_areas_chart"
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
            use_container_width=True, 
            config={"displayModeBar": False},
            key="calls_timeline_chart"
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
            use_container_width=True, 
            config={"displayModeBar": False},
            key="category_donut_chart"
        )

st.markdown('<div class="gap-md"></div>', unsafe_allow_html=True)

# ── Latest Incident Records ───────────────────────────────────
st.markdown('<div class="sec-header"><span class="sec-title">Latest Incident Records</span><span class="sec-chip">Live · Top 50</span></div>', unsafe_allow_html=True)
if crime_raw.empty:
    st.markdown('<div class="empty"><div class="empty-glyph">🗄️</div><div class="empty-msg">Pipeline not yet loaded</div><div class="empty-hint">Trigger: <code>airflow dags trigger spd_crime_pipeline</code> or open Airflow at <code>localhost:8080</code></div></div>', unsafe_allow_html=True)
else:
    # Show clean columns only - limit to 50 rows for performance
    display_cols = [c for c in ["offense_id", "report_datetime", "offense_category",
                                "offense_sub_category", "neighborhood", "is_shooting"]
                   if c in crime_raw.columns]
    
    if display_cols:
        display_data = crime_raw[display_cols].head(50)
    else:
        # Fallback: show first 5 columns if predefined columns don't exist
        display_data = crime_raw.iloc[:50, :5]
    
    st.dataframe(
        display_data,
        use_container_width=True, 
        height=340, 
        hide_index=True,
    )

# Footer
st.markdown(f"""
<div class="pg-footer">
    <span class="footer-l">ITDS344 · Group 7 · Data Engineering</span>
    <span class="footer-r">Streamlit · Kafka · Airflow · MongoDB · {now_str}</span>
</div>
""", unsafe_allow_html=True)

# Add spacing at the bottom to ensure content is scrollable
st.markdown('<div style="height:50px;"></div>', unsafe_allow_html=True)

# Debug: Add visual marker to test scrolling
st.markdown("""
<div style="text-align:center;padding:20px;background:rgba(184,76,30,0.05);border-radius:8px;margin-top:20px;">
    <p style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:#968b7e;margin:0;">
        ✅ If you can see this message, page scrolling is working properly
    </p>
</div>
""", unsafe_allow_html=True)