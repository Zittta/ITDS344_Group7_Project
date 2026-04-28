"""
Seattle Public Safety Intelligence Dashboard
ITDS344 Group 7 — Gold Layer Analytics
"""

import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime

st.set_page_config(
    page_title="Seattle Public Safety Intelligence",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* Allow the page to scroll naturally */
    html { overflow: auto !important; }
    body { overflow: auto !important; height: auto !important; }
    /* Custom scrollbar styling */
    ::-webkit-scrollbar { width: 10px; }
    ::-webkit-scrollbar-track { background: #f0f0f0; }
    ::-webkit-scrollbar-thumb { background: #888; border-radius: 5px; }
    .stDataFrame { max-height: 400px; overflow: auto; }
</style>
""", unsafe_allow_html=True)


# ─── MongoDB ──────────────────────────────────────────────────────────────────

@st.cache_resource(ttl=300)
def connect_mongo():
    for uri in [os.environ.get("MONGO_URI", ""), "mongodb://mongo:27017", "mongodb://localhost:27017"]:
        if not uri:
            continue
        try:
            c = MongoClient(uri, serverSelectionTimeoutMS=3000, maxPoolSize=10)
            c.server_info()
            return c, True, uri
        except:
            continue
    return None, False, None


@st.cache_data(ttl=300)
def load_gold(collection: str, query: dict = None, projection: dict = None, limit: int = 0):
    client, ok, _ = connect_mongo()
    if not ok:
        return pd.DataFrame()
    try:
        coll = client["gold"][collection]
        proj = projection or {"_id": 0}
        cur  = coll.find(query or {}, proj)
        if limit:
            cur = cur.limit(limit)
        return pd.DataFrame(list(cur))
    except Exception as e:
        st.warning(f"Cannot load {collection}: {e}")
        return pd.DataFrame()


# ─── Chart helpers ────────────────────────────────────────────────────────────

def bar_chart(df, x, y, title="", color="#b84c1e", horizontal=False):
    if df.empty:
        return go.Figure()
    if horizontal:
        fig = go.Figure(go.Bar(y=df[x], x=df[y], orientation="h", marker_color=color))
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
    else:
        fig = go.Figure(go.Bar(x=df[x], y=df[y], marker_color=color))
    fig.update_layout(
        title=title, height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ─── Map helpers (from reference dashboard) ───────────────────────────────────

@st.cache_data(ttl=300)
def load_crime_by_neighborhood():
    """Aggregate crimes by neighborhood with centroid coordinates from dim_location."""
    client, ok, _ = connect_mongo()
    if not ok:
        return pd.DataFrame()
    try:
        db = client["gold"]
        pipeline = [
            {"$match": {"neighborhood": {"$exists": True, "$ne": None},
                        "location_id":  {"$exists": True, "$ne": None}}},
            {"$group": {"_id": {"neighborhood": "$neighborhood",
                                "location_id":  "$location_id"},
                        "crime_count": {"$sum": 1}}},
            {"$group": {"_id":             "$_id.neighborhood",
                        "crime_count":     {"$sum": "$crime_count"},
                        "sample_location": {"$first": "$_id.location_id"}}},
            {"$sort": {"crime_count": -1}},
        ]
        nbhd = pd.DataFrame(list(db["fact_crime_events"].aggregate(pipeline)))
        if nbhd.empty:
            return pd.DataFrame()
        nbhd = nbhd.rename(columns={"_id": "neighborhood"})
        locs = pd.DataFrame(list(db["dim_location"].find(
            {"location_id": {"$in": nbhd["sample_location"].tolist()}},
            {"_id": 0, "location_id": 1, "latitude": 1, "longitude": 1},
        )))
        if locs.empty:
            return pd.DataFrame()
        result = nbhd.merge(locs, left_on="sample_location", right_on="location_id", how="inner")
        result["latitude"]  = pd.to_numeric(result["latitude"],  errors="coerce")
        result["longitude"] = pd.to_numeric(result["longitude"], errors="coerce")
        result = result.dropna(subset=["latitude", "longitude"])
        result = result[result["latitude"].between(47.0, 48.2) & result["longitude"].between(-122.8, -121.8)]
        return result[["neighborhood", "crime_count", "latitude", "longitude"]]
    except Exception as e:
        st.warning(f"Could not load neighborhood crime data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_calls_geo(limit: int = 3000):
    """Load sample 911 call lat/lon for density heatmap layer."""
    client, ok, _ = connect_mongo()
    if not ok:
        return pd.DataFrame()
    try:
        db = client["gold"]
        pipeline = [
            {"$match": {"latitude":  {"$exists": True, "$ne": None},
                        "longitude": {"$exists": True, "$ne": None}}},
            {"$project": {"_id": 0, "latitude": 1, "longitude": 1}},
            {"$limit": limit},
        ]
        df = pd.DataFrame(list(db["fact_911_calls"].aggregate(pipeline)))
        if df.empty:
            return df
        df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df = df.dropna(subset=["latitude", "longitude"])
        df = df[df["latitude"].between(47.0, 48.2) & df["longitude"].between(-122.8, -121.8)]
        return df
    except Exception as e:
        st.warning(f"Could not load 911 geo: {e}")
        return pd.DataFrame()


def create_layered_map(crime_df, calls_df, show_crime=True, show_calls=True, height=520):
    """2-layer map: 911 density heatmap (background) + crime neighborhood bubbles (foreground)."""
    fig = go.Figure()
    if show_calls and not calls_df.empty:
        fig.add_trace(go.Densitymapbox(
            lat=calls_df["latitude"].values,
            lon=calls_df["longitude"].values,
            radius=16,
            colorscale=[
                [0.0, "rgba(100,150,255,0)"],
                [0.2, "rgba(100,150,255,0.4)"],
                [0.5, "rgba(50,100,220,0.6)"],
                [0.8, "rgba(20,50,180,0.75)"],
                [1.0, "rgba(10,20,120,0.85)"],
            ],
            showscale=False, name="911 Calls", hoverinfo="skip",
        ))
    if show_crime and not crime_df.empty:
        fig.add_trace(go.Scattermapbox(
            lat=crime_df["latitude"], lon=crime_df["longitude"],
            mode="markers",
            marker=dict(
                size=crime_df["crime_count"] / crime_df["crime_count"].max() * 50 + 10,
                color=crime_df["crime_count"],
                colorscale=[[0.0,"#fde725"],[0.2,"#fca636"],[0.4,"#e16462"],
                            [0.6,"#b12a90"],[0.8,"#6a00a8"],[1.0,"#0d0887"]],
                opacity=0.85, showscale=show_crime, sizemode="diameter",
                colorbar=dict(title="Crime<br>Count", thickness=15, len=0.7, x=1.02),
            ),
            text=crime_df.apply(
                lambda r: f"<b>{r.get('neighborhood','Unknown')}</b><br>{int(r['crime_count']):,} crimes",
                axis=1),
            hovertemplate="%{text}<extra></extra>",
            name="Crime by Neighborhood",
        ))
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox=dict(center=dict(lat=47.6062, lon=-122.3321), zoom=10.5),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=0, b=0), height=height, showlegend=False,
    )
    return fig


client, mongo_ok, mongo_uri = connect_mongo()

with st.spinner("Loading gold layer data…"):
    # Core fact tables
    crime_raw   = load_gold("fact_crime_events", limit=500_000)
    calls_raw   = load_gold("fact_911_calls",    limit=500_000)
    dim_offense = load_gold("dim_offense")

    # Aggregations — existing
    offense_agg  = load_gold("agg_crime_by_offense_category")
    heatmap_data = load_gold("agg_911_by_hour_day")
    capita_raw   = load_gold("agg_crime_per_capita")

    # NEW collections
    safety_profile         = load_gold("agg_neighborhood_safety_profile")
    trend_monthly          = load_gold("agg_crime_trend_monthly")
    dim_nbhd               = load_gold("dim_neighborhood")
    crime_by_neighborhood  = load_crime_by_neighborhood()
    calls_geo              = load_calls_geo(limit=3000)

# Filter out administrative/unmapped neighborhoods with zero data
if not safety_profile.empty:
    safety_profile = safety_profile[
        safety_profile["total_crimes"].fillna(0).gt(0) |
        safety_profile["total_911_calls"].fillna(0).gt(0)
    ].reset_index(drop=True)


# ─── SIDEBAR ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🚔 Seattle Safety Intelligence")
    st.caption("ITDS344 · Group 7 · Data Engineering")
    st.divider()

    st.subheader("System Status")
    if mongo_ok:
        st.success("✅ MongoDB Connected")
        st.caption(mongo_uri)
    else:
        st.error("❌ MongoDB Offline")

    st.divider()
    st.subheader("Data Sources")
    st.caption("🔴 Seattle Real-Time 911 (Socrata API)")
    st.caption("🔴 SPD Crime Reports (Socrata API)")
    st.caption("🔴 ACS Population (Census CSV)")

    st.divider()
    st.subheader("Gold Collections")
    gold_cols = [
        "fact_crime_events", "fact_911_calls",
        "dim_location", "dim_offense", "dim_neighborhood", "dim_demographics",
        "agg_crime_by_offense_category", "agg_crime_per_capita",
        "agg_crime_trend_monthly", "agg_911_per_capita",
        "agg_neighborhood_safety_profile", "agg_911_by_hour_day",
    ]
    if mongo_ok:
        for col in gold_cols:
            try:
                n = client["gold"][col].count_documents({})
                st.caption(f"📦 {col}: {n:,}")
            except:
                st.caption(f"📦 {col}: –")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
st.title("Seattle Public Safety Intelligence Dashboard")
st.caption(f"Real-time Analytics Platform · Refreshed: {datetime.now().strftime('%d %b %Y %H:%M')}")
st.divider()

# ════════════════════════════════════════════════════════════
# SECTION 1: Time-filtered KPI Summary
# ════════════════════════════════════════════════════════════
st.subheader("📊 Summary Dashboard")

col_period, col_date = st.columns([2, 3])
with col_period:
    time_period = st.selectbox("Time Period",
        ["Today", "Select Day", "Select Month", "Select Year", "All Time"],
        key="dash_period")

selected_date = selected_month = selected_year = None
with col_date:
    if time_period == "Select Day":
        selected_date = st.date_input("Pick a date", value=pd.Timestamp.now().date())
    elif time_period == "Select Month":
        c1, c2 = st.columns(2)
        with c1: selected_month = st.selectbox("Month", range(1, 13),
            index=pd.Timestamp.now().month - 1,
            format_func=lambda x: pd.Timestamp(2024, x, 1).strftime("%B"))
        with c2: selected_year = st.number_input("Year", 2020, 2026, 2024, key="my_year")
    elif time_period == "Select Year":
        selected_year = st.number_input("Year", 2020, 2026, 2024, key="sy_year")


def _filter_df(df, dt_col):
    if df.empty or dt_col not in df.columns:
        return df
    d = df.copy()
    d["_dt"] = pd.to_datetime(d[dt_col], errors="coerce").dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")
    if time_period == "Today":
        today = pd.Timestamp.now(tz="UTC").date()
        d = d[d["_dt"].dt.date == today]
    elif time_period == "Select Day" and selected_date:
        d = d[d["_dt"].dt.date == selected_date]
    elif time_period == "Select Month" and selected_month and selected_year:
        d = d[(d["_dt"].dt.year == selected_year) & (d["_dt"].dt.month == selected_month)]
    elif time_period == "Select Year" and selected_year:
        d = d[d["_dt"].dt.year == selected_year]
    return d


crime_f = _filter_df(crime_raw, "report_date_time")
calls_f = _filter_df(calls_raw, "call_datetime")

period_labels = {
    "Today": f"Today ({pd.Timestamp.now().date().strftime('%B %d, %Y')})",
    "Select Day": selected_date.strftime("%B %d, %Y") if selected_date else "–",
    "Select Month": f"{pd.Timestamp(selected_year or 2024, selected_month or 1, 1).strftime('%B %Y')}" if selected_month else "–",
    "Select Year": str(selected_year) if selected_year else "–",
    "All Time": "All Time",
}
st.caption(f"📅 Showing: **{period_labels.get(time_period, 'All Time')}**")

# KPI row
k1, k2, k3, k4 = st.columns(4)
crime_count    = len(crime_f)
calls_count    = len(calls_f)
shooting_count = int(crime_f["is_shooting"].sum()) if "is_shooting" in crime_f.columns else 0
nbhd_count     = len(safety_profile) if not safety_profile.empty else 0

k1.metric("🔪 Total Crimes",       f"{crime_count:,}")
k2.metric("📞 911 Calls",          f"{calls_count:,}")
k3.metric("🔫 Shooting Incidents", f"{shooting_count:,}")
k4.metric("🏘️ Neighborhoods Covered", f"{nbhd_count:,}")

# Detailed records expander
with st.expander("📋 View Detailed Records"):
    tab1, tab2 = st.tabs(["Crime Records", "911 Call Records"])
    with tab1:
        if not crime_f.empty:
            cols = [c for c in ["offense_id", "report_date_time", "offense_category",
                                 "neighborhood", "is_shooting"] if c in crime_f.columns]
            st.dataframe(crime_f[cols].tail(100), use_container_width=True, height=300, hide_index=True)
        else:
            st.info("No crime records for selected period")
    with tab2:
        if not calls_f.empty:
            cols = [c for c in ["event_id", "call_datetime", "event_type",
                                 "address", "neighborhood_name"] if c in calls_f.columns]
            st.dataframe(calls_f[cols].tail(100), use_container_width=True, height=300, hide_index=True)
        else:
            st.info("No 911 call records for selected period")

st.divider()

# ════════════════════════════════════════════════════════════
# SECTION 2: Neighborhood Safety Profile (cross-dataset)
# ════════════════════════════════════════════════════════════
st.subheader("🏘️ Neighborhood Safety Profile")
st.caption("รวมข้อมูล Crime + 911 + Population per Neighborhood — ใช้ประเมินความเสี่ยงแต่ละพื้นที่")

if not safety_profile.empty:
    top_n = st.slider("จำนวน Neighborhoods", 5, 30, 15, key="profile_n")

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**🔴 Crime Rate per 10,000 Population**")
        if "crime_rate_per_10k" in safety_profile.columns:
            top_crime = (safety_profile
                .dropna(subset=["crime_rate_per_10k"])
                .nlargest(top_n, "crime_rate_per_10k")
                [["neighborhood_name", "crime_rate_per_10k"]])
            st.plotly_chart(
                bar_chart(top_crime, "neighborhood_name", "crime_rate_per_10k",
                          color="#e74c3c", horizontal=True),
                use_container_width=True, key="crime_rate_chart"
            )

    with col_b:
        st.markdown("**📞 911 Call Rate per 10,000 Population**")
        if "calls_911_rate_per_10k" in safety_profile.columns:
            top_calls = (safety_profile
                .dropna(subset=["calls_911_rate_per_10k"])
                .nlargest(top_n, "calls_911_rate_per_10k")
                [["neighborhood_name", "calls_911_rate_per_10k"]])
            st.plotly_chart(
                bar_chart(top_calls, "neighborhood_name", "calls_911_rate_per_10k",
                          color="#3498db", horizontal=True),
                use_container_width=True, key="calls_rate_chart"
            )

    # Scatter: Crime Rate vs Poverty
    st.markdown("**📊 Crime Rate vs Poverty % (Demographic Correlation)**")
    scatter_cols = ["neighborhood_name", "crime_rate_per_10k", "poverty_pct",
                    "total_population", "median_household_income"]
    scatter_df = safety_profile.dropna(subset=["crime_rate_per_10k", "poverty_pct"])
    if not scatter_df.empty:
        fig_scatter = px.scatter(
            scatter_df,
            x="poverty_pct", y="crime_rate_per_10k",
            size="total_population", color="crime_rate_per_10k",
            hover_name="neighborhood_name",
            hover_data={"median_household_income": True, "total_population": ":,"},
            color_continuous_scale="YlOrRd",
            labels={"poverty_pct": "Poverty % (ACS)", "crime_rate_per_10k": "Crime Rate per 10K"},
            title="Crime Rate vs Poverty Rate by Neighborhood (bubble size = population)",
            height=450,
        )
        fig_scatter.update_layout(paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_scatter, use_container_width=True, key="scatter_poverty")

    # Safety profile data table
    with st.expander("📋 Full Neighborhood Safety Profile Table"):
        display_cols = [c for c in [
            "neighborhood_name", "total_population", "total_crimes",
            "shooting_incidents", "total_911_calls",
            "crime_rate_per_10k", "calls_911_rate_per_10k",
            "shooting_rate_per_10k", "poverty_pct", "median_age",
            "median_household_income",
        ] if c in safety_profile.columns]
        st.dataframe(
            safety_profile[display_cols].sort_values("crime_rate_per_10k", ascending=False),
            use_container_width=True, height=400, hide_index=True
        )
else:
    st.info("ยังไม่มีข้อมูล agg_neighborhood_safety_profile — รัน spd_crime_pipeline ก่อน")

st.divider()

# ════════════════════════════════════════════════════════════
# SECTION 4: Crime Trend Monthly (by Neighborhood)
# ════════════════════════════════════════════════════════════
st.subheader("📈 Crime Trend by Neighborhood & Category")
st.caption("ใช้วิเคราะห์แนวโน้มปัญหาในแต่ละพื้นที่ตามช่วงเวลา")

if not trend_monthly.empty:
    col_t1, col_t2 = st.columns(2)

    available_nbhd = sorted(trend_monthly["neighborhood"].dropna().unique().tolist()) \
        if "neighborhood" in trend_monthly.columns else []
    available_cat  = sorted(trend_monthly["offense_category"].dropna().unique().tolist()) \
        if "offense_category" in trend_monthly.columns else []

    with col_t1:
        sel_nbhd = st.multiselect("เลือก Neighborhood",
            available_nbhd, default=available_nbhd[:3] if len(available_nbhd) >= 3 else available_nbhd,
            key="trend_nbhd")
    with col_t2:
        sel_cat = st.multiselect("เลือก Offense Category",
            available_cat, default=available_cat[:2] if len(available_cat) >= 2 else available_cat,
            key="trend_cat")

    trend_f = trend_monthly.copy()
    if sel_nbhd:
        trend_f = trend_f[trend_f["neighborhood"].isin(sel_nbhd)]
    if sel_cat:
        trend_f = trend_f[trend_f["offense_category"].isin(sel_cat)]

    if not trend_f.empty and "year" in trend_f.columns and "month" in trend_f.columns:
        trend_f["period"] = trend_f.apply(
            lambda r: f"{int(r.get('year', 2024))}-{int(r.get('month', 1)):02d}", axis=1)
        trend_agg = (trend_f.groupby(["period", "neighborhood"], as_index=False)["crime_count"].sum()
                     .sort_values("period"))
        fig_trend = px.line(trend_agg, x="period", y="crime_count", color="neighborhood",
                            title="Monthly Crime Count by Neighborhood",
                            labels={"period": "Month", "crime_count": "Crime Count"},
                            height=400)
        fig_trend.update_layout(paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_trend, use_container_width=True, key="trend_line")
    else:
        st.info("กรุณาเลือก Neighborhood และ Category")
else:
    st.info("ยังไม่มีข้อมูล agg_crime_trend_monthly — รัน spd_crime_pipeline ก่อน")

st.divider()


# ════════════════════════════════════════════════════════════
# SECTION 3: Seattle Crime Map (layered)
# ════════════════════════════════════════════════════════════
st.subheader("🗺️ Seattle Crime Map")

# Layer toggles
col_t1, col_t2 = st.columns(2)
with col_t1:
    show_crime = st.checkbox("🔴 Crime by Neighborhood",  value=True, key="crime_layer_toggle")
with col_t2:
    show_calls = st.checkbox("📞 911 Calls Density", value=True, key="calls_layer_toggle")

if crime_by_neighborhood.empty and calls_geo.empty:
    st.warning("ไม่มีข้อมูลแผนที่ — ตรวจสอบว่ารัน Kafka consumer และ spd_crime_pipeline แล้ว")
else:
    st.plotly_chart(
        create_layered_map(crime_by_neighborhood, calls_geo,
                           show_crime=show_crime, show_calls=show_calls, height=550),
        use_container_width=True,
        config={"displayModeBar": False, "scrollZoom": True},
        key="layered_crime_map"
    )
    caption_parts = []
    if show_crime and not crime_by_neighborhood.empty:
        caption_parts.append(f"🔴 {len(crime_by_neighborhood):,} neighborhoods · {int(crime_by_neighborhood['crime_count'].sum()):,} crimes")
    if show_calls and not calls_geo.empty:
        caption_parts.append(f"📞 {len(calls_geo):,} 911 call locations")
    if caption_parts:
        st.caption(" | ".join(caption_parts))

col_l, col_r = st.columns(2)

with col_l:
    st.subheader("Crime Categories")
    view_type = st.radio("View By", ["Category", "Subcategory"], horizontal=True, key="crime_view_type")

    if crime_raw.empty:
        st.warning("ไม่มีข้อมูล crime")
    elif view_type == "Category":
        if not offense_agg.empty and "offense_category" in offense_agg.columns:
            offense_agg["crime_count"] = pd.to_numeric(
                offense_agg.get("crime_count", 0), errors="coerce").fillna(0)
            grp = (offense_agg.groupby("offense_category", as_index=False)["crime_count"]
                   .sum().sort_values("crime_count", ascending=False).head(15))
            if not grp.empty:
                st.plotly_chart(
                    bar_chart(grp, "offense_category", "crime_count", color="#b84c1e"),
                    use_container_width=True, key="offense_chart"
                )
                st.caption(f"📊 {len(grp)} categories · {int(grp['crime_count'].sum()):,} crimes")
        else:
            st.warning("Category data not found")
    else:  # Subcategory
        if not dim_offense.empty and "offense_sub_category" in dim_offense.columns:
            if "offense_dim_id" in crime_raw.columns and "offense_dim_id" in dim_offense.columns:
                merged_sub = crime_raw.merge(
                    dim_offense[["offense_dim_id", "offense_sub_category"]],
                    on="offense_dim_id", how="left"
                )
                sub_grp = (merged_sub.groupby("offense_sub_category", as_index=False)
                           .size().rename(columns={"size": "crime_count"})
                           .sort_values("crime_count", ascending=False).head(15))
                if not sub_grp.empty:
                    st.plotly_chart(
                        bar_chart(sub_grp, "offense_sub_category", "crime_count", color="#b84c1e"),
                        use_container_width=True, key="offense_chart"
                    )
                    st.caption(f"📊 {len(sub_grp)} subcategories · {int(sub_grp['crime_count'].sum()):,} crimes")
                else:
                    st.info("No subcategory data")
            else:
                st.warning("Cannot join crime data — missing offense_dim_id")
        else:
            st.warning("Subcategory data not found in dim_offense")

with col_r:
    st.markdown("**NIBRS Crime Classification**")
    if not dim_offense.empty and not crime_raw.empty:
        if "offense_dim_id" in crime_raw.columns and "nibrs_group" in dim_offense.columns:
            merged = crime_raw.merge(
                dim_offense[["offense_dim_id", "nibrs_group", "crime_against"]],
                on="offense_dim_id", how="left"
            )
            nibrs_counts = (merged.groupby("nibrs_group", as_index=False)
                            .size().rename(columns={"size": "count"}))
            if not nibrs_counts.empty:
                fig_pie = go.Figure(go.Pie(
                    labels=nibrs_counts["nibrs_group"], values=nibrs_counts["count"],
                    hole=0.4, marker=dict(colors=["#e74c3c", "#3498db", "#95a5a6"])
                ))
                fig_pie.update_layout(
                    title="NIBRS Group A (Serious) vs B",
                    height=400, paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=20, r=20, t=40, b=20)
                )
                st.plotly_chart(fig_pie, use_container_width=True, key="nibrs_pie")

st.divider()

# ════════════════════════════════════════════════════════════
# SECTION 7: 911 Dispatch Heatmap
# ════════════════════════════════════════════════════════════
st.subheader("⏰ 911 Dispatch Patterns (Hour × Day of Week)")

if not heatmap_data.empty and {"hour", "day_of_week", "call_count"} <= set(heatmap_data.columns):
    pivot = heatmap_data.pivot(index="hour", columns="day_of_week", values="call_count").fillna(0)
    day_map = {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
               5: "Thursday", 6: "Friday", 7: "Saturday"}
    pivot.columns = [day_map.get(int(d), f"Day {d}") for d in pivot.columns]

    fig_heat = go.Figure(go.Heatmap(
        z=pivot.values, x=pivot.columns, y=pivot.index,
        colorscale="YlOrRd", text=pivot.values,
        texttemplate="%{text:.0f}", textfont={"size": 9},
        colorbar=dict(title="Calls"),
    ))
    fig_heat.update_layout(
        title="911 Call Volume by Hour × Day of Week",
        xaxis_title="Day of Week", yaxis_title="Hour (0-23)",
        height=500, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(fig_heat, use_container_width=True, key="heatmap_911")

    peak = heatmap_data.loc[heatmap_data["call_count"].idxmax()]
    peak_day = day_map.get(int(peak["day_of_week"]), "?")
    st.caption(f"📊 Peak: **{peak_day} at {int(peak['hour']):02d}:00** — {int(peak['call_count']):,} calls")
else:
    st.info("ยังไม่มีข้อมูล agg_911_by_hour_day")

st.divider()

# ─── Footer ───────────────────────────────────────────────────────────────────
st.caption("✨ ITDS344 · Group 7 · Data Engineering · Streamlit · Kafka · Airflow · MongoDB")
for _ in range(2):
    st.write("")
