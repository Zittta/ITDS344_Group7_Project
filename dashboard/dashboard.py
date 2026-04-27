"""
Seattle Crime Intelligence Dashboard - REBUILT FOR SCROLLING
ITDS344 Group 7
"""

import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from datetime import datetime

# ─── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Seattle Crime Intelligence",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CRITICAL: Force scrolling CSS ───────────────────────────
st.markdown("""
<style>
    /* Force everything to be scrollable */
    html, body, .stApp, section.main {
        height: auto !important;
        overflow: visible !important;
    }
    
    /* Better scrollbar */
    ::-webkit-scrollbar { width: 10px; }
    ::-webkit-scrollbar-track { background: #f0f0f0; }
    ::-webkit-scrollbar-thumb { background: #888; border-radius: 5px; }
    ::-webkit-scrollbar-thumb:hover { background: #555; }
    
    /* Basic styling */
    .main { padding: 2rem; }
    .stDataFrame { max-height: 400px; overflow: auto; }
</style>
""", unsafe_allow_html=True)


# ─── MongoDB connection ───────────────────────────────────────
@st.cache_resource(ttl=300)
def connect_mongo():
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
        except:
            continue
    return None, False, None


@st.cache_data(ttl=300)
def load_data(collection_name):
    client, ok, uri = connect_mongo()
    if not ok:
        return pd.DataFrame()
    try:
        db = client["gold"]
        data = list(db[collection_name].find({}, {"_id": 0}).limit(500000))
        return pd.DataFrame(data)
    except:
        return pd.DataFrame()


# ─── Simple chart functions ───────────────────────────────────
def simple_bar_chart(df, x_col, y_col, title=""):
    if df.empty:
        return go.Figure()
    
    fig = go.Figure(go.Bar(
        x=df[x_col],
        y=df[y_col],
        marker=dict(color='#b84c1e')
    ))
    fig.update_layout(
        title=title,
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
    )
    return fig


def create_gauge_chart(value, title, max_value=100, suffix="%", color_ranges=None):
    """Create a gauge chart for metrics display."""
    if color_ranges is None:
        color_ranges = [
            (0, 30, "#e74c3c"),    # Red: 0-30%
            (30, 70, "#f39c12"),   # Orange: 30-70%
            (70, 100, "#27ae60")   # Green: 70-100%
        ]
    
    # Determine color based on value
    gauge_color = "#3498db"  # Default blue
    for low, high, color in color_ranges:
        if low <= value <= high:
            gauge_color = color
            break
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={'text': title, 'font': {'size': 16}},
        number={'suffix': suffix, 'font': {'size': 28}},
        gauge={
            'axis': {'range': [0, max_value], 'tickwidth': 1},
            'bar': {'color': gauge_color},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [0, max_value], 'color': '#f0f0f0'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))
    
    fig.update_layout(
        height=280,
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        font={'color': "#444", 'family': "Arial"}
    )
    
    return fig


def create_layered_map(crime_df, calls_df, show_crime=True, show_calls=True, height=520):
    """Create map with toggleable overlaying layers."""
    fig = go.Figure()
    
    # Layer 1: 911 calls density (background)
    if show_calls and not calls_df.empty:
        fig.add_trace(go.Densitymapbox(
            lat=calls_df["latitude"].values,
            lon=calls_df["longitude"].values,
            radius=16,
            colorscale=[
                [0.0,  "rgba(100,150,255,0)"],
                [0.2,  "rgba(100,150,255,0.4)"],
                [0.5,  "rgba(50,100,220,0.6)"],
                [0.8,  "rgba(20,50,180,0.75)"],
                [1.0,  "rgba(10,20,120,0.85)"],
            ],
            showscale=False,
            name="911 Calls",
            hoverinfo="skip",
        ))
    
    # Layer 2: Crime by neighborhood bubbles (foreground)
    if show_crime and not crime_df.empty:
        fig.add_trace(go.Scattermapbox(
            lat=crime_df["latitude"],
            lon=crime_df["longitude"],
            mode="markers",
            marker=dict(
                size=crime_df["crime_count"] / crime_df["crime_count"].max() * 50 + 10,
                color=crime_df["crime_count"],
                colorscale=[
                    [0.0, "#fde725"],
                    [0.2, "#fca636"],
                    [0.4, "#e16462"],
                    [0.6, "#b12a90"],
                    [0.8, "#6a00a8"],
                    [1.0, "#0d0887"]
                ],
                opacity=0.85,
                showscale=show_crime,
                colorbar=dict(
                    title="Crime<br>Count",
                    thickness=15,
                    len=0.7,
                    x=1.02,
                ) if show_crime else None,
                sizemode='diameter',
            ),
            text=crime_df.apply(lambda row: f"<b>{row.get('neighborhood', 'Unknown')}</b><br>{int(row['crime_count']):,} crimes", axis=1),
            hovertemplate="%{text}<extra></extra>",
            name="Crime by Neighborhood",
        ))
    
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox=dict(
            center=dict(lat=47.6062, lon=-122.3321),
            zoom=10.5
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=0, b=0),
        height=height,
        showlegend=False,
    )
    return fig


@st.cache_data(ttl=300)
def load_crime_by_neighborhood():
    """Aggregate crimes by neighborhood with center coordinates."""
    client, ok, uri = connect_mongo()
    if not ok:
        return pd.DataFrame()
    
    try:
        db = client["gold"]
        
        # Aggregate crimes by neighborhood with location
        pipeline = [
            {
                "$match": {
                    "neighborhood": {"$exists": True, "$ne": None},
                    "location_id": {"$exists": True, "$ne": None}
                }
            },
            {
                "$group": {
                    "_id": {
                        "neighborhood": "$neighborhood",
                        "location_id": "$location_id"
                    },
                    "crime_count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.neighborhood",
                    "crime_count": {"$sum": "$crime_count"},
                    "sample_location": {"$first": "$_id.location_id"}
                }
            },
            {"$sort": {"crime_count": -1}}
        ]
        
        neighborhood_crimes = pd.DataFrame(list(db["fact_crime_events"].aggregate(pipeline)))
        if neighborhood_crimes.empty:
            return pd.DataFrame()
        
        neighborhood_crimes = neighborhood_crimes.rename(columns={"_id": "neighborhood"})
        
        # Get coordinates for each neighborhood (using sample location)
        location_ids = neighborhood_crimes["sample_location"].tolist()
        locations = pd.DataFrame(list(db["dim_location"].find(
            {"location_id": {"$in": location_ids}},
            {"_id": 0, "location_id": 1, "latitude": 1, "longitude": 1}
        )))
        
        if not locations.empty:
            # Merge to get coordinates
            result = neighborhood_crimes.merge(
                locations,
                left_on="sample_location",
                right_on="location_id",
                how="inner"
            )
            result["latitude"] = pd.to_numeric(result["latitude"], errors="coerce")
            result["longitude"] = pd.to_numeric(result["longitude"], errors="coerce")
            result = result.dropna(subset=["latitude", "longitude"])
            result = result[
                result["latitude"].between(47.0, 48.2) &
                result["longitude"].between(-122.8, -121.8)
            ]
            return result[["neighborhood", "crime_count", "latitude", "longitude"]]
        
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not load neighborhood crime data: {e}")
        return pd.DataFrame()


def heatmap_mapbox(df, lat_col="latitude", lon_col="longitude", weight_col=None, height=520):
    """Optimized density heatmap on OpenStreetMap tiles."""
    if df.empty:
        return go.Figure()
    
    z = df[weight_col].values if weight_col and weight_col in df.columns else None
    
    fig = go.Figure(go.Densitymapbox(
        lat=df[lat_col].values,
        lon=df[lon_col].values,
        z=z,
        radius=16,
        colorscale=[
            [0.0,  "rgba(253,231,159,0)"],
            [0.2,  "rgba(253,180,98,0.55)"],
            [0.5,  "rgba(220,60,40,0.80)"],
            [0.8,  "rgba(150,0,30,0.90)"],
            [1.0,  "rgba(80,0,20,1.0)"],
        ],
        showscale=False,
    ))
    fig.update_layout(
        mapbox_style="open-street-map",
        mapbox=dict(center=dict(lat=47.6062, lon=-122.3321), zoom=11),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=0, b=0),
        height=height,
    )
    return fig


@st.cache_data(ttl=300)
def load_crime_geo(limit=500):
    """Load top crime hotspots - optimized for performance."""
    client, ok, uri = connect_mongo()
    if not ok:
        return pd.DataFrame()
    
    try:
        db = client["gold"]
        
        # Step 1: Get top N locations by crime count
        pipeline = [
            {"$match": {"location_id": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$location_id", "crime_count": {"$sum": 1}}},
            {"$sort": {"crime_count": -1}},
            {"$limit": limit}
        ]
        loc_counts = pd.DataFrame(list(db["fact_crime_events"].aggregate(pipeline)))
        if loc_counts.empty:
            return pd.DataFrame()
        loc_counts = loc_counts.rename(columns={"_id": "location_id"})
        
        # Step 2: Get lat/lon for these locations
        location_ids = loc_counts["location_id"].tolist()
        dim_loc = pd.DataFrame(list(db["dim_location"].find(
            {
                "location_id": {"$in": location_ids},
                "latitude": {"$ne": None}, 
                "longitude": {"$ne": None}
            },
            {"_id": 0, "location_id": 1, "latitude": 1, "longitude": 1},
        )))
        if dim_loc.empty:
            return pd.DataFrame()
        
        # Step 3: Merge
        merged = loc_counts.merge(dim_loc, on="location_id", how="inner")
        merged["latitude"] = pd.to_numeric(merged["latitude"], errors="coerce")
        merged["longitude"] = pd.to_numeric(merged["longitude"], errors="coerce")
        merged = merged.dropna(subset=["latitude", "longitude"])
        merged = merged[
            merged["latitude"].between(47.0, 48.2) &
            merged["longitude"].between(-122.8, -121.8)
        ]
        return merged
    except Exception as e:
        st.warning(f"Could not load crime geo: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_calls_geo(limit=3000):
    """Load 911 call locations - optimized."""
    client, ok, uri = connect_mongo()
    if not ok:
        return pd.DataFrame()
    
    try:
        db = client["gold"]
        pipeline = [
            {"$match": {
                "latitude": {"$exists": True, "$ne": None},
                "longitude": {"$exists": True, "$ne": None}
            }},
            {"$project": {"_id": 0, "latitude": 1, "longitude": 1}},
            {"$limit": limit}
        ]
        df = pd.DataFrame(list(db["fact_911_calls"].aggregate(pipeline)))
        if df.empty:
            return df
        
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df = df.dropna(subset=["latitude", "longitude"])
        df = df[(df["latitude"].between(47.0, 48.2)) & (df["longitude"].between(-122.8, -121.8))]
        return df
    except Exception as e:
        st.warning(f"Could not load 911 geo: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_911_heatmap_data():
    """Load aggregated 911 call data by hour and day of week."""
    client, ok, uri = connect_mongo()
    if not ok:
        return pd.DataFrame()
    try:
        db = client["gold"]
        data = list(db["agg_911_by_hour_day"].find({}, {"_id": 0}))
        return pd.DataFrame(data)
    except:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_event_types():
    """Load 911 event types with call counts."""
    client, ok, uri = connect_mongo()
    if not ok:
        return pd.DataFrame()
    try:
        db = client["gold"]
        
        # Aggregate call counts by event type
        pipeline = [
            {"$group": {"_id": "$event_type", "call_count": {"$sum": 1}}},
            {"$sort": {"call_count": -1}},
            {"$limit": 15}
        ]
        data = list(db["fact_911_calls"].aggregate(pipeline))
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.rename(columns={"_id": "event_type"})
        return df
    except:
        return pd.DataFrame()


# ─── Load data ────────────────────────────────────────────────
with st.spinner('Loading data...'):
    client, mongo_ok, mongo_uri = connect_mongo()
    
    crime_raw = load_data("fact_crime_events")
    calls_raw = load_data("fact_911_calls")
    offense_raw = load_data("agg_crime_by_offense_category")
    capita_raw = load_data("agg_crime_per_capita")
    dim_offense = load_data("dim_offense")  # For subcategory data
    
    # Load geo data for maps
    crime_by_neighborhood = load_crime_by_neighborhood()
    calls_geo = load_calls_geo(limit=3000)
    
    # Load new analytics data
    heatmap_911 = load_911_heatmap_data()
    event_types = load_event_types()


# ─── SIDEBAR ──────────────────────────────────────────────────
with st.sidebar:
    st.title("🚔 Seattle Crime Intelligence")
    st.caption("ITDS344 · Group 7 · Data Engineering")
    
    st.divider()
    
    st.subheader("System Status")
    if mongo_ok:
        st.success(f"✅ MongoDB Connected")
        st.caption(mongo_uri)
    else:
        st.error("❌ MongoDB Offline")
    
    st.divider()
    
    st.subheader("Data Sources")
    st.caption("🔴 Seattle Real-Time 911")
    st.caption("🔴 SPD Crime Reports")
    st.caption("🔴 ACS Population")


# ─── MAIN DASHBOARD ───────────────────────────────────────────
st.title("Seattle Crime Dashboard")
st.caption(f"Real-time Crime Intelligence Platform · Last refreshed: {datetime.now().strftime('%d %b %Y %H:%M')}")

st.divider()

# ── Today's Dashboard ─────────────────────────────────────────
st.subheader("📊 Dashboard")

# Time period selector
col_period, col_date = st.columns([2, 3])
with col_period:
    time_period = st.selectbox(
        "Time Period",
        ["Today", "Select Day", "Select Month", "Select Year", "All Time"],
        key="dashboard_period"
    )

# Date picker based on selection
selected_date = None
selected_month = None
selected_year = None

with col_date:
    if time_period == "Select Day":
        selected_date = st.date_input("Pick a date", value=pd.Timestamp.now().date(), key="day_picker")
    elif time_period == "Select Month":
        col_m, col_y = st.columns(2)
        with col_m:
            selected_month = st.selectbox("Month", range(1, 13), index=pd.Timestamp.now().month - 1, 
                                         format_func=lambda x: pd.Timestamp(2024, x, 1).strftime("%B"), key="month_picker")
        with col_y:
            selected_year = st.number_input("Year", min_value=2020, max_value=2026, value=2024, key="month_year_picker")
    elif time_period == "Select Year":
        selected_year = st.number_input("Year", min_value=2020, max_value=2026, value=2024, key="year_picker")
    else:
        st.write("")  # Empty space

period_label = "All Time"  # Default label ป้องกัน NameError
# Filter data based on selection
crime_filtered = crime_raw.copy()
calls_filtered = calls_raw.copy()



# --- Filter crimes by report_date_time เท่านั้น ไม่มี fallback ---
if not crime_raw.empty and "report_date_time" in crime_raw.columns:
    crime_filtered = crime_raw.copy()
    crime_filtered["_dashboard_datetime"] = pd.to_datetime(crime_filtered["report_date_time"], errors="coerce")

    if time_period == "Today":
        today_utc = pd.Timestamp.now(tz="UTC").date()
        crime_filtered = crime_filtered[crime_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.date == today_utc]
        period_label = f"Today ({today_utc.strftime('%B %d, %Y')})"
    elif time_period == "Select Day" and selected_date:
        # selected_date is naive, treat as UTC date
        crime_filtered = crime_filtered[crime_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.date == selected_date]
        period_label = selected_date.strftime("%B %d, %Y")
    elif time_period == "Select Month" and selected_month and selected_year:
        crime_filtered = crime_filtered[
            (crime_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.year == selected_year) &
            (crime_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.month == selected_month)
        ]
        period_label = f"{pd.Timestamp(selected_year, selected_month, 1).strftime('%B %Y')}"
    elif time_period == "Select Year" and selected_year:
        crime_filtered = crime_filtered[crime_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.year == selected_year]
        period_label = str(selected_year)
    else:  # All Time
        period_label = "All Time"
else:
    crime_filtered = pd.DataFrame()  # ไม่มีข้อมูลหรือไม่มี field

if not calls_raw.empty and "call_datetime" in calls_raw.columns:
    calls_filtered["call_datetime_parsed"] = pd.to_datetime(calls_filtered["call_datetime"], errors="coerce")
    
    if time_period == "Today":
        today = pd.Timestamp.now().date()
        calls_filtered = calls_filtered[calls_filtered["call_datetime_parsed"].dt.date == today]
    elif time_period == "Select Day" and selected_date:
        calls_filtered = calls_filtered[calls_filtered["call_datetime_parsed"].dt.date == selected_date]
    elif time_period == "Select Month" and selected_month and selected_year:
        calls_filtered = calls_filtered[
            (calls_filtered["call_datetime_parsed"].dt.year == selected_year) &
            (calls_filtered["call_datetime_parsed"].dt.month == selected_month)
        ]
    elif time_period == "Select Year" and selected_year:
        calls_filtered = calls_filtered[calls_filtered["call_datetime_parsed"].dt.year == selected_year]

st.caption(f"📅 Showing data for: **{period_label}**")


# ═══ KPI CARDS ROW (4 cards in grid) ═════════════════════════
col1, col2, col3, col4 = st.columns(4)






# --- Filter 911 Calls by call_datetime เท่านั้น ไม่มี fallback ---
if not calls_raw.empty and "call_datetime" in calls_raw.columns:
    calls_filtered = calls_raw.copy()
    calls_filtered["_dashboard_datetime"] = pd.to_datetime(calls_filtered["call_datetime"], errors="coerce")

    if time_period == "Today":
        today_utc = pd.Timestamp.now(tz="UTC").date()
        calls_filtered = calls_filtered[calls_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.date == today_utc]
    elif time_period == "Select Day" and selected_date:
        calls_filtered = calls_filtered[calls_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.date == selected_date]
    elif time_period == "Select Month" and selected_month and selected_year:
        calls_filtered = calls_filtered[
            (calls_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.year == selected_year) &
            (calls_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.month == selected_month)
        ]
    elif time_period == "Select Year" and selected_year:
        calls_filtered = calls_filtered[calls_filtered["_dashboard_datetime"].dt.tz_localize("UTC", nonexistent='NaT', ambiguous='NaT').dt.year == selected_year]
    # else: All Time (no filter)
else:
    calls_filtered = pd.DataFrame()  # ไม่มีข้อมูลหรือไม่มี field

crime_count = len(crime_filtered)
calls_count = len(calls_filtered)

# Calculate additional metrics
shooting_count = crime_filtered["is_shooting"].sum() if "is_shooting" in crime_filtered.columns else 0
shooting_rate = (shooting_count / crime_count * 100) if crime_count > 0 else 0

police_sent_count = calls_filtered["is_police_sent"].sum() if "is_police_sent" in calls_filtered.columns else 0
police_response_rate = (police_sent_count / calls_count * 100) if calls_count > 0 else 0


with col1:
    st.metric("Total Crimes", f"{crime_count:,}")

with col2:
    st.metric("911 Calls", f"{calls_count:,}")

with col3:
    st.metric("Shooting Incidents", f"{int(shooting_count):,}")


# View detailed data
with st.expander("📋 View Detailed Records"):
    tab1, tab2 = st.tabs(["Crime Records", "911 Call Records"])
    
    with tab1:
        if not crime_filtered.empty:
            # Always show report_date_time if present
            base_cols = ["offense_id", "report_date_time", "offense_date", "offense_category", 
                         "offense_sub_category", "neighborhood", "precinct", "is_shooting"]
            display_cols = [c for c in base_cols if c in crime_filtered.columns]
            if display_cols:
                crime_display = crime_filtered[display_cols].tail(100).copy()
                # Format datetime columns as UTC (GMT+0000)
                if "offense_date" in crime_display.columns:
                    crime_display["offense_date"] = pd.to_datetime(crime_display["offense_date"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M UTC")
                if "report_date_time" in crime_display.columns:
                    crime_display["report_date_time"] = pd.to_datetime(crime_display["report_date_time"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M UTC")
                st.dataframe(crime_display, use_container_width=True, height=400, hide_index=True)
                st.caption(f"📊 Showing last 100 of {len(crime_filtered):,} total crimes")
            else:
                st.dataframe(crime_filtered.tail(100), use_container_width=True, height=400, hide_index=True)
        else:
            st.info("No crime records for the selected period")
    
    with tab2:
        if not calls_filtered.empty:
            display_cols = [c for c in ["event_id", "call_datetime", "event_type", 
                                       "address", "is_police_sent", "latitude", "longitude"]
                           if c in calls_filtered.columns]
            if display_cols:
                calls_display = calls_filtered[display_cols].tail(100).copy()
                if "call_datetime" in calls_display.columns:
                    calls_display["call_datetime"] = pd.to_datetime(calls_display["call_datetime"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M UTC")
                st.dataframe(calls_display, use_container_width=True, height=400, hide_index=True)
                st.caption(f"📊 Showing last 100 of {len(calls_filtered):,} total 911 calls")
            else:
                st.dataframe(calls_filtered.tail(100), use_container_width=True, height=400, hide_index=True)
        else:
            st.info("No 911 call records for the selected period")

st.divider()
# ── Seattle Crime Map ─────────────────────────────────────────
st.subheader("🗺️ Seattle Crime Map")

# Initialize session state for map toggles
if "show_crime_layer" not in st.session_state:
    st.session_state.show_crime_layer = True
if "show_calls_layer" not in st.session_state:
    st.session_state.show_calls_layer = True

# Layer toggles
col_toggle1, col_toggle2 = st.columns(2)
with col_toggle1:
    show_crime = st.checkbox("🔴 Crime by Neighborhood", 
                            value=st.session_state.show_crime_layer, 
                            key="crime_layer_toggle")
    st.session_state.show_crime_layer = show_crime
    
with col_toggle2:
    show_calls = st.checkbox("📞 911 Calls Density", 
                            value=st.session_state.show_calls_layer, 
                            key="calls_layer_toggle")
    st.session_state.show_calls_layer = show_calls

# Render map with selected layers
if crime_by_neighborhood.empty and calls_geo.empty:
    st.warning("No map data available. Trigger `spd_crime_pipeline` in Airflow and ensure Kafka is running.")
else:
    st.plotly_chart(
        create_layered_map(crime_by_neighborhood, calls_geo, show_crime=show_crime, show_calls=show_calls, height=550),
        use_container_width=True,
        config={"displayModeBar": False, "scrollZoom": True},
        key="layered_crime_map"
    )
    
    # Build caption
    caption_parts = []
    if show_crime and not crime_by_neighborhood.empty:
        total_crimes = int(crime_by_neighborhood["crime_count"].sum())
        caption_parts.append(f"🔴 {len(crime_by_neighborhood):,} neighborhoods · {total_crimes:,} crimes")
    if show_calls and not calls_geo.empty:
        caption_parts.append(f"📞 {len(calls_geo):,} 911 call locations")
    
    if caption_parts:
        st.caption(" | ".join(caption_parts))

st.divider()

# ═══ CRIME ANALYSIS GRID (2 columns) ═════════════════════════
col_left, col_right = st.columns(2)

# ── LEFT: Crime Categories Chart ──────────────────────────────
with col_left:
    st.subheader("Crime Categories")
    
    # View type selector
    view_type = st.radio(
        "View By",
        ["Category", "Subcategory"],
        horizontal=True,
        key="crime_view_type"
    )
    
    if crime_raw.empty:
        st.warning("No crime data available. Run the pipeline first.")
    else:
        if view_type == "Category":
            # Use pre-aggregated category data
            if not offense_raw.empty and "offense_category" in offense_raw.columns:
                if "crime_count" in offense_raw.columns:
                    offense_raw["crime_count"] = pd.to_numeric(offense_raw["crime_count"], errors="coerce").fillna(0)
                    offense_agg = (
                        offense_raw.groupby("offense_category", as_index=False)["crime_count"]
                        .sum()
                        .sort_values("crime_count", ascending=False)
                        .head(15)
                    )
                else:
                    offense_agg = (
                        offense_raw.groupby("offense_category", as_index=False)
                        .size()
                        .rename(columns={"size": "crime_count"})
                        .sort_values("crime_count", ascending=False)
                        .head(15)
                    )
                
                if not offense_agg.empty:
                    fig = simple_bar_chart(offense_agg, "offense_category", "crime_count", "")
                    st.plotly_chart(fig, use_container_width=True, key="offense_chart")
                    st.caption(f"📊 {len(offense_agg)} categories · {offense_agg['crime_count'].sum():,} crimes")
                else:
                    st.info("No category data available")
            else:
                st.warning("Category data not found")
        
        else:  # Subcategory
            # Use dim_offense which has offense_sub_category
            if not dim_offense.empty and "offense_sub_category" in dim_offense.columns:
                # Join fact_crime_events with dim_offense to get subcategories
                if "offense_dim_id" in crime_raw.columns and "offense_dim_id" in dim_offense.columns:
                    # Merge to get subcategory
                    crime_with_subcat = crime_raw.merge(
                        dim_offense[["offense_dim_id", "offense_sub_category"]], 
                        on="offense_dim_id", 
                        how="left"
                    )
                    
                    offense_agg = (
                        crime_with_subcat.groupby("offense_sub_category", as_index=False)
                        .size()
                        .rename(columns={"size": "crime_count"})
                        .sort_values("crime_count", ascending=False)
                        .head(15)
                    )
                    
                    if not offense_agg.empty:
                        fig = simple_bar_chart(offense_agg, "offense_sub_category", "crime_count", "")
                        st.plotly_chart(fig, use_container_width=True, key="offense_chart")
                        st.caption(f"📊 {len(offense_agg)} subcategories · {offense_agg['crime_count'].sum():,} crimes")
                    else:
                        st.info("No subcategory data available")
                else:
                    st.warning("Cannot join crime data with offense dimension - missing offense_dim_id")
            else:
                st.warning("Subcategory data not found in dim_offense table")

# ── RIGHT: Highest Risk Areas Chart ───────────────────────────
with col_right:
    st.subheader("Highest Risk Areas")
    
    # Number of top areas slider
    top_n = st.slider("Number of neighborhoods", min_value=5, max_value=20, value=10, key="top_n_slider")
    
    if capita_raw.empty:
        st.warning("No per capita data available")
    else:
        if "neighborhood_name" in capita_raw.columns and "crime_rate_per_10k" in capita_raw.columns:
            capita_raw["crime_rate_per_10k"] = pd.to_numeric(capita_raw["crime_rate_per_10k"], errors="coerce").fillna(0)
            capita_top = capita_raw.sort_values("crime_rate_per_10k", ascending=False).head(top_n)
            
            fig = simple_bar_chart(capita_top, "neighborhood_name", "crime_rate_per_10k", "")
            st.plotly_chart(fig, use_container_width=True, key="risk_chart")
            st.caption(f"📊 Top {len(capita_top)} by crime rate per 10K population")
        else:
            st.warning("Missing columns in capita data")

st.divider()

# ═══ 911 & NIBRS ANALYSIS GRID (2 columns) ═══════════════════
col_left, col_right = st.columns(2)

# ── LEFT: Top 911 Event Types ─────────────────────────────────
with col_left:
    st.subheader("📞 Top 911 Event Types")
    
    if event_types.empty:
        st.warning("No event type data available")
    else:
        if "event_type" in event_types.columns and "call_count" in event_types.columns:
            fig = simple_bar_chart(event_types, "event_type", "call_count", "")
            st.plotly_chart(fig, use_container_width=True, key="event_types_chart")
            st.caption(f"📊 Top {len(event_types)} types · {event_types['call_count'].sum():,} calls")
        else:
            st.warning("Missing columns in event type data")

# ── RIGHT: NIBRS Crime Classification ─────────────────────────
with col_right:
    st.subheader("🔍 NIBRS Classification")
    
    if dim_offense.empty or crime_raw.empty:
        st.warning("No NIBRS data available")
    else:
        if "offense_dim_id" in crime_raw.columns and "nibrs_group" in dim_offense.columns:
            # Join crime data with offense dimension to get NIBRS group
            crime_nibrs = crime_raw.merge(
                dim_offense[["offense_dim_id", "nibrs_group", "crime_against"]], 
                on="offense_dim_id", 
                how="left"
            )
            
            # NIBRS Group A vs B (single chart in this column)
            if "nibrs_group" in crime_nibrs.columns:
                nibrs_group_counts = (
                    crime_nibrs.groupby("nibrs_group", as_index=False)
                    .size()
                    .rename(columns={"size": "crime_count"})
                    .sort_values("crime_count", ascending=False)
                )
                
                if not nibrs_group_counts.empty:
                    fig = go.Figure(data=[go.Pie(
                        labels=nibrs_group_counts["nibrs_group"],
                        values=nibrs_group_counts["crime_count"],
                        hole=0.4,
                        marker=dict(colors=["#e74c3c", "#3498db"])
                    )])
                    fig.update_layout(
                        title="Group A (Serious) vs Group B",
                        height=400,
                        margin=dict(l=20, r=20, t=40, b=20),
                        paper_bgcolor='rgba(0,0,0,0)',
                    )
                    st.plotly_chart(fig, use_container_width=True, key="nibrs_group_chart")
                    st.caption("📊 Group A: Serious | Group B: Less serious")

st.divider()

# ── Additional NIBRS: Crime Against Category ──────────────────
st.subheader("🎯 Crime Against Category Distribution")

if not dim_offense.empty and not crime_raw.empty:
    if "offense_dim_id" in crime_raw.columns and "crime_against" in dim_offense.columns:
        crime_nibrs = crime_raw.merge(
            dim_offense[["offense_dim_id", "crime_against"]], 
            on="offense_dim_id", 
            how="left"
        )
        
        if "crime_against" in crime_nibrs.columns:
            # Get all crime_against categories
            crime_against_counts = (
                crime_nibrs[crime_nibrs["crime_against"].notna()]
                .groupby("crime_against", as_index=False)
                .size()
                .rename(columns={"size": "crime_count"})
                .sort_values("crime_count", ascending=False)
            )
            
            if not crime_against_counts.empty:
                # Calculate total for percentages (use all crimes for accurate percentage)
                total_count = crime_against_counts["crime_count"].sum()
                
                # Create a list of all categories with their data (only real data, no padding)
                categories_data = []
                for _, row in crime_against_counts.iterrows():
                    categories_data.append({
                        "category": row["crime_against"],
                        "count": int(row["crime_count"]),
                        "pct": row["crime_count"] / total_count * 100
                    })
                
                # First row: up to 3 columns
                col1, col2, col3 = st.columns(3)
                
                if len(categories_data) > 0:
                    with col1:
                        data = categories_data[0]
                        st.metric(
                            f"{data['category']} ({data['pct']:.1f}%)",
                            f"{data['count']:,}",
                            None
                        )
                
                if len(categories_data) > 1:
                    with col2:
                        data = categories_data[1]
                        st.metric(
                            f"{data['category']} ({data['pct']:.1f}%)",
                            f"{data['count']:,}",
                            None
                        )
                
                if len(categories_data) > 2:
                    with col3:
                        data = categories_data[2]
                        st.metric(
                            f"{data['category']} ({data['pct']:.1f}%)",
                            f"{data['count']:,}",
                            None
                        )
                
                # Second row: up to 3 more columns (if there are more categories)
                if len(categories_data) > 3:
                    col4, col5, col6 = st.columns(3)
                    
                    with col4:
                        data = categories_data[3]
                        st.metric(
                            f"{data['category']} ({data['pct']:.1f}%)",
                            f"{data['count']:,}",
                            None
                        )
                    
                    if len(categories_data) > 4:
                        with col5:
                            data = categories_data[4]
                            st.metric(
                                f"{data['category']} ({data['pct']:.1f}%)",
                                f"{data['count']:,}",
                                None
                            )
                    
                
                st.caption("📊 PERSON: Against individuals | PROPERTY: Against belongings | SOCIETY: Against public order")

st.divider()

# ═══ 911 DISPATCH HEATMAP (Full Width) ═══════════════════════
st.subheader("⏰ 911 Dispatch Patterns")

if heatmap_911.empty:
    st.warning("No heatmap data available")
else:
    if "hour" in heatmap_911.columns and "day_of_week" in heatmap_911.columns and "call_count" in heatmap_911.columns:
        # Create pivot table for heatmap
        heatmap_pivot = heatmap_911.pivot(index="hour", columns="day_of_week", values="call_count").fillna(0)
        
        # Map day numbers to names (handle both 0-6 and 0-7 ranges)
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_mapping = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 
                      4: "Friday", 5: "Saturday", 6: "Sunday", 7: "Monday"}  # 7 wraps to Monday
        heatmap_pivot.columns = [day_mapping.get(int(d), f"Day {d}") for d in heatmap_pivot.columns]
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_pivot.values,
            x=heatmap_pivot.columns,
            y=heatmap_pivot.index,
            colorscale="YlOrRd",
            text=heatmap_pivot.values,
            texttemplate="%{text:.0f}",
            textfont={"size": 10},
            colorbar=dict(title="Calls")
        ))
        
        fig.update_layout(
            title="911 Call Volume by Hour and Day of Week",
            xaxis_title="Day of Week",
            yaxis_title="Hour of Day (0-23)",
            height=600,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
        )
        
        st.plotly_chart(fig, use_container_width=True, key="heatmap_911")
        
        # Find peak hour
        peak_idx = heatmap_911["call_count"].idxmax()
        if pd.notna(peak_idx):
            peak_row = heatmap_911.loc[peak_idx]
            peak_hour = int(peak_row["hour"])
            peak_day = day_mapping.get(int(peak_row["day_of_week"]), "Unknown")
            peak_count = int(peak_row["call_count"])
            st.caption(f"📊 Peak: **{peak_day} at {peak_hour:02d}:00** with {peak_count:,} calls")
    else:
        st.warning("Missing required columns in heatmap data")

st.divider()

# ── Footer ────────────────────────────────────────────────────
st.caption("✨ ITDS344 · Group 7 · Data Engineering · Streamlit · Kafka · Airflow · MongoDB")
st.caption("🚀 Seattle Crime Intelligence Dashboard · Grid Layout with Performance Gauges")

# Add extra space to ensure scrolling
for i in range(3):
    st.write("")
