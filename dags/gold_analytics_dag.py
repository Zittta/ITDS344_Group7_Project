"""
Gold DAG — Silver → Gold Analytics  (Star Schema + Aggregations in MongoDB)
============================================================================
Builds the Gold layer (Data Warehouse) by reading clean silver collections
and producing analytics-ready structures in MongoDB gold layer.

Star Schema (per PDF design):
  Fact tables:
    gold.fact_crime_events   — one row per offense event (grain = 1 offense)
    gold.fact_911_calls      — one row per 911 call (grain = 1 dispatch)

  Dimension tables:
    gold.dim_time            — date / hour breakdown
    gold.dim_location        — geographic police boundaries
    gold.dim_offense         — NIBRS offense type classification
    gold.dim_event_type      — 911 dispatch event types
    gold.dim_demographics    — neighborhood population (from ACS)

  Pre-computed aggregation views (analytics-ready, rebuilt daily):
    gold.agg_crime_by_neighborhood_month  — crime count by hood + month
    gold.agg_crime_by_offense_category    — crime count by category + month
    gold.agg_911_by_hour_day              — 911 call volume by hour + day-of-week
    gold.agg_crime_per_capita             — crime rate per 10k population per hood

Incremental Load:
  - Fact tables process only silver records newer than gold.watermarks.
  - Dimension tables are incrementally updated (upsert on natural key).
  - Aggregations are fully rebuilt each run (cheap given daily grain).

Idempotency:
  - All upserts use a natural or hash key — running twice is safe.

Schedule: @daily  (gold is rebuilt from yesterday's silver data)

═══════════════════════════════════════════════════════════════════════════════
NEXT STEP — Production Dashboard (Phase 3 recommendation)
═══════════════════════════════════════════════════════════════════════════════
After the gold layer is stable, connect it to a dashboard tool:

Option A — Grafana + MongoDB plugin
  • Install Grafana with the "grafana-mongodb-datasource" plugin.
  • Point it at the gold MongoDB collections.
  • Build panels on top of:
      - agg_crime_by_neighborhood_month  → choropleth map or bar chart
      - agg_911_by_hour_day              → heatmap (hour × day of week)
      - agg_crime_per_capita             → ranked neighborhood table
      - fact_crime_events filtered by date range → trend line

Option B — Apache Superset
  • Use pymongo + Flask-MongoEngine or export gold views to PostgreSQL
    (via a nightly ETL job from gold MongoDB → Postgres gold schema)
    then connect Superset to Postgres.
  • Superset supports rich interactive dashboards with drill-down capability.

Option C — Python Dash / Streamlit (lightweight, course-appropriate)
  • Build a Streamlit app that connects to MongoDB gold layer directly.
  • Use pymongo to query aggregation collections.
  • Visualise with Plotly: crime heatmap, time-series, per-capita choropleth.
  • Deploy as an extra Docker service in docker-compose.yml.

Key business questions the dashboard should answer (from project PDF):
  1. Which neighborhoods have the highest crime rate per capita?
  2. What time of day / day of week do 911 calls peak?
  3. How does crime volume trend over months in 2024?
  4. What are the top 5 offense categories by count?
  5. Is there a correlation between population density and crime rate?
═══════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
MONGO_URI  = os.getenv("MONGO_URI", "mongodb://mongo:27017")
SILVER_DB  = "silver"
GOLD_DB    = "gold"
CHUNK_SIZE = 1_000


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _hash_key(*parts) -> str:
    return hashlib.md5("|".join(str(p) for p in parts).encode()).hexdigest()[:16]


def _get_watermark(db_gold, source: str) -> datetime:
    doc = db_gold["watermarks"].find_one({"source": source})
    if doc and doc.get("last_processed_at"):
        ts = doc["last_processed_at"]
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime(2020, 1, 1, tzinfo=timezone.utc)


def _set_watermark(db_gold, source: str, ts: datetime) -> None:
    db_gold["watermarks"].update_one(
        {"source": source},
        {"$set": {"last_processed_at": ts, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


def _bulk_upsert(collection, ops: list, label: str) -> int:
    if not ops:
        return 0
    try:
        result = collection.bulk_write(ops, ordered=False)
        return result.upserted_count
    except BulkWriteError as exc:
        log.warning("[GOLD][%s] BulkWriteError: %d errors (non-fatal)",
                    label, len(exc.details.get("writeErrors", [])))
        return 0


# ─── Task 1: Build dim_time ───────────────────────────────────────────────────

def build_dim_time(**context) -> dict:
    """
    Extracts all unique (date, hour) combinations from silver crime and 911,
    then upserts into gold.dim_time.
    time_id = integer key YYYYMMDDHH  (e.g. 2024011514)
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["dim_time"]
    coll.create_index("time_id", unique=True, background=True)

    time_keys: set[int] = set()

    for dt_field, collection_name in [
        ("call_datetime",    "silver_911_clean"),
        ("report_date_time", "silver_crime_clean"),
    ]:
        for doc in db_silver[collection_name].find({dt_field: {"$ne": None}}, {dt_field: 1}):
            dt = doc.get(dt_field)
            if isinstance(dt, datetime):
                time_keys.add(int(dt.strftime("%Y%m%d%H")))

    ops = []
    for tk in time_keys:
        s      = str(tk)
        year, month, day, hour = int(s[:4]), int(s[4:6]), int(s[6:8]), int(s[8:10])
        dt_obj = datetime(year, month, day, hour, tzinfo=timezone.utc)
        ops.append(UpdateOne(
            {"time_id": tk},
            {"$setOnInsert": {
                "time_id":     tk,
                "date":        dt_obj.date().isoformat(),
                "year":        year,
                "month":       month,
                "day":         day,
                "hour":        hour,
                "day_of_week": dt_obj.strftime("%A"),   # "Monday" … "Sunday"
                "is_weekend":  dt_obj.weekday() >= 5,
            }},
            upsert=True,
        ))
        if len(ops) >= CHUNK_SIZE:
            _bulk_upsert(coll, ops, "dim_time")
            ops = []

    _bulk_upsert(coll, ops, "dim_time")
    log.info("[Gold] dim_time: %d time keys processed", len(time_keys))
    client.close()
    return {"time_keys": len(time_keys)}


# ─── Task 2: Build dim_location ───────────────────────────────────────────────

def build_dim_location(**context) -> dict:
    """
    Extracts unique location combinations from silver crime data.
    location_id = MD5 hash of (precinct, sector, beat, neighborhood).
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["dim_location"]
    coll.create_index("location_id", unique=True, background=True)

    seen: dict[str, dict] = {}

    for doc in db_silver["silver_crime_clean"].find(
        {},
        {"precinct": 1, "sector": 1, "beat": 1, "neighborhood": 1,
         "reporting_area": 1, "latitude": 1, "longitude": 1},
        batch_size=5_000,
    ):
        precinct = doc.get("precinct", "")
        sector   = doc.get("sector", "")
        beat     = doc.get("beat", "")
        hood     = doc.get("neighborhood", "")
        loc_id   = _hash_key(precinct, sector, beat, hood)
        if loc_id not in seen:
            seen[loc_id] = {
                "location_id":   loc_id,
                "precinct":      precinct,
                "sector":        sector,
                "beat":          beat,
                "neighborhood":  hood,
                "reporting_area": doc.get("reporting_area", ""),
                "latitude":      doc.get("latitude"),
                "longitude":     doc.get("longitude"),
            }

    ops = [
        UpdateOne({"location_id": v["location_id"]}, {"$setOnInsert": v}, upsert=True)
        for v in seen.values()
    ]
    _bulk_upsert(coll, ops, "dim_location")
    log.info("[Gold] dim_location: %d unique locations", len(seen))
    client.close()
    return {"unique_locations": len(seen)}


# ─── Task 3: Build dim_offense ────────────────────────────────────────────────

def build_dim_offense(**context) -> dict:
    """
    Extracts unique offense type combinations from silver crime data.
    offense_dim_id = MD5 hash of NIBRS code + category.
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["dim_offense"]
    coll.create_index("offense_dim_id", unique=True, background=True)

    seen: dict[str, dict] = {}

    for doc in db_silver["silver_crime_clean"].find(
        {},
        {"offense_category": 1, "offense_sub_category": 1,
         "nibrs_offense_code": 1, "nibrs_offense_code_description": 1,
         "nibrs_crime_against_category": 1, "nibrs_group": 1},
        batch_size=5_000,
    ):
        code     = doc.get("nibrs_offense_code", "")
        category = doc.get("offense_category", "")
        oid      = _hash_key(code, category)
        if oid not in seen:
            seen[oid] = {
                "offense_dim_id":               oid,
                "offense_category":             category,
                "offense_sub_category":         doc.get("offense_sub_category", ""),
                "crime_against":                doc.get("nibrs_crime_against_category", ""),
                "nibrs_group":                  doc.get("nibrs_group", ""),
                "offense_code":                 code,
                "offense_description":          doc.get("nibrs_offense_code_description", ""),
            }

    ops = [
        UpdateOne({"offense_dim_id": v["offense_dim_id"]}, {"$setOnInsert": v}, upsert=True)
        for v in seen.values()
    ]
    _bulk_upsert(coll, ops, "dim_offense")
    log.info("[Gold] dim_offense: %d unique offense types", len(seen))
    client.close()
    return {"unique_offenses": len(seen)}


# ─── Task 4: Build dim_event_type ────────────────────────────────────────────

def build_dim_event_type(**context) -> dict:
    """
    Extracts unique 911 dispatch event types from silver 911 data.
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["dim_event_type"]
    coll.create_index("event_type_id", unique=True, background=True)

    event_types = db_silver["silver_911_clean"].distinct("event_type")

    ops = []
    for et in event_types:
        et_id = _hash_key(et)
        ops.append(UpdateOne(
            {"event_type_id": et_id},
            {"$setOnInsert": {
                "event_type_id":       et_id,
                "event_type":          et,
                "police_required_flag": bool(et and ("POLICE" in et or "SPD" in et or "OFFICER" in et)),
            }},
            upsert=True,
        ))

    _bulk_upsert(coll, ops, "dim_event_type")
    log.info("[Gold] dim_event_type: %d unique event types", len(event_types))
    client.close()
    return {"unique_event_types": len(event_types)}


# ─── Task 5: Build dim_demographics ─────────────────────────────────────────

def build_dim_demographics(**context) -> dict:
    """
    Builds dim_demographics from silver.silver_population_clean.
    Keyed by neighborhood_name.
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["dim_demographics"]
    coll.create_index("neighborhood_name", unique=True, background=True)

    ops = []
    for doc in db_silver["silver_population_clean"].find({}):
        hood = doc.get("neighborhood_name", "")
        if not hood:
            continue
        ops.append(UpdateOne(
            {"neighborhood_name": hood},
            {"$set": {
                "neighborhood_name":      hood,
                "acs_year":               doc.get("acs_year"),
                "total_population":       doc.get("total_population"),
                "median_age":             doc.get("median_age"),
                "male_pct":               doc.get("male_pct"),
                "female_pct":             doc.get("female_pct"),
                "white_pct":              doc.get("white_pct"),
                "black_pct":              doc.get("black_pct"),
                "hispanic_pct":           doc.get("hispanic_pct"),
                "median_household_income": doc.get("median_household_income"),
                "poverty_pct":            doc.get("poverty_pct"),
                "total_housing_units":    doc.get("total_housing_units"),
                "_updated_at":            datetime.now(timezone.utc),
            }},
            upsert=True,
        ))

    _bulk_upsert(coll, ops, "dim_demographics")
    log.info("[Gold] dim_demographics: %d neighborhoods", len(ops))
    client.close()
    return {"neighborhoods": len(ops)}


# ─── Task 6: Build fact_crime_events ─────────────────────────────────────────

def build_fact_crime(**context) -> dict:
    """
    Builds gold.fact_crime_events from silver.silver_crime_clean.
    Incremental: only processes silver records newer than gold watermark.
    Joins with dim_time, dim_location, dim_offense via Python lookup.
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["fact_crime_events"]
    coll.create_index("offense_id", unique=True, background=True)

    watermark = _get_watermark(db_gold, "fact_crime_events")

    # Build in-memory dim lookup dicts for fast join
    loc_idx = {
        _hash_key(d["precinct"], d["sector"], d["beat"], d["neighborhood"]): d["location_id"]
        for d in db_gold["dim_location"].find({}, {"location_id": 1, "precinct": 1,
                                                   "sector": 1, "beat": 1, "neighborhood": 1})
    }
    off_idx = {
        _hash_key(d["offense_code"], d["offense_category"]): d["offense_dim_id"]
        for d in db_gold["dim_offense"].find({}, {"offense_dim_id": 1,
                                                   "offense_code": 1, "offense_category": 1})
    }

    cursor = db_silver["silver_crime_clean"].find(
        {"_silver_processed_at": {"$gt": watermark}},
        batch_size=CHUNK_SIZE,
    ).sort("_silver_processed_at", 1)

    ops          = []
    max_silver   = watermark
    processed    = 0
    now          = datetime.now(timezone.utc)

    for doc in cursor:
        offense_id = doc.get("offense_id")
        if not offense_id:
            continue

        # Compute dim keys
        report_dt = doc.get("report_date_time")
        time_id   = int(report_dt.strftime("%Y%m%d%H")) if isinstance(report_dt, datetime) else None

        loc_key    = _hash_key(doc.get("precinct", ""), doc.get("sector", ""),
                               doc.get("beat", ""), doc.get("neighborhood", ""))
        location_id = loc_idx.get(loc_key)

        off_key    = _hash_key(doc.get("nibrs_offense_code", ""), doc.get("offense_category", ""))
        offense_dim_id = off_idx.get(off_key)

        fact_doc = {
            "offense_id":       offense_id,
            "time_id":          time_id,
            "location_id":      location_id,
            "offense_dim_id":   offense_dim_id,
            "report_number":    doc.get("report_number", ""),
            "report_date_time": report_dt,
            "offense_date":     doc.get("offense_date"),
            "is_shooting":      doc.get("is_shooting", False),
            "neighborhood":     doc.get("neighborhood", ""),
            "offense_category": doc.get("offense_category", ""),
            "_silver_processed_at": doc.get("_silver_processed_at"),
            "_gold_loaded_at":  now,
        }

        ops.append(UpdateOne(
            {"offense_id": offense_id},
            {"$set": fact_doc, "$setOnInsert": {"_created_at": now}},
            upsert=True,
        ))

        sp = doc.get("_silver_processed_at")
        if isinstance(sp, datetime):
            sp = sp if sp.tzinfo else sp.replace(tzinfo=timezone.utc)
            if sp > max_silver:
                max_silver = sp
        processed += 1

        if len(ops) >= CHUNK_SIZE:
            _bulk_upsert(coll, ops, "fact_crime_events")
            ops = []

    _bulk_upsert(coll, ops, "fact_crime_events")
    _set_watermark(db_gold, "fact_crime_events", max_silver)

    log.info("[Gold] fact_crime_events: processed=%d", processed)
    client.close()
    return {"processed": processed}


# ─── Task 7: Build fact_911_calls ────────────────────────────────────────────

def build_fact_911(**context) -> dict:
    """
    Builds gold.fact_911_calls from silver.silver_911_clean.
    Incremental load with gold watermark.
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["fact_911_calls"]
    coll.create_index("event_id", unique=True, background=True)

    watermark = _get_watermark(db_gold, "fact_911_calls")

    et_idx = {
        d["event_type"]: d["event_type_id"]
        for d in db_gold["dim_event_type"].find({}, {"event_type_id": 1, "event_type": 1})
    }

    cursor = db_silver["silver_911_clean"].find(
        {"_silver_processed_at": {"$gt": watermark}},
        batch_size=CHUNK_SIZE,
    ).sort("_silver_processed_at", 1)

    ops        = []
    max_silver = watermark
    processed  = 0
    now        = datetime.now(timezone.utc)

    for doc in cursor:
        event_id = doc.get("event_id")
        if not event_id:
            continue

        call_dt  = doc.get("call_datetime")
        time_id  = int(call_dt.strftime("%Y%m%d%H")) if isinstance(call_dt, datetime) else None
        et_id    = et_idx.get(doc.get("event_type", ""))

        fact_doc = {
            "event_id":        event_id,
            "time_id":         time_id,
            "event_type_id":   et_id,
            "event_type":      doc.get("event_type", ""),
            "call_datetime":   call_dt,
            "address":         doc.get("address", ""),
            "latitude":        doc.get("latitude"),
            "longitude":       doc.get("longitude"),
            "is_police_sent":  doc.get("is_police_sent", False),
            "_silver_processed_at": doc.get("_silver_processed_at"),
            "_gold_loaded_at": now,
        }

        ops.append(UpdateOne(
            {"event_id": event_id},
            {"$set": fact_doc, "$setOnInsert": {"_created_at": now}},
            upsert=True,
        ))

        sp = doc.get("_silver_processed_at")
        if isinstance(sp, datetime):
            sp = sp if sp.tzinfo else sp.replace(tzinfo=timezone.utc)
            if sp > max_silver:
                max_silver = sp
        processed += 1

        if len(ops) >= CHUNK_SIZE:
            _bulk_upsert(coll, ops, "fact_911_calls")
            ops = []

    _bulk_upsert(coll, ops, "fact_911_calls")
    _set_watermark(db_gold, "fact_911_calls", max_silver)

    log.info("[Gold] fact_911_calls: processed=%d", processed)
    client.close()
    return {"processed": processed}


# ─── Task 8: Build aggregations ──────────────────────────────────────────────

def build_aggregations(**context) -> dict:
    """
    Builds pre-computed analytics aggregation collections from Gold fact tables.
    These are fully rebuilt each run (cheap at daily grain).

    Collections:
      agg_crime_by_neighborhood_month  → crime count per neighborhood per month
      agg_crime_by_offense_category    → crime count per category per month
      agg_911_by_hour_day              → 911 call volume by hour × day_of_week
      agg_crime_per_capita             → crime_rate per 10k population per hood
    """
    client   = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_gold  = client[GOLD_DB]
    results  = {}

    # ── 1. Crime by neighborhood × month ─────────────────────────────────────
    pipeline_crime_hood = [
        {"$match": {"neighborhood": {"$ne": ""}}},
        {"$addFields": {
            "year":  {"$year": "$report_date_time"},
            "month": {"$month": "$report_date_time"},
        }},
        {"$group": {
            "_id":              {"neighborhood": "$neighborhood", "year": "$year", "month": "$month"},
            "crime_count":      {"$sum": 1},
            "shooting_count":   {"$sum": {"$cond": ["$is_shooting", 1, 0]}},
        }},
        {"$project": {
            "_id":          0,
            "neighborhood": "$_id.neighborhood",
            "year":         "$_id.year",
            "month":        "$_id.month",
            "crime_count":  1,
            "shooting_count": 1,
        }},
        {"$out": "agg_crime_by_neighborhood_month"},
    ]
    db_gold["fact_crime_events"].aggregate(pipeline_crime_hood)
    results["agg_crime_by_neighborhood_month"] = db_gold["agg_crime_by_neighborhood_month"].count_documents({})

    # ── 2. Crime by offense_category × month ─────────────────────────────────
    pipeline_crime_cat = [
        {"$match": {"offense_category": {"$ne": ""}}},
        {"$addFields": {
            "year":  {"$year": "$report_date_time"},
            "month": {"$month": "$report_date_time"},
        }},
        {"$group": {
            "_id":            {"offense_category": "$offense_category", "year": "$year", "month": "$month"},
            "crime_count":    {"$sum": 1},
        }},
        {"$project": {
            "_id":             0,
            "offense_category": "$_id.offense_category",
            "year":            "$_id.year",
            "month":           "$_id.month",
            "crime_count":     1,
        }},
        {"$out": "agg_crime_by_offense_category"},
    ]
    db_gold["fact_crime_events"].aggregate(pipeline_crime_cat)
    results["agg_crime_by_offense_category"] = db_gold["agg_crime_by_offense_category"].count_documents({})

    # ── 3. 911 call volume by hour × day_of_week ─────────────────────────────
    pipeline_911 = [
        {"$match": {"call_datetime": {"$ne": None}}},
        {"$addFields": {
            "hour":        {"$hour": "$call_datetime"},
            "day_of_week": {"$dayOfWeek": "$call_datetime"},   # 1=Sun … 7=Sat
        }},
        {"$group": {
            "_id":       {"hour": "$hour", "day_of_week": "$day_of_week"},
            "call_count": {"$sum": 1},
        }},
        {"$project": {
            "_id":         0,
            "hour":        "$_id.hour",
            "day_of_week": "$_id.day_of_week",
            "call_count":  1,
        }},
        {"$out": "agg_911_by_hour_day"},
    ]
    db_gold["fact_911_calls"].aggregate(pipeline_911)
    results["agg_911_by_hour_day"] = db_gold["agg_911_by_hour_day"].count_documents({})

    # ── 4. Crime per-capita per neighborhood ─────────────────────────────────
    # Mapping from crime neighborhood names (UPPERCASE, SPD source) to
    # population neighborhood names (Title Case, ACS source)
    CRIME_TO_POP: dict[str, str] = {
        "ALASKA JUNCTION":                    "West Seattle Junction",
        "ALKI":                               "Alki/Admiral",
        "BALLARD NORTH":                      "Ballard",
        "BALLARD SOUTH":                      "Ballard",
        "BELLTOWN":                           "Belltown",
        "BITTERLAKE":                         "Bitter Lake",
        "BRIGHTON/DUNLAP":                    "Columbia City",
        "CAPITOL HILL":                       "Capitol Hill",
        "CENTRAL AREA/SQUIRE PARK":           "Central Area/Squire Park",
        "CHINATOWN/INTERNATIONAL DISTRICT":   "Pioneer Square/International District",
        "CLAREMONT/RAINIER VISTA":            "Rainier Beach",
        "COLUMBIA CITY":                      "Columbia City",
        "COMMERCIAL DUWAMISH":                "Duwamish/SODO",
        "COMMERCIAL HARBOR ISLAND":           "Duwamish/SODO",
        "DOWNTOWN COMMERCIAL":                "Downtown Commercial Core",
        "EASTLAKE - EAST":                    "Eastlake",
        "EASTLAKE - WEST":                    "Eastlake",
        "FAUNTLEROY SW":                      "Fauntleroy/Seaview",
        "FIRST HILL":                         "First Hill",
        "FREMONT":                            "Fremont",
        "GENESEE":                            "West Seattle Junction/Genesee Hill",
        "GEORGETOWN":                         "Georgetown",
        "GREENWOOD":                          "Greenwood",
        "HIGH POINT":                         "High Point",
        "HIGHLAND PARK":                      "Highland Park",
        "HILLMAN CITY":                       "Columbia City",
        "JUDKINS PARK/NORTH BEACON HILL":     "Judkins Park",
        "LAKECITY":                           "Lake City",
        "LAKEWOOD/SEWARD PARK":               "Seward Park",
        "MADISON PARK":                       "Madison Park",
        "MADRONA/LESCHI":                     "Madrona/Leschi",
        "MAGNOLIA":                           "Magnolia",
        "MID BEACON HILL":                    "Beacon Hill",
        "MILLER PARK":                        "Miller Park",
        "MONTLAKE/PORTAGE BAY":               "Montlake/Portage Bay",
        "MORGAN":                             "Morgan Junction",
        "MOUNT BAKER":                        "Mt Baker",
        "NEW HOLLY":                          "South Beacon Hill/NewHolly",
        "NORTH ADMIRAL":                      "Admiral",
        "NORTH BEACON HILL":                  "North Beacon Hill",
        "NORTH DELRIDGE":                     "North Delridge",
        "NORTHGATE":                          "Northgate",
        "PHINNEY RIDGE":                      "Greenwood/Phinney Ridge",
        "PIGEON POINT":                       "North Delridge",
        "PIONEER SQUARE":                     "Pioneer Square/International District",
        "QUEEN ANNE":                         "Queen Anne",
        "RAINIER BEACH":                      "Rainier Beach",
        "RAINIER VIEW":                       "Rainier Beach",
        "ROOSEVELT/RAVENNA":                  "Roosevelt",
        "ROXHILL/WESTWOOD/ARBOR HEIGHTS":     "Roxhill/Westwood",
        "SANDPOINT":                          "Laurelhurst/Sand Point",
        "SLU/CASCADE":                        "South Lake Union",
        "SODO":                               "Duwamish/SODO",
        "SOUTH BEACON HILL":                  "South Beacon Hill/NewHolly",
        "SOUTH DELRIDGE":                     "North Delridge",
        "SOUTH PARK":                         "South Park",
        "UNIVERSITY":                         "University District",
        "WALLINGFORD":                        "Wallingford",
    }

    # Build raw crime counts keyed by SPD neighborhood name
    crime_counts_raw: dict[str, int] = {}
    for doc in db_gold["fact_crime_events"].find(
        {"neighborhood": {"$ne": ""}},
        {"neighborhood": 1},
        batch_size=10_000,
    ):
        hood = doc.get("neighborhood", "")
        crime_counts_raw[hood] = crime_counts_raw.get(hood, 0) + 1

    # Aggregate crime counts under population neighborhood names
    crime_counts_by_pop: dict[str, int] = {}
    for crime_hood, count in crime_counts_raw.items():
        pop_hood = CRIME_TO_POP.get(crime_hood)
        if pop_hood:
            crime_counts_by_pop[pop_hood] = crime_counts_by_pop.get(pop_hood, 0) + count

    per_capita_ops = []
    for doc in db_gold["dim_demographics"].find({"total_population": {"$gt": 0}}):
        hood = doc.get("neighborhood_name", "")
        pop  = doc.get("total_population", 0)
        cnt  = crime_counts_by_pop.get(hood, 0)
        rate = round(cnt / pop * 10_000, 2) if pop > 0 else None
        per_capita_ops.append(UpdateOne(
            {"neighborhood_name": hood},
            {"$set": {
                "neighborhood_name": hood,
                "total_population":  pop,
                "total_crimes":      cnt,
                "crime_rate_per_10k": rate,
                "_updated_at":       datetime.now(timezone.utc),
            }},
            upsert=True,
        ))

    if per_capita_ops:
        try:
            db_gold["agg_crime_per_capita"].bulk_write(per_capita_ops, ordered=False)
        except BulkWriteError:
            pass

    results["agg_crime_per_capita"] = db_gold["agg_crime_per_capita"].count_documents({})

    log.info("[Gold] Aggregations built: %s", results)
    client.close()
    return results


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":           "group7",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=10),
}

with DAG(
    dag_id="gold_analytics",
    description="Build Gold Star Schema + aggregation views from Silver MongoDB layer",
    default_args=default_args,
    schedule_interval="*/30 * * * *",   # every 30 min — refreshes analytics from silver
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["gold", "analytics", "warehouse", "star-schema"],
    max_active_runs=1,
) as dag:

    t_dim_time = PythonOperator(
        task_id="build_dim_time",
        python_callable=build_dim_time,
    )
    t_dim_loc = PythonOperator(
        task_id="build_dim_location",
        python_callable=build_dim_location,
    )
    t_dim_off = PythonOperator(
        task_id="build_dim_offense",
        python_callable=build_dim_offense,
    )
    t_dim_et = PythonOperator(
        task_id="build_dim_event_type",
        python_callable=build_dim_event_type,
    )
    t_dim_demo = PythonOperator(
        task_id="build_dim_demographics",
        python_callable=build_dim_demographics,
    )
    t_fact_crime = PythonOperator(
        task_id="build_fact_crime_events",
        python_callable=build_fact_crime,
    )
    t_fact_911 = PythonOperator(
        task_id="build_fact_911_calls",
        python_callable=build_fact_911,
    )
    t_agg = PythonOperator(
        task_id="build_aggregations",
        python_callable=build_aggregations,
        doc_md=(
            "Materialise analytics views: crime by neighbourhood/month, "
            "offense category trends, 911 hour heatmap, crime per-capita rate."
        ),
    )

    # Build all dimensions in parallel first, then facts, then aggregations
    dims = [t_dim_time, t_dim_loc, t_dim_off, t_dim_et, t_dim_demo]
    facts = [t_fact_crime, t_fact_911]

    for fact in facts:
        dims >> fact
        fact >> t_agg
