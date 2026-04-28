"""
SPD Crime Pipeline — End-to-End  (Socrata API → Bronze → Silver → Gold)
========================================================================
Full medallion pipeline for Seattle Police Department Crime data source.

Flow:
  silver_transform_crime                          (Task 1)
      ↓  [parallel]
  gold_dim_location ─────┐
  gold_dim_offense  ─────┤
  gold_dim_neighborhood ─┘                        (Task 2a/b/c)
      ↓  [all done]
  gold_fact_crime_events                          (Task 3)
      ↓  [parallel]
  gold_agg_crime_by_category ──────────────────┐
  gold_agg_crime_per_capita ───────────────────┤  (Task 4a/b/c/d)
  gold_agg_crime_trend_monthly ────────────────┤
  gold_agg_neighborhood_safety_profile ─────────┘

Bronze:  Kafka consumer_bronze.py (Docker service) → MongoDB bronze.spd_crime
Silver:  bronze.spd_crime → silver.silver_crime_clean
Gold:    silver.silver_crime_clean →
           gold.dim_location          — unique precinct/sector/beat/neighborhood combos
           gold.dim_offense           — NIBRS offense taxonomy
           gold.dim_neighborhood      — neighborhood hub (links crime + 911 + population)
           gold.fact_crime_events     — 1 row per offense
           gold.agg_crime_by_offense_category  — crime × category × month
           gold.agg_crime_per_capita           — crime rate per 10K population
           gold.agg_crime_trend_monthly        — crime trend by neighborhood × month
           gold.agg_neighborhood_safety_profile — composite safety score per neighborhood

Schedule: every 60 minutes (incremental watermark-based load)
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from airflow import DAG
from airflow.models.baseoperator import cross_downstream
from airflow.operators.python import PythonOperator
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
MONGO_URI     = os.getenv("MONGO_URI", "mongodb://mongo:27017")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
BRONZE_DB     = "bronze"
SILVER_DB     = "silver"
GOLD_DB       = "gold"
BATCH_SIZE    = 50_000
UPSERT_CHUNK  = 2_000
CHUNK_SIZE    = 1_000

SOURCE_CFG = {
    "api_url":            "https://data.seattle.gov/resource/tazs-3rd5.json",
    "timestamp_field":    "report_date_time",
    "unique_key":         "offense_id",
    "required_fields":    ["offense_id", "report_date_time"],
    "initial_start_date": "2024-01-01T00:00:00",
}

# Neighborhood name mapping: SPD UPPERCASE → list of ACS Title Case neighborhoods
# Many-to-many: one SPD area can contribute to multiple ACS neighborhoods
# (e.g., DOWNTOWN COMMERCIAL covers both Downtown and Downtown Commercial Core)
CRIME_TO_POP: dict[str, list[str]] = {
    "ALASKA JUNCTION":                    ["West Seattle Junction"],
    "ALKI":                               ["Alki/Admiral"],
    "BALLARD NORTH":                      ["Ballard", "Ballard-Interbay-Northend", "Sunset Hill/Loyal Heights", "Whittier Heights"],
    "BALLARD SOUTH":                      ["Ballard", "Ballard-Interbay-Northend"],
    "BELLTOWN":                           ["Belltown"],
    "BITTERLAKE":                         ["Bitter Lake", "Broadview/Bitter Lake", "Haller Lake", "Licton Springs"],
    "BRIGHTON/DUNLAP":                    ["Columbia City", "Graham"],
    "CAPITOL HILL":                       ["Capitol Hill", "First Hill/Capitol Hill", "North Capitol Hill"],
    "CENTRAL AREA/SQUIRE PARK":           ["Central Area/Squire Park", "Central District", "Central District South"],
    "CHINATOWN/INTERNATIONAL DISTRICT":   ["Pioneer Square/International District"],
    "CLAREMONT/RAINIER VISTA":            ["Rainier Beach", "Othello"],
    "COLUMBIA CITY":                      ["Columbia City"],
    "COMMERCIAL DUWAMISH":                ["Duwamish/SODO", "Greater Duwamish"],
    "COMMERCIAL HARBOR ISLAND":           ["Duwamish/SODO", "Greater Duwamish"],
    "DOWNTOWN COMMERCIAL":                ["Downtown Commercial Core", "Downtown"],
    "EASTLAKE - EAST":                    ["Eastlake", "Cascade/Eastlake"],
    "EASTLAKE - WEST":                    ["Eastlake", "Cascade/Eastlake"],
    "FAUNTLEROY SW":                      ["Fauntleroy/Seaview"],
    "FIRST HILL":                         ["First Hill", "First Hill/Capitol Hill"],
    "FREMONT":                            ["Fremont"],
    "GENESEE":                            ["West Seattle Junction/Genesee Hill"],
    "GEORGETOWN":                         ["Georgetown", "Riverview"],
    "GREENWOOD":                          ["Greenwood", "Crown Hill"],
    "HIGH POINT":                         ["High Point"],
    "HIGHLAND PARK":                      ["Highland Park", "Westwood-Highland Park"],
    "HILLMAN CITY":                       ["Columbia City", "Graham"],
    "JUDKINS PARK/NORTH BEACON HILL":     ["Judkins Park", "North Beacon Hill/Jefferson Park"],
    "LAKECITY":                           ["Lake City", "Olympic Hills/Victory Heights", "Cedar Park/Meadowbrook", "Pinehurst-Haller Lake"],
    "LAKEWOOD/SEWARD PARK":               ["Seward Park"],
    "MADISON PARK":                       ["Madison Park"],
    "MADRONA/LESCHI":                     ["Madrona/Leschi"],
    "MAGNOLIA":                           ["Magnolia", "Interbay"],
    "MID BEACON HILL":                    ["Beacon Hill"],
    "MILLER PARK":                        ["Miller Park", "Madison-Miller"],
    "MONTLAKE/PORTAGE BAY":               ["Montlake/Portage Bay"],
    "MORGAN":                             ["Morgan Junction"],
    "MOUNT BAKER":                        ["Mt Baker", "Mt. Baker/North Rainier"],
    "NEW HOLLY":                          ["South Beacon Hill/NewHolly"],
    "NORTH ADMIRAL":                      ["Admiral"],
    "NORTH BEACON HILL":                  ["North Beacon Hill", "North Beacon Hill/Jefferson Park"],
    "NORTH DELRIDGE":                     ["North Delridge"],
    "NORTHGATE":                          ["Northgate", "Northgate/Maple Leaf", "Haller Lake"],
    "PHINNEY RIDGE":                      ["Greenwood/Phinney Ridge", "Aurora-Licton Springs"],
    "PIGEON POINT":                       ["North Delridge"],
    "PIONEER SQUARE":                     ["Pioneer Square/International District"],
    "QUEEN ANNE":                         ["Queen Anne", "Uptown"],
    "RAINIER BEACH":                      ["Rainier Beach", "Othello"],
    "RAINIER VIEW":                       ["Rainier Beach"],
    "ROOSEVELT/RAVENNA":                  ["Roosevelt", "Ravenna/Bryant"],
    "ROXHILL/WESTWOOD/ARBOR HEIGHTS":     ["Roxhill/Westwood", "Arbor Heights"],
    "SANDPOINT":                          ["Laurelhurst/Sand Point", "Wedgwood/View Ridge"],
    "SLU/CASCADE":                        ["South Lake Union", "Cascade/Eastlake"],
    "SODO":                               ["Duwamish/SODO", "Greater Duwamish"],
    "SOUTH BEACON HILL":                  ["South Beacon Hill/NewHolly"],
    "SOUTH DELRIDGE":                     ["North Delridge"],
    "SOUTH PARK":                         ["South Park", "Riverview"],
    "UNIVERSITY":                         ["University District"],
    "WALLINGFORD":                        ["Wallingford", "Green Lake"],
}

# Fallback lat/lon centroids for ACS neighborhoods that cannot be derived from dim_location.
# Used when no SPD neighborhood in CRIME_TO_POP maps to this ACS area (e.g. Council Districts).
KNOWN_CENTROIDS: dict[str, tuple] = {
    # Administrative districts (Council Districts — no SPD boundary match)
    "Council District 1":           (47.680, -122.353),
    "Council District 2":           (47.550, -122.270),
    "Council District 3":           (47.618, -122.308),
    "Council District 4":           (47.670, -122.303),
    "Council District 5":           (47.720, -122.330),
    "Council District 6":           (47.672, -122.383),
    "Council District 7":           (47.611, -122.342),
    "Outside Centers":              (47.580, -122.340),
    # Real neighborhoods with no direct SPD name match
    "Pinehurst-Haller Lake":        (47.727, -122.319),
    "Aurora-Licton Springs":        (47.709, -122.336),
    "Downtown":                     (47.605, -122.334),
    "Othello":                      (47.543, -122.279),
    "Crown Hill":                   (47.693, -122.374),
    "Madison-Miller":               (47.622, -122.301),
    "First Hill/Capitol Hill":      (47.614, -122.318),
    "Central District South":       (47.598, -122.297),
    "Central District":             (47.608, -122.299),
    "Graham":                       (47.533, -122.278),
    "Westwood-Highland Park":       (47.529, -122.367),
    "Green Lake":                   (47.681, -122.332),
    "Greater Duwamish":             (47.566, -122.345),
    "Uptown":                       (47.624, -122.352),
    "Ballard-Interbay-Northend":    (47.667, -122.382),
    "Olympic Hills/Victory Heights":(47.722, -122.310),
    "Haller Lake":                  (47.715, -122.332),
    "Mt. Baker/North Rainier":      (47.576, -122.290),
    "Interbay":                     (47.645, -122.374),
    "Arbor Heights":                (47.519, -122.383),
    "Wedgwood/View Ridge":          (47.685, -122.296),
    "Sunset Hill/Loyal Heights":    (47.691, -122.396),
    "Cascade/Eastlake":             (47.634, -122.326),
    "Ravenna/Bryant":               (47.673, -122.310),
    "Northgate/Maple Leaf":         (47.698, -122.322),
    "Cedar Park/Meadowbrook":       (47.714, -122.296),
    "Whittier Heights":             (47.687, -122.374),
    "North Beacon Hill/Jefferson Park": (47.571, -122.303),
    "North Capitol Hill":           (47.626, -122.315),
    "Broadview/Bitter Lake":        (47.724, -122.349),
    "North Beach/Blue Ridge":       (47.738, -122.378),
    "Riverview":                    (47.527, -122.364),
    "Licton Springs":               (47.705, -122.334),
}


# ─── Shared helpers ───────────────────────────────────────────────────────────

def _hash_key(*parts) -> str:
    return hashlib.md5("|".join(str(p) for p in parts).encode()).hexdigest()[:16]


def _parse_dt(val: str) -> Optional[datetime]:
    if not val:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _bulk_upsert(collection, ops: list, label: str) -> int:
    if not ops:
        return 0
    try:
        result = collection.bulk_write(ops, ordered=False)
        return result.upserted_count
    except BulkWriteError as exc:
        log.warning("[%s] BulkWriteError: %d errors (non-fatal)",
                    label, len(exc.details.get("writeErrors", [])))
        return 0


# ── Bronze watermark ──────────────────────────────────────────────────────────

def _get_bronze_wm(db) -> str:
    doc = db["watermarks"].find_one({"source": "spd_crime", "layer": "bronze"})
    if doc and doc.get("last_ingested_dt"):
        ts = doc["last_ingested_dt"]
        if isinstance(ts, datetime):
            return ts.strftime("%Y-%m-%dT%H:%M:%S")
    return SOURCE_CFG["initial_start_date"]


def _set_bronze_wm(db, dt_str: str) -> None:
    try:
        dt = datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return
    db["watermarks"].update_one(
        {"source": "spd_crime", "layer": "bronze"},
        {"$set": {"last_ingested_dt": dt, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


# ── Silver watermark ──────────────────────────────────────────────────────────

def _get_silver_wm(db_silver) -> datetime:
    doc = db_silver["watermarks"].find_one({"source": "spd_crime"})
    if doc and doc.get("last_processed_at"):
        ts = doc["last_processed_at"]
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime(2020, 1, 1, tzinfo=timezone.utc)


def _set_silver_wm(db_silver, ts: datetime) -> None:
    db_silver["watermarks"].update_one(
        {"source": "spd_crime"},
        {"$set": {"last_processed_at": ts, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


# ── Gold watermark ────────────────────────────────────────────────────────────

def _get_gold_wm(db_gold, source: str) -> datetime:
    doc = db_gold["watermarks"].find_one({"source": source})
    if doc and doc.get("last_processed_at"):
        ts = doc["last_processed_at"]
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime(2020, 1, 1, tzinfo=timezone.utc)


def _set_gold_wm(db_gold, source: str, ts: datetime) -> None:
    db_gold["watermarks"].update_one(
        {"source": source},
        {"$set": {"last_processed_at": ts, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


# ─── Task 1: Silver Transform ─────────────────────────────────────────────────

def silver_transform_crime(**context) -> dict:
    """
    Cleans bronze.spd_crime → silver.silver_crime_clean.
    DQ Rule 1: offense_id must not be null.
    DQ Rule 2: report_date_time must parse to a valid ISO datetime.
    DQ Rule 3: lat/lon must be in valid WGS84 range if present.
    DQ Rule 4: offense_category must not be null (defaults to UNKNOWN if missing).
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_bronze = client[BRONZE_DB]
    db_silver = client[SILVER_DB]
    coll_out  = db_silver["silver_crime_clean"]
    coll_out.create_index("offense_id", background=True)

    watermark = _get_silver_wm(db_silver)
    log.info("[Silver-Crime] Processing bronze records since %s", watermark)

    cursor     = db_bronze["spd_crime"].find(
        {"_ingested_at": {"$gt": watermark}},
        batch_size=CHUNK_SIZE,
    ).sort("_ingested_at", 1)

    ops          = []
    max_ingested = watermark
    processed    = 0
    skipped_dq   = 0
    now          = datetime.now(timezone.utc)


    for doc in cursor:
        if not doc.get("offense_id"):
            skipped_dq += 1
            continue

        report_dt = _parse_dt(doc.get("report_date_time", ""))
        if report_dt is None:
            skipped_dq += 1
            continue

        # DQ Rule 5 (Range): report_date_time must not be in the future (1-day buffer for timezone drift)
        if report_dt > now + timedelta(days=1):
            log.warning("[DQ-RANGE][silver-crime] Future report_date_time=%s for offense_id=%s — dropped",
                        report_dt, doc.get("offense_id"))
            skipped_dq += 1
            continue

        # Data cleaning: skip invalid offense categories
        offense_cat = (doc.get("offense_category") or "").strip().upper()
        invalid_categories = ["UNKNOWN", "NOT_A_CRIME", "REDACTED", "OOJ", "-", ""]
        if offense_cat in invalid_categories or offense_cat.startswith("99"):
            skipped_dq += 1
            continue

        # Data cleaning: skip invalid neighborhoods
        neighborhood = (doc.get("neighborhood") or "").strip()
        if neighborhood in ["UNKNOWN", "UNKNOW", "-", "REDACTED", "OOJ", ""] or neighborhood.startswith("99"):
            skipped_dq += 1
            continue

        # Data cleaning: skip if nibrs_crime_against_category is NOT_A_CRIME
        nibrs_crime_against = (doc.get("nibrs_crime_against_category") or "").strip().upper()
        if nibrs_crime_against == "NOT_A_CRIME":
            skipped_dq += 1
            continue

        # Data cleaning: skip if beat or precinct is OOJ
        beat = (doc.get("beat") or "").strip().upper()
        precinct = (doc.get("precinct") or "").strip().upper()
        if beat == "OOJ" or precinct == "OOJ":
            skipped_dq += 1
            continue

        # Data cleaning: skip if nibrs_offense_code or offense_sub_category is 999 or offense_sub_category is UNKNOW or UNKNOWN
        nibrs_offense_code = (doc.get("nibrs_offense_code") or "").strip()
        offense_sub_category = (doc.get("offense_sub_category") or "").strip().upper()
        if nibrs_offense_code == "999" or offense_sub_category in ("999", "UNKNOW", "UNKNOWN"):
            skipped_dq += 1
            continue

        # Data cleaning: skip if sector startswith 99
        sector = (doc.get("sector") or "").strip().upper()
        if sector.startswith("99"):
            skipped_dq += 1
            continue

        # Data cleaning: skip if block_address, census_block_2020, reporting_area, latitude, longitude is REDACTED
        block_address = (doc.get("block_address") or "").strip().upper()
        census_block_2020 = (doc.get("census_block_2020") or "").strip().upper()
        reporting_area = (doc.get("reporting_area") or "").strip().upper()
        lat_raw = doc.get("latitude")
        lon_raw = doc.get("longitude")
        lat = _safe_float(lat_raw)
        lon = _safe_float(lon_raw)
        if block_address == "REDACTED" or block_address == "-" or census_block_2020 == "REDACTED" or reporting_area == "REDACTED":
            skipped_dq += 1
            continue
        if (isinstance(lat_raw, str) and lat_raw.strip().upper() == "REDACTED") or (isinstance(lon_raw, str) and lon_raw.strip().upper() == "REDACTED"):
            skipped_dq += 1
            continue
        if lat is not None and not -90 <= lat <= 90:
            lat = None
        if lon is not None and not -180 <= lon <= 180:
            lon = None

        # Fixed is_shooting logic: check for "Shots Fired" or "Shooting" keywords
        shooting_type = (doc.get("shooting_type_group") or "").strip()
        is_shooting = bool(re.search(r"(shots?\s+fired|shooting)", shooting_type, re.I))

        silver_doc = {
            "offense_id":                     doc["offense_id"],
            "report_number":                  (doc.get("report_number") or "").strip(),
            "report_date_time":               report_dt,
            "offense_date":                   _parse_dt(doc.get("offense_date", "")),
            "offense_category":               offense_cat,
            "offense_sub_category":           offense_sub_category,
            "nibrs_offense_code":             nibrs_offense_code,
            "nibrs_offense_code_description": (doc.get("nibrs_offense_code_description") or "").strip(),
            "nibrs_crime_against_category":   nibrs_crime_against,
            "nibrs_group":                    (doc.get("nibrs_group_a_b") or "").strip().upper(),
            "is_shooting":                    is_shooting,
            "block_address":                  block_address,
            "latitude":                       lat,
            "longitude":                      lon,
            "precinct":                       precinct,
            "sector":                         sector,
            "beat":                           beat,
            "neighborhood":                   neighborhood,
            "census_block_2020":              census_block_2020,
            "reporting_area":                 (doc.get("reporting_area") or "").strip(),
            "_source":                        "spd_crime",
            "_bronze_ingested_at":            doc.get("_ingested_at"),
            "_silver_processed_at":           now,
        }

        ops.append(UpdateOne(
            {"offense_id": silver_doc["offense_id"]},
            {"$set": silver_doc, "$setOnInsert": {"_created_at": now}},
            upsert=True,
        ))

        _ia = doc.get("_ingested_at")
        if _ia:
            _ia = _ia if _ia.tzinfo else _ia.replace(tzinfo=timezone.utc)
            if _ia > max_ingested:
                max_ingested = _ia
        processed += 1

        if len(ops) >= CHUNK_SIZE:
            _bulk_upsert(coll_out, ops, "silver-crime")
            ops = []

    _bulk_upsert(coll_out, ops, "silver-crime")
    _set_silver_wm(db_silver, max_ingested)
    log.info("[Silver-Crime] processed=%d skipped_dq=%d watermark=%s",
             processed, skipped_dq, max_ingested)
    client.close()
    return {"processed": processed, "skipped_dq": skipped_dq}


# ─── Task 3a: Gold — dim_location ────────────────────────────────────────────

def gold_dim_location(**context) -> dict:
    """
    Extracts unique (precinct, sector, beat, neighborhood) combos
    from silver_crime_clean → gold.dim_location.
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
                "location_id":    loc_id,
                "precinct":       precinct,
                "sector":         sector,
                "beat":           beat,
                "neighborhood":   hood,
                "reporting_area": doc.get("reporting_area", ""),
                "latitude":       doc.get("latitude"),
                "longitude":      doc.get("longitude"),
            }

    ops = [
        UpdateOne({"location_id": v["location_id"]}, {"$setOnInsert": v}, upsert=True)
        for v in seen.values()
    ]
    _bulk_upsert(coll, ops, "dim_location")
    log.info("[Gold] dim_location: %d unique locations", len(seen))
    client.close()
    return {"unique_locations": len(seen)}


# ─── Task 3c: Gold — dim_offense ─────────────────────────────────────────────

def gold_dim_offense(**context) -> dict:
    """
    Extracts unique NIBRS offense type combinations
    from silver_crime_clean → gold.dim_offense.
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
        offense_sub_category = (doc.get("offense_sub_category", "") or "").strip().upper()
        if offense_sub_category in ("UNKNOW", "UNKNOWN"):
            continue
        oid      = _hash_key(code, category)
        if oid not in seen:
            seen[oid] = {
                "offense_dim_id":       oid,
                "offense_category":     category,
                "offense_sub_category": doc.get("offense_sub_category", ""),
                "crime_against":        doc.get("nibrs_crime_against_category", ""),
                "nibrs_group":          doc.get("nibrs_group", ""),
                "offense_code":         code,
                "offense_description":  doc.get("nibrs_offense_code_description", ""),
            }

    ops = [
        UpdateOne({"offense_dim_id": v["offense_dim_id"]}, {"$setOnInsert": v}, upsert=True)
        for v in seen.values()
    ]
    _bulk_upsert(coll, ops, "dim_offense")
    log.info("[Gold] dim_offense: %d unique offense types", len(seen))
    client.close()
    return {"unique_offenses": len(seen)}


# ─── Task 4: Gold — fact_crime_events ────────────────────────────────────────

def gold_fact_crime(**context) -> dict:
    """
    Builds gold.fact_crime_events from silver.silver_crime_clean.
    Incremental load via gold watermark.
    Joins with dim_time, dim_location, dim_offense via in-memory lookup.
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["fact_crime_events"]
    coll.create_index("offense_id", unique=True, background=True)

    watermark = _get_gold_wm(db_gold, "fact_crime_events")

    loc_idx = {
        _hash_key(d["precinct"], d["sector"], d["beat"], d["neighborhood"]): d["location_id"]
        for d in db_gold["dim_location"].find(
            {}, {"location_id": 1, "precinct": 1, "sector": 1, "beat": 1, "neighborhood": 1}
        )
    }
    off_idx = {
        _hash_key(d["offense_code"], d["offense_category"]): d["offense_dim_id"]
        for d in db_gold["dim_offense"].find(
            {}, {"offense_dim_id": 1, "offense_code": 1, "offense_category": 1}
        )
    }

    cursor     = db_silver["silver_crime_clean"].find(
        {"_silver_processed_at": {"$gt": watermark}},
        batch_size=CHUNK_SIZE,
    ).sort("_silver_processed_at", 1)

    ops        = []
    max_silver = watermark
    processed  = 0
    now        = datetime.now(timezone.utc)

    for doc in cursor:
        offense_id = doc.get("offense_id")
        if not offense_id:
            continue

        report_dt      = doc.get("report_date_time")
        time_id        = int(report_dt.strftime("%Y%m%d%H")) if isinstance(report_dt, datetime) else None
        loc_key        = _hash_key(doc.get("precinct", ""), doc.get("sector", ""),
                                   doc.get("beat", ""), doc.get("neighborhood", ""))
        location_id    = loc_idx.get(loc_key)
        off_key        = _hash_key(doc.get("nibrs_offense_code", ""), doc.get("offense_category", ""))
        offense_dim_id = off_idx.get(off_key)

        fact_doc = {
            "offense_id":           offense_id,
            "time_id":              time_id,
            "location_id":          location_id,
            "offense_dim_id":       offense_dim_id,
            "report_date_time":     report_dt,
            "year":                 report_dt.year  if isinstance(report_dt, datetime) else None,
            "month":                report_dt.month if isinstance(report_dt, datetime) else None,
            "offense_category":     doc.get("offense_category", ""),
            "neighborhood":         doc.get("neighborhood", ""),
            "is_shooting":          doc.get("is_shooting", False),
            "_silver_processed_at": doc.get("_silver_processed_at"),
            "_gold_loaded_at":      now,
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
    _set_gold_wm(db_gold, "fact_crime_events", max_silver)
    log.info("[Gold] fact_crime_events: processed=%d", processed)
    client.close()
    return {"processed": processed}


# ─── Task 5a: Gold — agg_crime_by_offense_category ───────────────────────────

def gold_agg_crime_by_category(**context) -> dict:
    """Materialises crime count per offense category per month (full rebuild)."""
    client  = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_gold = client[GOLD_DB]

    pipeline = [
        {"$match": {"offense_category": {"$ne": ""}}},
        {"$addFields": {
            "year":  {"$year": "$report_date_time"},
            "month": {"$month": "$report_date_time"},
        }},
        {"$group": {
            "_id":          {"offense_category": "$offense_category", "year": "$year", "month": "$month"},
            "crime_count":  {"$sum": 1},
        }},
        {"$project": {
            "_id":              0,
            "offense_category": "$_id.offense_category",
            "year":             "$_id.year",
            "month":            "$_id.month",
            "crime_count":      1,
        }},
        {"$out": "agg_crime_by_offense_category"},
    ]
    db_gold["fact_crime_events"].aggregate(pipeline)
    count = db_gold["agg_crime_by_offense_category"].count_documents({})
    log.info("[Gold] agg_crime_by_offense_category: %d rows", count)
    client.close()
    return {"agg_crime_by_offense_category": count}


# ─── Task 5c: Gold — agg_crime_per_capita ────────────────────────────────────

def gold_agg_crime_per_capita(**context) -> dict:
    """
    Computes crime rate per 10k population per neighborhood (full rebuild).
    Joins fact_crime_events with dim_demographics.
    Requires seattle_population_pipeline to have run at least once.
    """
    client  = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_gold = client[GOLD_DB]

    # Build raw crime counts keyed by SPD neighborhood name
    crime_counts_raw: dict[str, int] = {}
    for doc in db_gold["fact_crime_events"].find(
        {"neighborhood": {"$ne": ""}}, {"neighborhood": 1}, batch_size=10_000
    ):
        hood = doc.get("neighborhood", "")
        crime_counts_raw[hood] = crime_counts_raw.get(hood, 0) + 1

    # Map SPD names → ACS names (many-to-many: one SPD can map to multiple ACS)
    crime_counts_by_pop: dict[str, int] = {}
    for crime_hood, count in crime_counts_raw.items():
        acs_names = CRIME_TO_POP.get(crime_hood, [])
        for pop_hood in acs_names:
            crime_counts_by_pop[pop_hood] = crime_counts_by_pop.get(pop_hood, 0) + count

    ops = []
    for doc in db_gold["dim_demographics"].find({"total_population": {"$gt": 0}}):
        hood = doc.get("neighborhood_name", "")
        pop  = doc.get("total_population", 0)
        cnt  = crime_counts_by_pop.get(hood, 0)
        rate = round(cnt / pop * 10_000, 2) if pop > 0 else None
        ops.append(UpdateOne(
            {"neighborhood_name": hood},
            {"$set": {
                "neighborhood_name":  hood,
                "total_population":   pop,
                "total_crimes":       cnt,
                "crime_rate_per_10k": rate,
                "_updated_at":        datetime.now(timezone.utc),
            }},
            upsert=True,
        ))

    if ops:
        try:
            db_gold["agg_crime_per_capita"].bulk_write(ops, ordered=False)
        except BulkWriteError:
            pass

    count = db_gold["agg_crime_per_capita"].count_documents({})
    log.info("[Gold] agg_crime_per_capita: %d neighborhoods", count)
    client.close()
    return {"agg_crime_per_capita": count}


# ─── Task 2c: Gold — dim_neighborhood ────────────────────────────────────────

def gold_dim_neighborhood(**context) -> dict:
    """
    Builds gold.dim_neighborhood — the central hub linking all 3 datasets.

    Sources:
      - crime neighborhoods (from silver_crime_clean, mapped via CRIME_TO_POP)
      - ACS demographics (from dim_demographics — name + population stats)
      - Representative lat/lon centroid from dim_location

    Result: 1 row per ACS neighborhood with crime + population metadata
    for cross-dataset analytics (crime rate, 911 rate, demographics).
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["dim_neighborhood"]
    coll.create_index("neighborhood_name", unique=True, background=True)

    # Step 1: Build centroid lookup: ACS neighborhood_name → avg (lat, lon) from dim_location
    # First build reverse map: ACS name → list of SPD names (many-to-many)
    acs_to_spd: dict[str, list[str]] = {}
    for spd_name, acs_names in CRIME_TO_POP.items():
        for acs_name in acs_names:
            acs_to_spd.setdefault(acs_name, []).append(spd_name)

    # Collect lat/lon per SPD neighborhood from dim_location
    spd_centroids: dict[str, list[tuple]] = {}
    for loc in db_gold["dim_location"].find(
        {"latitude": {"$ne": None}, "longitude": {"$ne": None}},
        {"neighborhood": 1, "latitude": 1, "longitude": 1},
    ):
        hood = loc.get("neighborhood", "")
        if hood:
            spd_centroids.setdefault(hood, []).append(
                (loc["latitude"], loc["longitude"])
            )

    def _acs_centroid(acs_name: str):
        """Average lat/lon across all SPD neighborhoods mapping to this ACS area.
        Falls back to KNOWN_CENTROIDS for administrative areas with no SPD match."""
        spd_names = acs_to_spd.get(acs_name, [])
        lats, lons = [], []
        for spd in spd_names:
            for lat, lon in spd_centroids.get(spd, []):
                lats.append(lat)
                lons.append(lon)
        if lats:
            return round(sum(lats) / len(lats), 6), round(sum(lons) / len(lons), 6)
        # Fallback: use hardcoded centroid if available
        if acs_name in KNOWN_CENTROIDS:
            return KNOWN_CENTROIDS[acs_name]
        return None, None

    # Step 2: Iterate dim_demographics (one row per ACS neighborhood)
    ops = []
    count = 0
    for demo in db_gold["dim_demographics"].find({}):
        acs_name = demo.get("neighborhood_name", "")
        if not acs_name:
            continue
        lat, lon = _acs_centroid(acs_name)
        neighborhood_doc = {
            "neighborhood_name":      acs_name,
            "acs_year":               demo.get("acs_year"),
            "total_population":       demo.get("total_population"),
            "median_age":             demo.get("median_age"),
            "median_household_income":demo.get("median_household_income"),
            "poverty_pct":            demo.get("poverty_pct"),
            "white_pct":              demo.get("white_pct"),
            "black_pct":              demo.get("black_pct"),
            "hispanic_pct":           demo.get("hispanic_pct"),
            "total_housing_units":    demo.get("total_housing_units"),
            "lat":                    lat,    # centroid latitude
            "lon":                    lon,    # centroid longitude
            "_updated_at":            datetime.now(timezone.utc),
        }
        ops.append(UpdateOne(
            {"neighborhood_name": acs_name},
            {"$set": neighborhood_doc},
            upsert=True,
        ))
        count += 1
        if len(ops) >= 500:
            db_gold["dim_neighborhood"].bulk_write(ops, ordered=False)
            ops = []

    if ops:
        db_gold["dim_neighborhood"].bulk_write(ops, ordered=False)

    log.info("[Gold] dim_neighborhood: %d neighborhoods", count)
    client.close()
    return {"dim_neighborhood": count}


# ─── Task 4c: Gold — agg_crime_trend_monthly ─────────────────────────────────

def gold_agg_crime_trend_monthly(**context) -> dict:
    """
    Pre-computes crime count by neighborhood × category × month.
    Uses CRIME_TO_POP mapping to align SPD neighborhood names with ACS names.
    Result enables: trend lines per area, compare areas over time.
    """
    client  = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_gold = client[GOLD_DB]

    pipeline = [
        {"$match": {
            "neighborhood":     {"$ne": ""},
            "report_date_time": {"$ne": None},
            "offense_category": {"$ne": ""},
        }},
        {"$addFields": {
            "year":  {"$year":  "$report_date_time"},
            "month": {"$month": "$report_date_time"},
        }},
        {"$group": {
            "_id": {
                "neighborhood":     "$neighborhood",
                "offense_category": "$offense_category",
                "year":             "$year",
                "month":            "$month",
            },
            "crime_count":    {"$sum": 1},
            "shooting_count": {"$sum": {"$cond": ["$is_shooting", 1, 0]}},
        }},
        {"$project": {
            "_id":              0,
            "neighborhood":     "$_id.neighborhood",
            "offense_category": "$_id.offense_category",
            "year":             "$_id.year",
            "month":            "$_id.month",
            "crime_count":      1,
            "shooting_count":   1,
        }},
        {"$out": "agg_crime_trend_monthly"},
    ]
    db_gold["fact_crime_events"].aggregate(pipeline)
    count = db_gold["agg_crime_trend_monthly"].count_documents({})
    log.info("[Gold] agg_crime_trend_monthly: %d rows", count)
    client.close()
    return {"agg_crime_trend_monthly": count}


# ─── Task 4d: Gold — agg_neighborhood_safety_profile ─────────────────────────

def gold_agg_neighborhood_safety_profile(**context) -> dict:
    """
    Builds composite neighborhood safety profile by joining:
      - crime counts + rates (from fact_crime_events + dim_demographics)
      - 911 call counts + rates (from fact_911_calls — if neighborhood_name populated)
      - demographic context (poverty, income, median_age from dim_neighborhood)

    This is the PRIMARY analytics collection for cross-dataset analysis.
    Result: 1 row per ACS neighborhood.
    """
    client  = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_gold = client[GOLD_DB]
    now     = datetime.now(timezone.utc)

    # ── 1. Crime counts per SPD neighborhood → map to ACS name ──
    crime_by_spd: dict[str, dict] = {}
    for doc in db_gold["fact_crime_events"].find(
        {"neighborhood": {"$ne": ""}},
        {"neighborhood": 1, "is_shooting": 1, "offense_category": 1},
        batch_size=10_000,
    ):
        hood = doc.get("neighborhood", "")
        if hood not in crime_by_spd:
            crime_by_spd[hood] = {"total_crimes": 0, "shooting_count": 0}
        crime_by_spd[hood]["total_crimes"] += 1
        if doc.get("is_shooting"):
            crime_by_spd[hood]["shooting_count"] += 1

    # Aggregate to ACS names (many-to-many: one SPD can map to multiple ACS)
    crime_by_acs: dict[str, dict] = {}
    for spd_hood, data in crime_by_spd.items():
        acs_names = CRIME_TO_POP.get(spd_hood, [])
        for acs_name in acs_names:
            if acs_name not in crime_by_acs:
                crime_by_acs[acs_name] = {"total_crimes": 0, "shooting_count": 0}
            crime_by_acs[acs_name]["total_crimes"]   += data["total_crimes"]
            crime_by_acs[acs_name]["shooting_count"] += data["shooting_count"]

    # ── 2. 911 calls per ACS neighborhood (from fact_911_calls.neighborhood_name) ──
    calls_by_acs: dict[str, int] = {}
    for doc in db_gold["fact_911_calls"].find(
        {"neighborhood_name": {"$ne": None}},
        {"neighborhood_name": 1},
        batch_size=10_000,
    ):
        acs_name = doc.get("neighborhood_name", "")
        if acs_name:
            calls_by_acs[acs_name] = calls_by_acs.get(acs_name, 0) + 1

    # ── 3. Merge with dim_neighborhood (demographics + centroid) ──
    ops = []
    for nbhd in db_gold["dim_neighborhood"].find({}):
        acs_name = nbhd.get("neighborhood_name", "")
        if not acs_name:
            continue
        pop      = nbhd.get("total_population") or 0
        crime    = crime_by_acs.get(acs_name, {}).get("total_crimes", 0)
        shooting = crime_by_acs.get(acs_name, {}).get("shooting_count", 0)
        calls    = calls_by_acs.get(acs_name, 0)

        crime_rate  = round(crime   / pop * 10_000, 2) if pop > 0 else None
        calls_rate  = round(calls   / pop * 10_000, 2) if pop > 0 else None
        shoot_rate  = round(shooting / pop * 10_000, 2) if pop > 0 else None

        profile_doc = {
            "neighborhood_name":      acs_name,
            "total_population":       pop,
            "median_age":             nbhd.get("median_age"),
            "median_household_income":nbhd.get("median_household_income"),
            "poverty_pct":            nbhd.get("poverty_pct"),
            "white_pct":              nbhd.get("white_pct"),
            "black_pct":              nbhd.get("black_pct"),
            "hispanic_pct":           nbhd.get("hispanic_pct"),
            "total_housing_units":    nbhd.get("total_housing_units"),
            "total_crimes":           crime,
            "shooting_incidents":     shooting,
            "total_911_calls":        calls,
            "crime_rate_per_10k":     crime_rate,
            "calls_911_rate_per_10k": calls_rate,
            "shooting_rate_per_10k":  shoot_rate,
            "lat":                    nbhd.get("lat"),
            "lon":                    nbhd.get("lon"),
            "_updated_at":            now,
        }
        ops.append(UpdateOne(
            {"neighborhood_name": acs_name},
            {"$set": profile_doc},
            upsert=True,
        ))

    if ops:
        try:
            db_gold["agg_neighborhood_safety_profile"].bulk_write(ops, ordered=False)
        except BulkWriteError:
            pass

    count = db_gold["agg_neighborhood_safety_profile"].count_documents({})
    log.info("[Gold] agg_neighborhood_safety_profile: %d neighborhoods", count)
    client.close()
    return {"agg_neighborhood_safety_profile": count}


# ─── Task 4e: Gold — agg_911_per_capita ──────────────────────────────────────

def gold_agg_911_per_capita(**context) -> dict:
    """
    Computes 911 call rate per 10K population per ACS neighborhood.
    Uses fact_911_calls.neighborhood_name (populated via nearest-centroid enrichment
    in the 911 pipeline after dim_neighborhood is built).
    """
    client  = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_gold = client[GOLD_DB]
    now     = datetime.now(timezone.utc)

    # Count 911 calls per ACS neighborhood
    calls_by_acs: dict[str, int] = {}
    for doc in db_gold["fact_911_calls"].find(
        {"neighborhood_name": {"$ne": None}},
        {"neighborhood_name": 1},
        batch_size=10_000,
    ):
        name = doc.get("neighborhood_name", "")
        if name:
            calls_by_acs[name] = calls_by_acs.get(name, 0) + 1

    ops = []
    for demo in db_gold["dim_demographics"].find({"total_population": {"$gt": 0}}):
        acs_name = demo.get("neighborhood_name", "")
        pop      = demo.get("total_population", 0)
        calls    = calls_by_acs.get(acs_name, 0)
        rate     = round(calls / pop * 10_000, 2) if pop > 0 else None
        ops.append(UpdateOne(
            {"neighborhood_name": acs_name},
            {"$set": {
                "neighborhood_name":       acs_name,
                "total_population":        pop,
                "total_911_calls":         calls,
                "calls_rate_per_10k":      rate,
                "_updated_at":             now,
            }},
            upsert=True,
        ))

    if ops:
        try:
            db_gold["agg_911_per_capita"].bulk_write(ops, ordered=False)
        except BulkWriteError:
            pass

    count = db_gold["agg_911_per_capita"].count_documents({})
    log.info("[Gold] agg_911_per_capita: %d neighborhoods", count)
    client.close()
    return {"agg_911_per_capita": count}


# ─── Task 4f: Fix null neighborhood_name in fact_911_calls (cold-start recovery) ───

def gold_reenrich_null_neighborhoods_911(**context) -> dict:
    """
    Self-healing: fixes fact_911_calls records where neighborhood_name is null.

    Root cause of nulls: 911 pipeline may run before dim_neighborhood is built
    (cold-start race condition). The 911 DAG's watermark then advances and the
    records are never re-processed by a normal 911 pipeline run.

    This task runs every crime pipeline cycle — after dim_neighborhood is
    guaranteed to exist — and updates any remaining null records in-place.
    Idempotent: re-running has no effect once all records are enriched.
    """
    client  = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_gold = client[GOLD_DB]

    # Load neighborhood centroids from dim_neighborhood
    nbhd_coords: list[dict] = list(db_gold["dim_neighborhood"].find(
        {"lat": {"$ne": None}, "lon": {"$ne": None}},
        {"_id": 0, "neighborhood_name": 1, "lat": 1, "lon": 1},
    ))

    if not nbhd_coords:
        log.warning("[reenrich-911] dim_neighborhood empty — skipping")
        client.close()
        return {"skipped": True, "reason": "dim_neighborhood empty"}

    def _nearest(lat, lon):
        if lat is None or lon is None:
            return None
        best, dist2 = None, float("inf")
        for n in nbhd_coords:
            d2 = (lat - n["lat"]) ** 2 + (lon - n["lon"]) ** 2
            if d2 < dist2:
                dist2, best = d2, n["neighborhood_name"]
        return best

    null_count = db_gold["fact_911_calls"].count_documents({"neighborhood_name": None})
    if null_count == 0:
        log.info("[reenrich-911] No null neighborhood_name records — nothing to do")
        client.close()
        return {"fixed": 0}

    log.info("[reenrich-911] Found %d null neighborhood_name records — re-enriching", null_count)

    ops   = []
    fixed = 0
    for doc in db_gold["fact_911_calls"].find(
        {"neighborhood_name": None},
        {"_id": 1, "latitude": 1, "longitude": 1},
        batch_size=2_000,
    ):
        name = _nearest(doc.get("latitude"), doc.get("longitude"))
        if name:
            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"neighborhood_name": name}}))
            fixed += 1
        if len(ops) >= 2_000:
            db_gold["fact_911_calls"].bulk_write(ops, ordered=False)
            ops = []
    if ops:
        db_gold["fact_911_calls"].bulk_write(ops, ordered=False)

    log.info("[reenrich-911] Fixed %d / %d records", fixed, null_count)
    client.close()
    return {"fixed": fixed, "total_null": null_count}


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":           "group7",
    "depends_on_past": False,
    "retries":         3,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="spd_crime_pipeline",
    description="End-to-end crime pipeline: Bronze → Silver → Gold (star schema + cross-dataset analytics)",
    default_args=default_args,
    schedule_interval="0 * * * *",   # every 60 minutes
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["pipeline", "crime", "silver", "gold"],
    max_active_runs=1,
) as dag:

    t_silver = PythonOperator(
        task_id="silver_transform_crime",
        python_callable=silver_transform_crime,
        doc_md="Clean & standardise bronze.spd_crime → silver.silver_crime_clean",
    )
    t_dim_loc = PythonOperator(
        task_id="gold_dim_location",
        python_callable=gold_dim_location,
        doc_md="Build gold.dim_location from silver_crime_clean",
    )
    t_dim_off = PythonOperator(
        task_id="gold_dim_offense",
        python_callable=gold_dim_offense,
        doc_md="Build gold.dim_offense from NIBRS codes in silver_crime_clean",
    )
    t_dim_nbhd = PythonOperator(
        task_id="gold_dim_neighborhood",
        python_callable=gold_dim_neighborhood,
        doc_md="Build gold.dim_neighborhood — hub linking crime + 911 + population",
    )
    t_fact = PythonOperator(
        task_id="gold_fact_crime_events",
        python_callable=gold_fact_crime,
        doc_md="Build gold.fact_crime_events — incremental fact table",
    )
    t_agg_cat = PythonOperator(
        task_id="gold_agg_crime_by_category",
        python_callable=gold_agg_crime_by_category,
        doc_md="Materialise agg_crime_by_offense_category (full rebuild)",
    )
    t_agg_pc = PythonOperator(
        task_id="gold_agg_crime_per_capita",
        python_callable=gold_agg_crime_per_capita,
        doc_md="Materialise agg_crime_per_capita — joins with dim_demographics",
    )
    t_agg_trend = PythonOperator(
        task_id="gold_agg_crime_trend_monthly",
        python_callable=gold_agg_crime_trend_monthly,
        doc_md="Materialise agg_crime_trend_monthly — neighborhood × category × month",
    )
    t_agg_911pc = PythonOperator(
        task_id="gold_agg_911_per_capita",
        python_callable=gold_agg_911_per_capita,
        doc_md="Materialise agg_911_per_capita — 911 rate per 10K per ACS neighborhood",
    )
    t_agg_profile = PythonOperator(
        task_id="gold_agg_neighborhood_safety_profile",
        python_callable=gold_agg_neighborhood_safety_profile,
        doc_md="Materialise agg_neighborhood_safety_profile — composite cross-dataset view",
    )

    t_reenrich_911 = PythonOperator(
        task_id="gold_reenrich_null_neighborhoods_911",
        python_callable=gold_reenrich_null_neighborhoods_911,
        doc_md="Fix null neighborhood_name in fact_911_calls (cold-start self-healing)",
    )

    # Silver → [dim_location, dim_offense, dim_neighborhood] → fact
    #        → [agg_category, agg_per_capita, agg_trend]
    # dim_neighborhood → reenrich_911 → [agg_911_pc, agg_profile]
    # fact             → [agg_911_pc, agg_profile]  (both gates must pass)
    t_silver >> [t_dim_loc, t_dim_off, t_dim_nbhd] >> t_fact
    t_dim_nbhd >> t_reenrich_911
    t_fact >> [t_agg_cat, t_agg_pc, t_agg_trend]
    cross_downstream([t_fact, t_reenrich_911], [t_agg_911pc, t_agg_profile])
