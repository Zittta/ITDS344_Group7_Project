"""
SPD Crime Pipeline — End-to-End  (Socrata API → Bronze → Silver → Gold)
========================================================================
Full medallion pipeline for Seattle Police Department Crime data source.

Flow:
  bronze_ingest_crime
      ↓
  silver_transform_crime
      ↓  [parallel]
  gold_dim_time ─────────┐
  gold_dim_location ─────┤
  gold_dim_offense ──────┘
      ↓  [all done]
  gold_fact_crime_events
      ↓  [parallel]
  gold_agg_crime_by_neighborhood ─┐
  gold_agg_crime_by_category ─────┤
  gold_agg_crime_per_capita ──────┘

Bronze:  Socrata API (tazs-3rd5) → MongoDB bronze.spd_crime
Silver:  bronze.spd_crime → silver.silver_crime_clean
Gold:    silver.silver_crime_clean → gold.dim_time, gold.dim_location,
                                      gold.dim_offense, gold.fact_crime_events,
                                      aggregations

Note: gold_dim_time upserts are idempotent — safe to run concurrently with
      seattle_911_pipeline which also upserts into gold.dim_time.
Note: gold_agg_crime_per_capita joins with gold.dim_demographics which is
      built by seattle_population_pipeline (trigger that pipeline first).

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

# Neighborhood name mapping: SPD UPPERCASE → ACS Title Case
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


# ─── Task 1: Bronze Ingestion ─────────────────────────────────────────────────

def bronze_ingest_crime(**context) -> dict:
    """
    Fetches new crime records from Socrata API (watermark-based incremental).
    DQ Rule 1: required fields (offense_id, report_date_time) must be non-empty.
    DQ Rule 2: timestamp field must be present.
    Idempotency: upsert on offense_id — re-runs are safe.
    """
    cfg    = SOURCE_CFG
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db     = client[BRONZE_DB]
    db["spd_crime"].create_index(cfg["unique_key"], background=True)

    watermark = _get_bronze_wm(db)
    ts_field  = cfg["timestamp_field"]
    log.info("[Bronze-Crime] Fetching records since %s", watermark)

    headers     = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    all_records: list[dict] = []
    offset      = 0

    while True:
        params = {
            "$where":  f"{ts_field} > '{watermark}'",
            "$limit":  BATCH_SIZE,
            "$offset": offset,
            "$order":  f"{ts_field} ASC",
        }
        try:
            resp = requests.get(cfg["api_url"], headers=headers, params=params, timeout=120)
            resp.raise_for_status()
            batch: list[dict] = resp.json()
        except requests.RequestException as exc:
            log.error("[Bronze-Crime] API error: %s", exc)
            client.close()
            raise

        if not batch:
            break
        all_records.extend(batch)
        log.info("[Bronze-Crime] page=%d total_so_far=%d", len(batch), len(all_records))
        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    if not all_records:
        log.info("[Bronze-Crime] No new records")
        client.close()
        return {"fetched": 0, "skipped_dq": 0}

    # DQ validation
    valid, skipped_dq = [], 0
    for rec in all_records:
        if any(not rec.get(f) for f in cfg["required_fields"]):
            skipped_dq += 1
            continue
        if not rec.get(ts_field):
            skipped_dq += 1
            continue
        valid.append(rec)
    log.info("[Bronze-Crime] DQ: total=%d valid=%d skipped=%d",
             len(all_records), len(valid), skipped_dq)

    # Upsert in chunks
    collection     = db["spd_crime"]
    now            = datetime.now(timezone.utc)
    total_upserted = 0
    for i in range(0, len(valid), UPSERT_CHUNK):
        chunk = valid[i:i + UPSERT_CHUNK]
        ops   = [
            UpdateOne(
                {cfg["unique_key"]: rec[cfg["unique_key"]]},
                {
                    "$setOnInsert": {**rec, "_source": "spd_crime", "_ingested_at": now},
                    "$set":         {"_last_seen_at": now},
                },
                upsert=True,
            )
            for rec in chunk
        ]
        total_upserted += _bulk_upsert(collection, ops, "bronze-crime")

    max_dt = max(r[ts_field] for r in valid if r.get(ts_field))
    _set_bronze_wm(db, max_dt)
    log.info("[Bronze-Crime] Done: fetched=%d upserted=%d skipped_dq=%d watermark=%s",
             len(all_records), total_upserted, skipped_dq, max_dt)
    client.close()
    return {"fetched": len(all_records), "upserted": total_upserted, "skipped_dq": skipped_dq}


# ─── Task 2: Silver Transform ─────────────────────────────────────────────────

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

    # Map SPD names → ACS names
    crime_counts_by_pop: dict[str, int] = {}
    for crime_hood, count in crime_counts_raw.items():
        pop_hood = CRIME_TO_POP.get(crime_hood)
        if pop_hood:
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


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":           "group7",
    "depends_on_past": False,
    "retries":         3,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="spd_crime_pipeline",
    description="End-to-end crime pipeline: Bronze → Silver → Gold",
    default_args=default_args,
    schedule_interval="0 * * * *",   # every 60 minutes
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["pipeline", "crime", "bronze", "silver", "gold"],
    max_active_runs=1,
) as dag:

    t_bronze = PythonOperator(
        task_id="bronze_ingest_crime",
        python_callable=bronze_ingest_crime,
        doc_md="Fetch new SPD crime records from Socrata API → bronze.spd_crime",
    )
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

    # Bronze → Silver → [dim_location, dim_offense] → fact → [agg x2]
    t_bronze >> t_silver >> [t_dim_loc, t_dim_off] >> t_fact >> [t_agg_cat, t_agg_pc]
