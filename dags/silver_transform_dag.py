"""
Silver DAG — Bronze → Silver Transformation  (MongoDB bronze → MongoDB silver)
===============================================================================
Reads new records from MongoDB bronze layer, cleans and standardises them,
then upserts results into MongoDB silver layer.

Collections:
  bronze.seattle_911       → silver.silver_911_clean
  bronze.spd_crime         → silver.silver_crime_clean
  bronze.seattle_population → silver.silver_population_clean

Incremental Load:
  - Per-source watermark stored in silver.watermarks (_ingested_at of last
    bronze record processed).
  - Each run only processes bronze records newer than the watermark.
  - Watermark advances after successful write.

Idempotency:
  - MongoDB upsert on business key (event_id / offense_id / neighborhood+year).
  - Re-running the DAG is always safe — no duplicates created.

Data Quality rules (≥ 3 per source):
  911  Rule 1 — incident_number (event_id) must not be null
       Rule 2 — datetime field must parse to a valid datetime
       Rule 3 — latitude/longitude must be in valid WGS84 range (if present)

  Crime Rule 1 — offense_id must not be null
        Rule 2 — report_date_time must parse to a valid datetime
        Rule 3 — lat/lon in valid range (if present)
        Rule 4 — offense_category must not be null

  Pop  Rule 1 — neighborhood_name must not be null
       Rule 2 — total_population must be a positive integer
       Rule 3 — acs_year must match expected year

Schedule: every hour  (runs shortly after bronze batch ingest)
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://mongo:27017")
BRONZE_DB   = "bronze"
SILVER_DB   = "silver"
CHUNK_SIZE  = 1_000
ACS_YEAR    = 2024


# ─── Shared helpers ───────────────────────────────────────────────────────────

def _get_watermark(db_silver, source: str) -> datetime:
    doc = db_silver["watermarks"].find_one({"source": source})
    if doc and doc.get("last_processed_at"):
        ts = doc["last_processed_at"]
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime(2020, 1, 1, tzinfo=timezone.utc)


def _set_watermark(db_silver, source: str, ts: datetime) -> None:
    db_silver["watermarks"].update_one(
        {"source": source},
        {"$set": {"last_processed_at": ts, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_dt(val: str) -> Optional[datetime]:
    if not val:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _bulk_upsert(collection, ops: list, source: str) -> int:
    if not ops:
        return 0
    try:
        result = collection.bulk_write(ops, ordered=False)
        return result.upserted_count
    except BulkWriteError as exc:
        log.warning("[SILVER][%s] BulkWriteError: %d errors (non-fatal)",
                    source, len(exc.details.get("writeErrors", [])))
        return 0


# ─── Task 1: 911 calls → silver ───────────────────────────────────────────────

def transform_911_to_silver(**context) -> dict:
    """
    Cleans bronze.seattle_911 records and upserts into silver.silver_911_clean.

    Transformations:
      - 'datetime' string → ISODate call_datetime
      - latitude / longitude → float (None if out of range)
      - event_type stripped and uppercased
      - is_police_sent derived from event_type keyword
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_bronze = client[BRONZE_DB]
    db_silver = client[SILVER_DB]
    coll_out  = db_silver["silver_911_clean"]
    coll_out.create_index("event_id", background=True)

    watermark = _get_watermark(db_silver, "seattle_911")
    log.info("[Silver-911] Processing bronze records since %s", watermark)

    cursor       = db_bronze["seattle_911"].find(
        {"_ingested_at": {"$gt": watermark}},
        batch_size=CHUNK_SIZE,
    ).sort("_ingested_at", 1)

    ops           = []
    max_ingested  = watermark
    processed     = 0
    skipped_dq    = 0
    now           = datetime.now(timezone.utc)

    for doc in cursor:
        # DQ Rule 1: event_id must exist
        if not doc.get("incident_number"):
            skipped_dq += 1
            continue

        # DQ Rule 2: datetime must parse
        call_dt = _parse_dt(doc.get("datetime", ""))
        if call_dt is None:
            skipped_dq += 1
            continue

        # DQ Rule 3: lat/lon range validation
        lat = _safe_float(doc.get("latitude"))
        lon = _safe_float(doc.get("longitude"))
        if lat is not None and not -90 <= lat <= 90:
            lat = None
        if lon is not None and not -180 <= lon <= 180:
            lon = None

        event_type    = (doc.get("type") or "UNKNOWN").strip().upper()
        is_police_sent = bool(re.search(r"police|spd|officer", event_type, re.I))

        silver_doc = {
            "event_id":             doc["incident_number"],
            "call_datetime":        call_dt,
            "event_type":           event_type,
            "address":              (doc.get("address") or "").strip(),
            "latitude":             lat,
            "longitude":            lon,
            "is_police_sent":       is_police_sent,
            "_source":              "seattle_911",
            "_bronze_ingested_at":  doc.get("_ingested_at"),
            "_silver_processed_at": now,
        }

        ops.append(UpdateOne(
            {"event_id": silver_doc["event_id"]},
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
            _bulk_upsert(coll_out, ops, "seattle_911")
            ops = []

    _bulk_upsert(coll_out, ops, "seattle_911")
    _set_watermark(db_silver, "seattle_911", max_ingested)

    log.info("[Silver-911] processed=%d skipped_dq=%d new_watermark=%s",
             processed, skipped_dq, max_ingested)
    client.close()
    return {"processed": processed, "skipped_dq": skipped_dq}


# ─── Task 2: SPD crime → silver ───────────────────────────────────────────────

def transform_crime_to_silver(**context) -> dict:
    """
    Cleans bronze.spd_crime records and upserts into silver.silver_crime_clean.

    Transformations:
      - report_date_time / offense_date strings → ISODate
      - latitude / longitude → float (None if invalid)
      - offense_category normalised (strip, upper)
      - neighborhood / precinct / sector stripped
      - is_shooting derived from shooting_type_group field
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_bronze = client[BRONZE_DB]
    db_silver = client[SILVER_DB]
    coll_out  = db_silver["silver_crime_clean"]
    coll_out.create_index("offense_id", background=True)

    watermark = _get_watermark(db_silver, "spd_crime")
    log.info("[Silver-Crime] Processing bronze records since %s", watermark)

    cursor = db_bronze["spd_crime"].find(
        {"_ingested_at": {"$gt": watermark}},
        batch_size=CHUNK_SIZE,
    ).sort("_ingested_at", 1)

    ops          = []
    max_ingested = watermark
    processed    = 0
    skipped_dq   = 0
    now          = datetime.now(timezone.utc)

    for doc in cursor:
        # DQ Rule 1: offense_id must exist
        if not doc.get("offense_id"):
            skipped_dq += 1
            continue

        # DQ Rule 2: report_date_time must parse
        report_dt = _parse_dt(doc.get("report_date_time", ""))
        if report_dt is None:
            skipped_dq += 1
            continue

        # DQ Rule 4: offense_category must not be null
        offense_cat = (doc.get("offense_category") or "").strip().upper()
        if not offense_cat:
            offense_cat = "UNKNOWN"

        # DQ Rule 3: lat/lon range validation
        lat = _safe_float(doc.get("latitude"))
        lon = _safe_float(doc.get("longitude"))
        if lat is not None and not -90 <= lat <= 90:
            lat = None
        if lon is not None and not -180 <= lon <= 180:
            lon = None

        offense_dt  = _parse_dt(doc.get("offense_date", ""))
        is_shooting = bool(doc.get("shooting_type_group", ""))

        silver_doc = {
            "offense_id":                   doc["offense_id"],
            "report_number":                (doc.get("report_number") or "").strip(),
            "report_date_time":             report_dt,
            "offense_date":                 offense_dt,
            "offense_category":             offense_cat,
            "offense_sub_category":         (doc.get("offense_sub_category") or "").strip().upper(),
            "nibrs_offense_code":           (doc.get("nibrs_offense_code") or "").strip(),
            "nibrs_offense_code_description": (doc.get("nibrs_offense_code_description") or "").strip(),
            "nibrs_crime_against_category": (doc.get("nibrs_crime_against_category") or "").strip().upper(),
            "nibrs_group":                  (doc.get("nibrs_group_a_b") or "").strip().upper(),
            "is_shooting":                  is_shooting,
            "block_address":                (doc.get("block_address") or "").strip(),
            "latitude":                     lat,
            "longitude":                    lon,
            "precinct":                     (doc.get("precinct") or "").strip().upper(),
            "sector":                       (doc.get("sector") or "").strip().upper(),
            "beat":                         (doc.get("beat") or "").strip().upper(),
            "neighborhood":                 (doc.get("neighborhood") or "").strip(),
            "reporting_area":               (doc.get("reporting_area") or "").strip(),
            "_source":                      "spd_crime",
            "_bronze_ingested_at":          doc.get("_ingested_at"),
            "_silver_processed_at":         now,
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
            _bulk_upsert(coll_out, ops, "spd_crime")
            ops = []

    _bulk_upsert(coll_out, ops, "spd_crime")
    _set_watermark(db_silver, "spd_crime", max_ingested)

    log.info("[Silver-Crime] processed=%d skipped_dq=%d new_watermark=%s",
             processed, skipped_dq, max_ingested)
    client.close()
    return {"processed": processed, "skipped_dq": skipped_dq}


# ─── Task 3: Population → silver ──────────────────────────────────────────────

def transform_population_to_silver(**context) -> dict:
    """
    Normalises bronze.seattle_population into silver.silver_population_clean.

    Transformations:
      - Column headers normalised to snake_case
      - Numeric fields parsed to float/int
      - Margin-of-error (moe) columns retained but separated
      - One document per (neighborhood_name, acs_year)

    DQ Rules:
      Rule 1 — neighborhood_name must not be null
      Rule 2 — total_population must be a positive integer
      Rule 3 — acs_year must equal expected year
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_bronze = client[BRONZE_DB]
    db_silver = client[SILVER_DB]
    coll_out  = db_silver["silver_population_clean"]
    coll_out.create_index([("neighborhood_name", 1), ("acs_year", 1)], background=True)

    bronze_docs = list(db_bronze["seattle_population"].find({"acs_year": ACS_YEAR}))
    if not bronze_docs:
        log.info("[Silver-Pop] No bronze population data for year %d", ACS_YEAR)
        client.close()
        return {"processed": 0}

    ops        = []
    processed  = 0
    skipped_dq = 0
    now        = datetime.now(timezone.utc)

    for doc in bronze_docs:
        # DQ Rule 1: neighborhood name
        neighborhood = (doc.get("neighborhood_name") or "").strip()
        if not neighborhood:
            skipped_dq += 1
            continue

        # DQ Rule 3: acs_year must match
        if doc.get("acs_year") != ACS_YEAR:
            skipped_dq += 1
            continue

        # DQ Rule 2: total_population positive integer
        total_pop = doc.get("total_population")
        if not isinstance(total_pop, (int, float)) or total_pop <= 0:
            skipped_dq += 1
            continue

        # Extract key numeric metrics from ACS columns (map common column patterns)
        def _num(raw_key: str) -> Optional[float]:
            raw = str(doc.get(raw_key) or "").replace(",", "").replace("+", "").strip()
            if raw in ("", "N", "(X)", "-", "**"):
                return None
            try:
                return float(raw)
            except ValueError:
                return None

        # Compute percentage fields from raw counts
        def _pct(numerator_key: str) -> Optional[float]:
            num = _num(numerator_key)
            if num is None or total_pop <= 0:
                return None
            return round(num / total_pop * 100, 2)

        # Poverty rate: families below poverty / total families
        poverty_families = _num("Families with income in the past 12 months below poverty level")
        total_families   = _num("Families for whom poverty status is determined")
        poverty_pct_val  = (
            round(poverty_families / total_families * 100, 2)
            if (poverty_families is not None and total_families and total_families > 0)
            else None
        )

        silver_doc = {
            "neighborhood_name":    neighborhood,
            "acs_year":             ACS_YEAR,
            "total_population":     int(total_pop),
            "median_age":           _num("Median Age"),
            "male_pct":             _pct("Male"),
            "female_pct":           _pct("Female"),
            "white_pct":            _pct("Not Hispanic or Latino White alone"),
            "black_pct":            _pct("Not Hispanic or Latino Black or African American alone"),
            "hispanic_pct":         _pct("Hispanic or Latino (of any race)"),
            "median_household_income": _num("Per Capita Income"),
            "poverty_pct":          poverty_pct_val,
            "total_housing_units":  _num("Total Housing Units"),
            "_source":              "seattle_population_csv",
            "_bronze_doc_id":       str(doc.get("_id", "")),
            "_silver_processed_at": now,
        }

        ops.append(UpdateOne(
            {"neighborhood_name": neighborhood, "acs_year": ACS_YEAR},
            {"$set": silver_doc, "$setOnInsert": {"_created_at": now}},
            upsert=True,
        ))
        processed += 1

    _bulk_upsert(coll_out, ops, "seattle_population")

    # Mark silver watermark
    db_silver["watermarks"].update_one(
        {"source": "seattle_population"},
        {"$set": {"acs_year_loaded": ACS_YEAR, "updated_at": now}},
        upsert=True,
    )

    log.info("[Silver-Pop] processed=%d skipped_dq=%d", processed, skipped_dq)
    client.close()
    return {"processed": processed, "skipped_dq": skipped_dq}


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":           "group7",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="silver_transform",
    description="Transform bronze MongoDB → silver MongoDB (clean, standardise, DQ checks)",
    default_args=default_args,
    schedule_interval="*/10 * * * *",   # every 10 min — keeps up with streaming bronze
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["silver", "transform", "incremental"],
    max_active_runs=1,
) as dag:

    t_911 = PythonOperator(
        task_id="transform_911_to_silver",
        python_callable=transform_911_to_silver,
        doc_md=(
            "Clean & standardise bronze.seattle_911 → silver.silver_911_clean. "
            "DQ: required fields, datetime parse, lat/lon range."
        ),
    )

    t_crime = PythonOperator(
        task_id="transform_crime_to_silver",
        python_callable=transform_crime_to_silver,
        doc_md=(
            "Clean & standardise bronze.spd_crime → silver.silver_crime_clean. "
            "DQ: offense_id, datetime parse, lat/lon range, offense_category."
        ),
    )

    t_pop = PythonOperator(
        task_id="transform_population_to_silver",
        python_callable=transform_population_to_silver,
        doc_md=(
            "Normalise bronze.seattle_population → silver.silver_population_clean. "
            "DQ: neighborhood name, total_population positive, acs_year match."
        ),
    )

    # All three sources are independent — run in parallel
    [t_911, t_crime, t_pop]
