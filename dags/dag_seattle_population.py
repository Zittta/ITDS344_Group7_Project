"""
Seattle Population Pipeline — End-to-End  (CSV → Bronze → Silver → Gold)
=========================================================================
Full medallion pipeline for Seattle Neighborhoods ACS Census data source.

Flow:
  validate_csv
      ↓
  bronze_load_population
      ↓
  silver_transform_population
      ↓
  gold_dim_demographics

Bronze:  data/raw_csv/seattle_neighborhoods_acs.csv → MongoDB bronze.seattle_population
Silver:  bronze.seattle_population → silver.silver_population_clean
Gold:    silver.silver_population_clean → gold.dim_demographics

Note: gold.dim_demographics is used by spd_crime_pipeline's
      gold_agg_crime_per_capita task. Run this pipeline first (once).

Schedule: None — manual trigger only (ACS 5-year estimates updated annually).
          Re-trigger when a new CSV is placed in data/raw_csv/.
"""
from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://mongo:27017")
BRONZE_DB        = "bronze"
SILVER_DB        = "silver"
GOLD_DB          = "gold"
CSV_PATH         = "/opt/airflow/data/raw_csv/seattle_neighborhoods_acs.csv"
ACS_YEAR         = 2024
REQUIRED_COLUMNS = {"Neighborhood Name", "Total Population"}

# Columns used by Silver — warn (not fail) if any are absent from the CSV
EXPECTED_NUMERIC_COLUMNS = {
    "Median Age",
    "Male",
    "Female",
    "Not Hispanic or Latino White alone",
    "Not Hispanic or Latino Black or African American alone",
    "Hispanic or Latino (of any race)",
    "Per Capita Income",
    "Families with income in the past 12 months below poverty level",
    "Families for whom poverty status is determined",
    "Total Housing Units",
}


# ─── Shared helpers ───────────────────────────────────────────────────────────

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


# ─── Task 1: Validate CSV ─────────────────────────────────────────────────────

def validate_csv(**context) -> dict:
    """
    DQ Rule 1: CSV file must exist and be non-empty.
    DQ Rule 2: Required columns (Neighborhood Name, Total Population) must be present.
    Pushes row/column count to XCom for downstream logging.
    """
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"Population CSV not found: {CSV_PATH}\n"
            "Place the ACS CSV at data/raw_csv/seattle_neighborhoods_acs.csv"
        )

    with open(CSV_PATH, encoding="utf-8-sig") as f:
        reader  = csv.DictReader(f)
        headers = set(reader.fieldnames or [])
        rows    = list(reader)

    if not rows:
        raise ValueError(f"CSV file is empty: {CSV_PATH}")

    missing_cols = REQUIRED_COLUMNS - headers
    if missing_cols:
        raise ValueError(
            f"CSV missing required columns: {missing_cols}\n"
            f"Available: {sorted(headers)}"
        )

    # DQ Rule 3 (Schema): warn if expected numeric columns used by Silver are absent
    missing_numeric = EXPECTED_NUMERIC_COLUMNS - headers
    if missing_numeric:
        log.warning(
            "[DQ-SCHEMA][validate-csv] %d expected numeric column(s) missing from CSV — "
            "Silver will receive None for those fields: %s",
            len(missing_numeric), sorted(missing_numeric),
        )

    log.info("CSV validation passed: %d rows, %d columns", len(rows), len(headers))
    return {"row_count": len(rows), "column_count": len(headers)}


# ─── Task 2: Bronze Load ──────────────────────────────────────────────────────

def bronze_load_population(**context) -> dict:
    """
    Loads ACS CSV → bronze.seattle_population.
    DQ Rule 3: Total Population must be numeric — non-numeric rows dropped.
    DQ Rule 4: Neighborhood Name must be non-empty — blank keys dropped.
    Idempotency: upsert on (neighborhood_name, acs_year) — re-runs are safe.
    Skips if this ACS year is already marked complete in watermarks.
    """
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db     = client[BRONZE_DB]
    coll   = db["seattle_population"]
    coll.create_index([("neighborhood_name", 1), ("acs_year", 1)], background=True)

    # Idempotency: skip if already loaded for this year
    wm = db["watermarks"].find_one({"source": "seattle_population", "layer": "bronze"})
    if wm and wm.get("acs_year") == ACS_YEAR and wm.get("status") == "complete":
        log.info("Population ACS year %d already loaded — skipping", ACS_YEAR)
        client.close()
        return {"skipped": True, "reason": f"acs_year={ACS_YEAR} already loaded"}

    now = datetime.now(timezone.utc)
    with open(CSV_PATH, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    ops        = []
    skipped_dq = 0

    for raw in rows:
        neighborhood = (raw.get("Neighborhood Name") or "").strip()
        if not neighborhood:
            skipped_dq += 1
            continue

        raw_pop = (raw.get("Total Population") or "").replace(",", "").strip()
        try:
            total_pop = int(float(raw_pop))
        except (ValueError, TypeError):
            log.warning("[DQ-POP] Non-numeric Total Population '%s' for '%s' — dropped",
                        raw_pop, neighborhood)
            skipped_dq += 1
            continue

        # DQ Rule 5 (Range): population must be positive
        if total_pop <= 0:
            log.warning("[DQ-RANGE][bronze-pop] total_population=%d ≤ 0 for '%s' — dropped",
                        total_pop, neighborhood)
            skipped_dq += 1
            continue

        doc = {
            **{k: v for k, v in raw.items()},
            "neighborhood_name": neighborhood,
            "total_population":  total_pop,
            "acs_year":          ACS_YEAR,
            "_source":           "seattle_population_csv",
            "_ingested_at":      now,
        }
        ops.append(UpdateOne(
            {"neighborhood_name": neighborhood, "acs_year": ACS_YEAR},
            {"$setOnInsert": doc, "$set": {"_last_updated_at": now}},
            upsert=True,
        ))

    upserted = _bulk_upsert(coll, ops, "bronze-population")

    db["watermarks"].update_one(
        {"source": "seattle_population", "layer": "bronze"},
        {"$set": {"acs_year": ACS_YEAR, "status": "complete",
                  "updated_at": now, "rows_loaded": len(ops)}},
        upsert=True,
    )

    log.info("Bronze population loaded: total=%d upserted=%d skipped_dq=%d",
             len(rows), upserted, skipped_dq)
    client.close()
    return {"total_rows": len(rows), "upserted": upserted, "skipped_dq": skipped_dq}


# ─── Task 3: Silver Transform ─────────────────────────────────────────────────

def silver_transform_population(**context) -> dict:
    """
    Normalises bronze.seattle_population → silver.silver_population_clean.
    DQ Rule 1: neighborhood_name must not be null.
    DQ Rule 2: total_population must be a positive integer.
    DQ Rule 3: acs_year must match expected year.
    Derives: percentage fields (male_pct, poverty_pct, race breakdowns, etc.).
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
        neighborhood = (doc.get("neighborhood_name") or "").strip()
        if not neighborhood:
            skipped_dq += 1
            continue

        if doc.get("acs_year") != ACS_YEAR:
            skipped_dq += 1
            continue

        total_pop = doc.get("total_population")
        if not isinstance(total_pop, (int, float)) or total_pop <= 0:
            skipped_dq += 1
            continue

        def _num(raw_key: str) -> Optional[float]:
            raw = str(doc.get(raw_key) or "").replace(",", "").replace("+", "").strip()
            if raw in ("", "N", "(X)", "-", "**"):
                return None
            try:
                return float(raw)
            except ValueError:
                return None

        def _pct(numerator_key: str) -> Optional[float]:
            num = _num(numerator_key)
            if num is None or total_pop <= 0:
                return None
            return round(num / total_pop * 100, 2)

        poverty_families = _num("Families with income in the past 12 months below poverty level")
        total_families   = _num("Families for whom poverty status is determined")
        poverty_pct_val  = (
            round(poverty_families / total_families * 100, 2)
            if (poverty_families is not None and total_families and total_families > 0)
            else None
        )

        # DQ Rule 4 (Range): percentage fields must be 0–100; clamp outliers to None
        def _validated_pct(val: Optional[float], label: str) -> Optional[float]:
            if val is None:
                return None
            if not 0.0 <= val <= 100.0:
                log.warning("[DQ-RANGE][silver-pop] %s=%.2f out of 0-100 for '%s' — set to None",
                            label, val, neighborhood)
                return None
            return val

        # DQ Rule 5 (Range): median_age must be positive
        median_age_val = _num("Median Age")
        if median_age_val is not None and median_age_val <= 0:
            log.warning("[DQ-RANGE][silver-pop] median_age=%.2f ≤ 0 for '%s' — set to None",
                        median_age_val, neighborhood)
            median_age_val = None

        silver_doc = {
            "neighborhood_name":       neighborhood,
            "acs_year":                ACS_YEAR,
            "total_population":        int(total_pop),
            "median_age":              median_age_val,
            "male_pct":                _validated_pct(_pct("Male"), "male_pct"),
            "female_pct":              _validated_pct(_pct("Female"), "female_pct"),
            "white_pct":               _validated_pct(_pct("Not Hispanic or Latino White alone"), "white_pct"),
            "black_pct":               _validated_pct(_pct("Not Hispanic or Latino Black or African American alone"), "black_pct"),
            "hispanic_pct":            _validated_pct(_pct("Hispanic or Latino (of any race)"), "hispanic_pct"),
            "median_household_income": _num("Per Capita Income"),
            "poverty_pct":             _validated_pct(poverty_pct_val, "poverty_pct"),
            "total_housing_units":     _num("Total Housing Units"),
            "_source":                 "seattle_population_csv",
            "_bronze_doc_id":          str(doc.get("_id", "")),
            "_silver_processed_at":    now,
        }

        ops.append(UpdateOne(
            {"neighborhood_name": neighborhood, "acs_year": ACS_YEAR},
            {"$set": silver_doc, "$setOnInsert": {"_created_at": now}},
            upsert=True,
        ))
        processed += 1

    _bulk_upsert(coll_out, ops, "silver-population")

    db_silver["watermarks"].update_one(
        {"source": "seattle_population"},
        {"$set": {"acs_year_loaded": ACS_YEAR, "updated_at": now}},
        upsert=True,
    )

    log.info("[Silver-Pop] processed=%d skipped_dq=%d", processed, skipped_dq)
    client.close()
    return {"processed": processed, "skipped_dq": skipped_dq}


# ─── Task 4: Gold — dim_demographics ─────────────────────────────────────────

def gold_dim_demographics(**context) -> dict:
    """
    Builds gold.dim_demographics from silver.silver_population_clean.
    Keyed by neighborhood_name.
    This dimension is consumed by spd_crime_pipeline's agg_crime_per_capita task.
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
                "neighborhood_name":       hood,
                "acs_year":                doc.get("acs_year"),
                "total_population":        doc.get("total_population"),
                "median_age":              doc.get("median_age"),
                "male_pct":                doc.get("male_pct"),
                "female_pct":              doc.get("female_pct"),
                "white_pct":               doc.get("white_pct"),
                "black_pct":               doc.get("black_pct"),
                "hispanic_pct":            doc.get("hispanic_pct"),
                "median_household_income": doc.get("median_household_income"),
                "poverty_pct":             doc.get("poverty_pct"),
                "total_housing_units":     doc.get("total_housing_units"),
                "_updated_at":             datetime.now(timezone.utc),
            }},
            upsert=True,
        ))

    _bulk_upsert(coll, ops, "dim_demographics")
    log.info("[Gold] dim_demographics: %d neighborhoods", len(ops))
    client.close()
    return {"neighborhoods": len(ops)}


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":           "group7",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=10),
}

with DAG(
    dag_id="seattle_population_pipeline",
    description="End-to-end population pipeline: CSV → Bronze → Silver → Gold",
    default_args=default_args,
    schedule_interval="@once",  # runs automatically once on first deploy; re-trigger manually when new CSV is placed
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pipeline", "population", "bronze", "silver", "gold"],
) as dag:

    t_validate = PythonOperator(
        task_id="validate_csv",
        python_callable=validate_csv,
        doc_md="Validate ACS CSV: file exists, non-empty, required columns present",
    )
    t_bronze = PythonOperator(
        task_id="bronze_load_population",
        python_callable=bronze_load_population,
        doc_md="Load ACS CSV → bronze.seattle_population (idempotent upsert, DQ-checked)",
    )
    t_silver = PythonOperator(
        task_id="silver_transform_population",
        python_callable=silver_transform_population,
        doc_md="Normalise bronze.seattle_population → silver.silver_population_clean",
    )
    t_gold = PythonOperator(
        task_id="gold_dim_demographics",
        python_callable=gold_dim_demographics,
        doc_md="Build gold.dim_demographics from silver_population_clean",
    )

    # validate_csv → bronze_load → silver_transform → gold_dim_demographics
    t_validate >> t_bronze >> t_silver >> t_gold