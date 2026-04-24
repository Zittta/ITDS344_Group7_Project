"""
Bronze DAG — Population CSV Ingestion  (Static CSV → MongoDB bronze)
=====================================================================
Loads the Seattle Neighborhoods ACS CSV file into MongoDB bronze layer.

  data/raw_csv/seattle_neighborhoods_acs.csv → bronze.seattle_population

Strategy:
  - Idempotency: upsert on "Neighborhood Name" — re-running is always safe.
  - State: tracks last loaded ACS year in bronze.watermarks.
  - Skips re-load if the same ACS year has already been ingested.

Data Quality checks (≥3 rules required):
  Rule 1 — File exists and is non-empty
  Rule 2 — Required columns present ("Neighborhood Name", "Total Population")
  Rule 3 — "Total Population" must be numeric (drop non-numeric rows)
  Rule 4 — "Neighborhood Name" must be non-empty (drop empty keys)

Schedule: @yearly  (ACS 5-year estimates updated annually)
           Also triggered manually when a new CSV is placed in data/raw_csv/.
"""
from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
MONGO_URI     = os.getenv("MONGO_URI", "mongodb://mongo:27017")
CSV_PATH      = "/opt/airflow/data/raw_csv/seattle_neighborhoods_acs.csv"
BRONZE_DB     = "bronze"
COLLECTION    = "seattle_population"
ACS_YEAR      = 2024

REQUIRED_COLUMNS = {"Neighborhood Name", "Total Population"}


# ─── Task 1: Validate CSV ─────────────────────────────────────────────────────

def validate_csv(**context) -> dict:
    """
    DQ Rule 1: file exists and is non-empty.
    DQ Rule 2: required columns are present.
    Returns metadata pushed to XCom for downstream tasks.
    """
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"Population CSV not found: {CSV_PATH}\n"
            "Download from https://data.seattle.gov/d/3nzs-xvkv → Export → CSV "
            f"and place at {CSV_PATH}"
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
            f"Available columns: {sorted(headers)}"
        )

    log.info("CSV validation passed: %d rows, %d columns", len(rows), len(headers))
    return {"row_count": len(rows), "column_count": len(headers), "headers": list(headers)}


# ─── Task 2: Load CSV → MongoDB bronze ────────────────────────────────────────

def load_to_bronze(**context) -> dict:
    """
    DQ Rule 3: 'Total Population' must be numeric — non-numeric rows dropped.
    DQ Rule 4: 'Neighborhood Name' must be non-empty — blank keys dropped.

    Idempotency: upsert on (neighborhood_name, acs_year).
    Skips re-load if this ACS year is already marked complete in watermarks.
    """
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db     = client[BRONZE_DB]

    # Check watermark — skip if already loaded
    wm = db["watermarks"].find_one({"source": "seattle_population", "layer": "bronze"})
    if wm and wm.get("acs_year") == ACS_YEAR and wm.get("status") == "complete":
        log.info("Population ACS year %d already loaded — skipping", ACS_YEAR)
        client.close()
        return {"skipped": True, "reason": f"acs_year={ACS_YEAR} already loaded"}

    now = datetime.now(timezone.utc)

    with open(CSV_PATH, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    collection = db[COLLECTION]
    collection.create_index([("neighborhood_name", 1), ("acs_year", 1)], background=True)

    ops          = []
    skipped_dq   = 0
    upserted     = 0

    for raw in rows:
        neighborhood = (raw.get("Neighborhood Name") or "").strip()

        # DQ Rule 4: non-empty neighborhood name
        if not neighborhood:
            skipped_dq += 1
            continue

        # DQ Rule 3: Total Population must be numeric
        raw_pop = (raw.get("Total Population") or "").replace(",", "").strip()
        try:
            total_pop = int(float(raw_pop))
        except (ValueError, TypeError):
            log.warning("[DQ-POP] Non-numeric Total Population '%s' for '%s' — dropped",
                        raw_pop, neighborhood)
            skipped_dq += 1
            continue

        # Build doc — preserve all original fields plus metadata
        doc = {
            **{k: v for k, v in raw.items()},
            "neighborhood_name":  neighborhood,
            "total_population":   total_pop,
            "acs_year":           ACS_YEAR,
            "_source":            "seattle_population_csv",
            "_ingested_at":       now,
        }

        ops.append(UpdateOne(
            {"neighborhood_name": neighborhood, "acs_year": ACS_YEAR},
            {"$setOnInsert": doc, "$set": {"_last_updated_at": now}},
            upsert=True,
        ))

    # Bulk upsert
    if ops:
        try:
            result = collection.bulk_write(ops, ordered=False)
            upserted = result.upserted_count
        except BulkWriteError as exc:
            log.warning("BulkWriteError (non-fatal): %d errors",
                        len(exc.details.get("writeErrors", [])))

    # Advance watermark
    db["watermarks"].update_one(
        {"source": "seattle_population", "layer": "bronze"},
        {"$set": {"acs_year": ACS_YEAR, "status": "complete",
                  "updated_at": now, "rows_loaded": len(ops)}},
        upsert=True,
    )

    log.info("Population loaded: total=%d upserted=%d skipped_dq=%d",
             len(rows), upserted, skipped_dq)
    client.close()
    return {"total_rows": len(rows), "upserted": upserted, "skipped_dq": skipped_dq}


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":           "group7",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=10),
}

with DAG(
    dag_id="bronze_population_ingest",
    description="Load Seattle Neighborhoods ACS CSV into MongoDB bronze (manual trigger only)",
    default_args=default_args,
    schedule_interval=None,   # static CSV — trigger manually when new file is placed
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "population", "static", "incremental"],
) as dag:

    t_validate = PythonOperator(
        task_id="validate_csv",
        python_callable=validate_csv,
        doc_md="Validate CSV file: existence, non-empty, required columns present",
    )

    t_load = PythonOperator(
        task_id="load_to_bronze_mongodb",
        python_callable=load_to_bronze,
        doc_md="Upsert CSV rows into bronze.seattle_population (idempotent, DQ-checked)",
    )

    t_validate >> t_load
