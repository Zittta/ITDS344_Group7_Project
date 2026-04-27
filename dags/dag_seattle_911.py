"""
Seattle 911 Pipeline — End-to-End  (Socrata API → Bronze → Silver → Gold)
=========================================================================
Full medallion pipeline for Seattle Real-Time Fire 911 Calls data source.

Flow:
  bronze_ingest_911
      ↓
  silver_transform_911
      ↓  [parallel]
  gold_dim_time ──────┐
  gold_dim_event_type ┤
      ↓  [both done] ─┘
  gold_fact_911_calls
      ↓
  gold_agg_911_by_hour_day

Bronze:  Socrata API (kzjm-xkqj) → MongoDB bronze.seattle_911
Silver:  bronze.seattle_911 → silver.silver_911_clean
Gold:    silver.silver_911_clean → gold.dim_time, gold.dim_event_type,
                                    gold.fact_911_calls, gold.agg_911_by_hour_day

Note: gold_dim_time upserts are idempotent — safe to run concurrently with
      spd_crime_pipeline which also upserts into gold.dim_time.

Schedule: every 5 minutes (incremental watermark-based load)
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
    "api_url":            "https://data.seattle.gov/resource/kzjm-xkqj.json",
    "timestamp_field":    "datetime",
    "unique_key":         "incident_number",
    "required_fields":    ["incident_number", "datetime"],
    "initial_start_date": "2024-01-01T00:00:00",
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
    doc = db["watermarks"].find_one({"source": "seattle_911", "layer": "bronze"})
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
        {"source": "seattle_911", "layer": "bronze"},
        {"$set": {"last_ingested_dt": dt, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


# ── Silver watermark ──────────────────────────────────────────────────────────

def _get_silver_wm(db_silver) -> datetime:
    doc = db_silver["watermarks"].find_one({"source": "seattle_911"})
    if doc and doc.get("last_processed_at"):
        ts = doc["last_processed_at"]
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime(2020, 1, 1, tzinfo=timezone.utc)


def _set_silver_wm(db_silver, ts: datetime) -> None:
    db_silver["watermarks"].update_one(
        {"source": "seattle_911"},
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

def bronze_ingest_911(**context) -> dict:
    """
    Consumes new 911 call records from Kafka topic 'bronze_911_calls'.
    DQ Rule 1: required fields (incident_number, datetime) must be non-empty.
    DQ Rule 2: timestamp field must be present and parseable.
    Idempotency: upsert on incident_number — re-runs are safe.
    """
    from kafka import KafkaConsumer
    import json

    cfg    = SOURCE_CFG
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db     = client[BRONZE_DB]
    db["seattle_911"].create_index(cfg["unique_key"], background=True)

    consumer = KafkaConsumer(
        "bronze_911_calls",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        group_id="bronze_ingest_911",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    buffer = []
    total_upserted = 0
    skipped_dq = 0
    ts_field = cfg["timestamp_field"]
    for message in consumer:
        rec = message.value
        # DQ check
        if any(not rec.get(f) for f in cfg["required_fields"]):
            skipped_dq += 1
            continue
        if not rec.get(ts_field):
            skipped_dq += 1
            continue
        buffer.append(rec)
        if len(buffer) >= UPSERT_CHUNK:
            now = datetime.now(timezone.utc)
            ops = [
                UpdateOne(
                    {cfg["unique_key"]: r[cfg["unique_key"]]},
                    {
                        "$setOnInsert": {**r, "_source": "seattle_911", "_ingested_at": now},
                        "$set": {"_last_seen_at": now},
                    },
                    upsert=True,
                )
                for r in buffer
            ]
            total_upserted += _bulk_upsert(db["seattle_911"], ops, "bronze-911")
            buffer = []
    # Flush remaining
    if buffer:
        now = datetime.now(timezone.utc)
        ops = [
            UpdateOne(
                {cfg["unique_key"]: r[cfg["unique_key"]]},
                {
                    "$setOnInsert": {**r, "_source": "seattle_911", "_ingested_at": now},
                    "$set": {"_last_seen_at": now},
                },
                upsert=True,
            )
            for r in buffer
        ]
        total_upserted += _bulk_upsert(db["seattle_911"], ops, "bronze-911")
    client.close()
    return {"upserted": total_upserted, "skipped_dq": skipped_dq}


# ─── Task 2: Silver Transform ─────────────────────────────────────────────────

def silver_transform_911(**context) -> dict:
    """
    Cleans bronze.seattle_911 → silver.silver_911_clean.
    DQ Rule 1: incident_number must not be null.
    DQ Rule 2: datetime must parse to a valid ISO datetime.
    DQ Rule 3: lat/lon must be in valid WGS84 range if present.
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_bronze = client[BRONZE_DB]
    db_silver = client[SILVER_DB]
    coll_out  = db_silver["silver_911_clean"]
    coll_out.create_index("event_id", background=True)

    watermark = _get_silver_wm(db_silver)
    log.info("[Silver-911] Processing bronze records since %s", watermark)

    cursor      = db_bronze["seattle_911"].find(
        {"_ingested_at": {"$gt": watermark}},
        batch_size=CHUNK_SIZE,
    ).sort("_ingested_at", 1)

    ops          = []
    max_ingested = watermark
    processed    = 0
    skipped_dq   = 0
    now          = datetime.now(timezone.utc)

    for doc in cursor:
        if not doc.get("incident_number"):
            skipped_dq += 1
            continue

        call_dt = _parse_dt(doc.get("datetime", ""))
        if call_dt is None:
            skipped_dq += 1
            continue

        lat = _safe_float(doc.get("latitude"))
        lon = _safe_float(doc.get("longitude"))
        if lat is not None and not -90 <= lat <= 90:
            lat = None
        if lon is not None and not -180 <= lon <= 180:
            lon = None

        event_type = (doc.get("type") or "UNKNOWN").strip().upper()

        silver_doc = {
            "event_id":             doc["incident_number"],
            "call_datetime":        call_dt,
            "event_type":           event_type,
            "address":              (doc.get("address") or "").strip(),
            "latitude":             lat,
            "longitude":            lon,
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
            _bulk_upsert(coll_out, ops, "silver-911")
            ops = []

    _bulk_upsert(coll_out, ops, "silver-911")
    _set_silver_wm(db_silver, max_ingested)
    log.info("[Silver-911] processed=%d skipped_dq=%d watermark=%s",
             processed, skipped_dq, max_ingested)
    client.close()
    return {"processed": processed, "skipped_dq": skipped_dq}


# ─── Task 3: Gold — fact_911_calls ───────────────────────────────────────────

def gold_fact_911(**context) -> dict:
    """
    Builds gold.fact_911_calls from silver.silver_911_clean.
    Incremental load via gold watermark. Joins dim_time + dim_event_type.
    """
    client    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_silver = client[SILVER_DB]
    db_gold   = client[GOLD_DB]
    coll      = db_gold["fact_911_calls"]
    coll.create_index("event_id", unique=True, background=True)

    watermark = _get_gold_wm(db_gold, "fact_911_calls")
    et_idx    = {
        d["event_type"]: d["event_type_id"]
        for d in db_gold["dim_event_type"].find({}, {"event_type_id": 1, "event_type": 1})
    }

    cursor     = db_silver["silver_911_clean"].find(
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

        call_dt = doc.get("call_datetime")
        time_id = int(call_dt.strftime("%Y%m%d%H")) if isinstance(call_dt, datetime) else None
        et_id   = et_idx.get(doc.get("event_type", ""))

        fact_doc = {
            "event_id":             event_id,
            "time_id":              time_id,
            "event_type_id":        et_id,
            "event_type":           doc.get("event_type", ""),
            "call_datetime":        call_dt,
            "address":              doc.get("address", ""),
            "latitude":             doc.get("latitude"),
            "longitude":            doc.get("longitude"),
            "_silver_processed_at": doc.get("_silver_processed_at"),
            "_gold_loaded_at":      now,
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
    _set_gold_wm(db_gold, "fact_911_calls", max_silver)
    log.info("[Gold] fact_911_calls: processed=%d", processed)
    client.close()
    return {"processed": processed}


# ─── Task 5: Gold — agg_911_by_hour_day ──────────────────────────────────────

def gold_agg_911(**context) -> dict:
    """Pre-computes 911 call volume by hour × day-of-week (full rebuild each run)."""
    client  = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db_gold = client[GOLD_DB]

    pipeline = [
        {"$match": {"call_datetime": {"$ne": None}}},
        {"$addFields": {
            "hour":        {"$hour": "$call_datetime"},
            "day_of_week": {"$dayOfWeek": "$call_datetime"},   # 1=Sun … 7=Sat
        }},
        {"$group": {
            "_id":        {"hour": "$hour", "day_of_week": "$day_of_week"},
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
    db_gold["fact_911_calls"].aggregate(pipeline)
    count = db_gold["agg_911_by_hour_day"].count_documents({})
    log.info("[Gold] agg_911_by_hour_day: %d rows", count)
    client.close()
    return {"agg_911_by_hour_day": count}


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":           "group7",
    "depends_on_past": False,
    "retries":         3,
    "retry_delay":     timedelta(minutes=2),
}

with DAG(
    dag_id="seattle_911_pipeline",
    description="End-to-end 911 pipeline: Bronze → Silver → Gold",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["pipeline", "911", "bronze", "silver", "gold"],
    max_active_runs=1,
) as dag:

    t_bronze = PythonOperator(
        task_id="bronze_ingest_911",
        python_callable=bronze_ingest_911,
        doc_md="Fetch new 911 records from Socrata API → bronze.seattle_911 (watermark-based)",
    )
    t_silver = PythonOperator(
        task_id="silver_transform_911",
        python_callable=silver_transform_911,
        doc_md="Clean & standardise bronze.seattle_911 → silver.silver_911_clean",
    )
    t_fact = PythonOperator(
        task_id="gold_fact_911_calls",
        python_callable=gold_fact_911,
        doc_md="Build gold.fact_911_calls — incremental fact table",
    )
    t_agg = PythonOperator(
        task_id="gold_agg_911_by_hour_day",
        python_callable=gold_agg_911,
        doc_md="Materialise agg_911_by_hour_day (full rebuild)",
    )

    # Bronze → Silver → fact_911_calls → agg
    t_bronze >> t_silver >> t_fact >> t_agg
