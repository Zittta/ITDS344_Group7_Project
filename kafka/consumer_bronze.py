"""
Kafka Consumer — Bronze Archive (MongoDB)
=========================================
Subscribes to Kafka topics and upserts raw records into MongoDB bronze layer.

  bronze_911_calls      → MongoDB: bronze.seattle_911
  bronze_crime_reports  → MongoDB: bronze.spd_crime

Design properties:
  1. Incremental   — Kafka consumer group offset ensures only new messages
                     are consumed on restart (no full re-read).
  2. Idempotency   — MongoDB upsert with $setOnInsert on business key:
                     re-delivered messages will NOT create duplicate documents.
  3. Data Quality  — Each batch is checked before write:
                       • Missing required fields  (records dropped + logged)
                       • In-batch duplicates      (deduped before write)
  4. Metadata      — Every document gains: _ingested_at, _source, _last_seen_at
  5. Streaming     — Runs continuously as Docker service (Bonus requirement)

Run: docker service (continuous loop, restarts automatically)
"""
from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("consumer_bronze")

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB        = "bronze"

TOPIC_CONFIG = {
    "bronze_911_calls": {
        "collection":      "seattle_911",
        "unique_key":      "incident_number",
        "required_fields": ["incident_number", "datetime"],
        "source_name":     "seattle_911",
        "timestamp_field": "datetime",
        "lat_field":       "latitude",
        "lon_field":       "longitude",
    },
    "bronze_crime_reports": {
        "collection":      "spd_crime",
        "unique_key":      "offense_id",
        "required_fields": ["offense_id", "report_date_time"],
        "source_name":     "spd_crime",
        "timestamp_field": "report_date_time",
        "lat_field":       "latitude",
        "lon_field":       "longitude",
    },
}

FLUSH_BATCH_SIZE       = 500
FLUSH_INTERVAL_SECONDS = 60


# ─── MongoDB helpers ───────────────────────────────────────────────────────────

def _get_mongo_db():
    """Connect to MongoDB and ensure indexes exist on unique key fields."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db = client[MONGO_DB]
    for cfg in TOPIC_CONFIG.values():
        db[cfg["collection"]].create_index(cfg["unique_key"], background=True)
    log.info("Connected to MongoDB: %s / %s", MONGO_URI, MONGO_DB)
    return client, db


# ─── DQ helpers ───────────────────────────────────────────────────────────────

def _try_parse_dt(val) -> bool:
    """Return True if val is a recognisable ISO-8601 timestamp string."""
    if not val:
        return False
    s = str(val).strip().rstrip("Z")
    if len(s) > 19 and s[19] in ("+", "-"):
        s = s[:19]
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            datetime.strptime(s, fmt)
            return True
        except ValueError:
            continue
    return False


def _try_float(val):
    """Parse val to float; return None on failure."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ─── Data Quality ─────────────────────────────────────────────────────────────

def _data_quality_check(topic: str, records: list[dict]) -> list[dict]:
    """
    DQ Rule 1 (Null)   : required fields must be non-empty — record dropped if any missing.
    DQ Rule 2 (Dup)    : in-batch dedup on unique_key — later duplicate dropped.
    DQ Rule 3 (Schema) : timestamp field must be parseable ISO-8601 — record dropped.
    DQ Rule 4 (Range)  : lat/lon outside WGS84 bounds — logged as warning (not dropped at bronze).
    Returns only records that passed Rules 1-3.
    """
    cfg        = TOPIC_CONFIG[topic]
    required   = cfg["required_fields"]
    unique_key = cfg["unique_key"]
    ts_field   = cfg.get("timestamp_field")
    lat_field  = cfg.get("lat_field")
    lon_field  = cfg.get("lon_field")
    total      = len(records)

    # DQ Rule 1 (Null): Missing required fields
    missing_count = 0
    after_missing: list[dict] = []
    for rec in records:
        missing = [f for f in required if not rec.get(f)]
        if missing:
            missing_count += 1
            log.warning("[DQ-MISSING][%s] dropped — missing %s | key=%s",
                        topic, missing, rec.get(unique_key, "?"))
        else:
            after_missing.append(rec)

    # DQ Rule 2 (Duplicate): In-batch dedup
    seen_keys: dict[str, dict] = {}
    dup_count = 0
    for rec in after_missing:
        key = str(rec.get(unique_key, ""))
        if key in seen_keys:
            dup_count += 1
        else:
            seen_keys[key] = rec
    deduped = list(seen_keys.values())

    # DQ Rule 3 (Schema): timestamp field must be parseable
    schema_count = 0
    after_schema: list[dict] = []
    if ts_field:
        for rec in deduped:
            ts_val = rec.get(ts_field, "")
            if not _try_parse_dt(ts_val):
                schema_count += 1
                log.warning("[DQ-SCHEMA][%s] Unparseable %s='%s' for key=%s — dropped",
                            topic, ts_field, ts_val, rec.get(unique_key, "?"))
            else:
                after_schema.append(rec)
    else:
        after_schema = deduped

    # DQ Rule 4 (Range): lat/lon WGS84 — warning only, not dropped at bronze layer
    range_warn = 0
    if lat_field and lon_field:
        for rec in after_schema:
            lat = _try_float(rec.get(lat_field))
            lon = _try_float(rec.get(lon_field))
            if lat is not None and not -90 <= lat <= 90:
                range_warn += 1
                log.warning("[DQ-RANGE][%s] lat=%.4f out of WGS84 range for key=%s",
                            topic, lat, rec.get(unique_key, "?"))
            if lon is not None and not -180 <= lon <= 180:
                range_warn += 1
                log.warning("[DQ-RANGE][%s] lon=%.4f out of WGS84 range for key=%s",
                            topic, lon, rec.get(unique_key, "?"))

    log.info(
        "[DQ][%s] total=%d | missing=%d | in-batch-dups=%d | schema_err=%d | range_warn=%d | passed=%d",
        topic, total, missing_count, dup_count, schema_count, range_warn, len(after_schema),
    )
    return after_schema


# ─── Kafka helper ─────────────────────────────────────────────────────────────

def _create_consumer() -> KafkaConsumer:
    """Retry connecting to Kafka until available."""
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPIC_CONFIG.keys(),
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="consumer_bronze_mongo",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=5_000,
            )
            log.info("Connected to Kafka. Subscribed: %s", list(TOPIC_CONFIG.keys()))
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready — retrying in 10s...")
            time.sleep(10)


# ─── MongoDB flush (replaces old file-based flush) ────────────────────────────

def _flush_to_mongo(db, topic: str, buffer: list[dict]) -> None:
    """
    1. Run DQ checks on buffer.
    2. Upsert surviving records into MongoDB bronze collection.
       $setOnInsert ensures idempotency: already-inserted docs are not overwritten.
       $set _last_seen_at is always updated so we can track re-deliveries.
    """
    if not buffer:
        return

    cfg        = TOPIC_CONFIG[topic]
    unique_key = cfg["unique_key"]
    collection = db[cfg["collection"]]

    records = _data_quality_check(topic, buffer)
    if not records:
        return

    now = datetime.now(timezone.utc)
    ops = []
    for rec in records:
        doc_on_insert = {
            **rec,
            "_source":      cfg["source_name"],
            "_ingested_at": now,        # set only on first insert
        }
        ops.append(UpdateOne(
            {unique_key: rec[unique_key]},
            {
                "$setOnInsert": doc_on_insert,
                "$set":         {"_last_seen_at": now},
            },
            upsert=True,
        ))

    try:
        result = collection.bulk_write(ops, ordered=False)
        log.info("[MONGO][%s] upserted=%d  matched(existing)=%d",
                 topic, result.upserted_count, result.matched_count)
    except BulkWriteError as exc:
        log.warning("[MONGO][%s] BulkWriteError: %d write errors (non-fatal)",
                    topic, len(exc.details.get("writeErrors", [])))


# ─── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    mongo_client, db = _get_mongo_db()
    consumer = _create_consumer()

    buffers:    dict[str, list[dict]] = defaultdict(list)
    last_flush: dict[str, float]      = {t: time.monotonic() for t in TOPIC_CONFIG}

    log.info("Bronze consumer (MongoDB) started.")

    while True:
        try:
            for message in consumer:
                topic = message.topic
                buffers[topic].append(message.value)

                now      = time.monotonic()
                due_size = len(buffers[topic]) >= FLUSH_BATCH_SIZE
                due_time = (now - last_flush[topic]) >= FLUSH_INTERVAL_SECONDS

                if due_size or due_time:
                    _flush_to_mongo(db, topic, buffers[topic])
                    buffers[topic] = []
                    last_flush[topic] = now
                    consumer.commit()

        except StopIteration:
            for topic, buf in buffers.items():
                if buf:
                    _flush_to_mongo(db, topic, buf)
                    buffers[topic] = []
                    last_flush[topic] = time.monotonic()
            consumer.commit()

        except Exception as exc:
            log.error("Unexpected error: %s — restarting in 15s", exc, exc_info=True)
            time.sleep(15)
            try:
                mongo_client, db = _get_mongo_db()
                consumer = _create_consumer()
            except Exception:
                pass
            buffers = defaultdict(list)


if __name__ == "__main__":
    main()
