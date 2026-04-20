"""
Kafka Consumer — Bronze Archive
================================
Subscribes to Kafka topics and writes raw records as immutable JSON files
to the Bronze layer (flat directory, no date sub-partitioning).

Topics → Bronze paths:
  bronze_911_calls      → data/bronze/seattle_911/*.json
  bronze_crime_reports  → data/bronze/spd_crime/*.json

Design properties:
  1. Incremental   — Kafka consumer group tracks offset so only new messages
                     are consumed on restart (no full re-read).
  2. Idempotency   — Each record's unique key is tracked in a persistent
                     seen-IDs state file.  Re-delivered Kafka messages or
                     container restarts will NOT produce duplicate Bronze files.
  3. Data Quality  — Every batch is checked before writing:
                       • Missing required fields  (records are dropped + logged)
                       • In-batch duplicates      (deduped before write)
                       • Cross-run duplicates     (filtered via seen-IDs set)

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("consumer_bronze")

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
BRONZE_BASE     = os.getenv("BRONZE_DIR",  "/data/bronze")
STATE_DIR       = os.getenv("STATE_DIR",   "/data/state")

TOPIC_CONFIG = {
    "bronze_911_calls": {
        "bronze_subdir":  "seattle_911",
        "file_prefix":    "seattle_911",
        "unique_key":     "incident_number",   # dedup key
        "required_fields": ["incident_number", "datetime"],
        "state_file":     "seen_911_ids.json",
    },
    "bronze_crime_reports": {
        "bronze_subdir":  "spd_crime",
        "file_prefix":    "spd_crime",
        "unique_key":     "report_number",     # dedup key
        "required_fields": ["report_number", "report_date_time"],
        "state_file":     "seen_crime_ids.json",
    },
}

# Flush to disk after this many records are buffered per topic
FLUSH_BATCH_SIZE = 500
# Or flush after this many seconds even if batch is not full
FLUSH_INTERVAL_SECONDS = 60


# ─── Idempotency: seen-IDs state ──────────────────────────────────────────────

def _load_seen_ids(state_file: str) -> set[str]:
    """Load the set of already-ingested unique keys from disk."""
    path = os.path.join(STATE_DIR, state_file)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def _save_seen_ids(state_file: str, seen: set[str]) -> None:
    """Persist the seen-IDs set to disk after each successful flush."""
    os.makedirs(STATE_DIR, exist_ok=True)
    path = os.path.join(STATE_DIR, state_file)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(seen), f)


# ─── Data Quality ─────────────────────────────────────────────────────────────

def _data_quality_check(topic: str, records: list[dict]) -> list[dict]:
    """
    Run three DQ checks on a batch of raw records:
      1. Missing required fields  → record is dropped and logged
      2. In-batch duplicate keys  → duplicate is dropped and counted
      3. (Cross-run duplicates handled later via seen-IDs set)

    Returns the list of records that passed all checks.
    """
    cfg = TOPIC_CONFIG[topic]
    required   = cfg["required_fields"]
    unique_key = cfg["unique_key"]
    total = len(records)

    # --- Check 1: Missing required fields ---
    missing_count = 0
    after_missing: list[dict] = []
    for rec in records:
        missing = [f for f in required if not rec.get(f)]
        if missing:
            missing_count += 1
            log.warning(
                "[DQ-MISSING][%s] Dropped record — missing fields %s | id=%s",
                topic, missing, rec.get(unique_key, "unknown"),
            )
        else:
            after_missing.append(rec)

    # --- Check 2: In-batch duplicates ---
    seen_in_batch: set[str] = set()
    deduped: list[dict] = []
    dup_count = 0
    for rec in after_missing:
        key = str(rec.get(unique_key, ""))
        if key in seen_in_batch:
            dup_count += 1
        else:
            seen_in_batch.add(key)
            deduped.append(rec)

    log.info(
        "[DQ][%s] total=%d | missing=%d | in-batch-dups=%d | passed=%d",
        topic, total, missing_count, dup_count, len(deduped),
    )
    return deduped


# ─── Kafka helpers ─────────────────────────────────────────────────────────────

def _create_consumer() -> KafkaConsumer:
    """Retry connecting to Kafka until available."""
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPIC_CONFIG.keys(),
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="consumer_bronze",
                auto_offset_reset="earliest",   # start from beginning on first run
                enable_auto_commit=False,        # manual commit after successful write
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=5_000,       # unblock poll loop every 5 s
            )
            log.info("Connected to Kafka. Subscribed: %s", list(TOPIC_CONFIG.keys()))
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready — retrying in 10s...")
            time.sleep(10)


# ─── Bronze write ──────────────────────────────────────────────────────────────

def _flush_buffer(
    topic: str,
    buffer: list[dict],
    seen_ids: set[str],
) -> None:
    """
    1. Run Data Quality checks on the buffer.
    2. Filter out already-seen record IDs (cross-run idempotency).
    3. Write surviving records to a flat Bronze JSON file.
    4. Update and persist the seen-IDs set.
    """
    if not buffer:
        return

    cfg        = TOPIC_CONFIG[topic]
    unique_key = cfg["unique_key"]

    # DQ checks (missing fields + in-batch dups)
    records = _data_quality_check(topic, buffer)

    # Cross-run idempotency: drop records already in Bronze
    before  = len(records)
    records = [r for r in records if str(r.get(unique_key, "")) not in seen_ids]
    skipped = before - len(records)
    if skipped:
        log.info(
            "[IDEMPOTENCY][%s] Skipped %d already-ingested records", topic, skipped
        )

    if not records:
        return

    # Flat path: data/bronze/{subdir}/{prefix}_{utc_timestamp}.json
    out_dir = os.path.join(BRONZE_BASE, cfg["bronze_subdir"])
    os.makedirs(out_dir, exist_ok=True)

    run_ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    filepath = os.path.join(out_dir, f"{cfg['file_prefix']}_{run_ts}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)

    # Persist updated seen-IDs
    for r in records:
        seen_ids.add(str(r.get(unique_key, "")))
    _save_seen_ids(cfg["state_file"], seen_ids)

    log.info("[%s] Wrote %d records → %s", topic, len(records), filepath)


# ─── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    consumer = _create_consumer()

    # Load per-topic seen-IDs from disk (survives container restarts)
    seen_ids: dict[str, set[str]] = {
        t: _load_seen_ids(cfg["state_file"]) for t, cfg in TOPIC_CONFIG.items()
    }

    buffers: dict[str, list[dict]] = defaultdict(list)
    last_flush_time: dict[str, float] = {t: time.monotonic() for t in TOPIC_CONFIG}

    log.info("Bronze consumer started.")

    while True:
        try:
            for message in consumer:
                topic = message.topic
                buffers[topic].append(message.value)

                now = time.monotonic()
                if (
                    len(buffers[topic]) >= FLUSH_BATCH_SIZE
                    or (now - last_flush_time[topic]) >= FLUSH_INTERVAL_SECONDS
                ):
                    _flush_buffer(topic, buffers[topic], seen_ids[topic])
                    buffers[topic] = []
                    last_flush_time[topic] = now
                    consumer.commit()  # commit offset only after successful write

        except StopIteration:
            # consumer_timeout_ms elapsed — flush remaining buffers
            for topic, buf in buffers.items():
                if buf:
                    _flush_buffer(topic, buf, seen_ids[topic])
                    buffers[topic] = []
                    last_flush_time[topic] = time.monotonic()
            consumer.commit()

        except Exception as exc:
            log.error("Unexpected error: %s — restarting in 15s", exc)
            time.sleep(15)
            consumer = _create_consumer()
            buffers = defaultdict(list)


if __name__ == "__main__":
    main()
