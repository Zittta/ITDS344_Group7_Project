# ITDS344 Group 7 — Seattle Public Safety Analytics Pipeline

**End-to-End Data Engineering Project**  
ITDS344 Data Engineering and Infrastructures (Semester 2/2568)

---

## Project Overview

สร้างระบบ Data Engineering แบบครบวงจร (Bronze → Silver → Gold) สำหรับการวิเคราะห์ Public Safety ของเมือง Seattle โดยรวมข้อมูล 911 Calls, Crime Reports และ Demographics เข้าด้วยกัน เพื่อสร้าง Dashboard แสดงสถิติอาชญากรรมแบบ Real-time

**Business Goals:**
- วิเคราะห์ Crime Trends และ Hotspots ตามย่านและช่วงเวลา
- คำนวณอัตราอาชญากรรมต่อประชากร (per-capita crime rate)
- เปรียบเทียบความถี่ 911 calls กับจำนวน crime reports
- Support data-driven decision making สำหรับการจัดเจ้าหน้าที่ลาดตระเวน

---

## Data Sources (3 แหล่ง)

| # | Dataset | Source | Type | Update Frequency | Records |
|---|---------|--------|------|------------------|---------|
| 1 | **Seattle Real-Time Fire 911 Calls** | [data.seattle.gov](https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj) | Socrata API | Every 5 minutes | ~8.7k (30 days) |
| 2 | **SPD Crime Data: 2008-Present** | [data.seattle.gov](https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5) | Socrata API | Daily | ~183k (since 2024-01-01) |
| 3 | **Seattle Neighborhoods ACS Population** | [data.seattle.gov](https://data.seattle.gov/d/3nzs-xvkv) | CSV file | Yearly (ACS 2024) | 95 neighborhoods |

**Join Key:** `neighborhood` field (Community Reporting Areas) — ใช้ร่วมกันได้ทั้ง 3 datasets

---

## Architecture

### Medallion Architecture (Bronze → Silver → Gold) + Kafka Streaming

```
 Socrata API (911, Crime)              ACS CSV (Population)
  updates every 5 min                   updates yearly
        │                                     │
        ▼                                     ▼
┌───────────────────────┐       ┌─────────────────────────────────┐
│  kafka_producer.py    │       │  ingestion_seattle_population   │
│  (Docker service)     │       │  (Airflow DAG — @yearly)        │
│  poll API → publish   │       │  copy CSV → Bronze              │
└──────────┬────────────┘       └──────────────┬──────────────────┘
           │ Kafka Topics                       │
           │ bronze_911_calls                   │
           │ bronze_crime_reports               │
      ┌────┴────┐                               │
      ▼         ▼                               │
┌──────────┐  ┌──────────────┐                  │
│ consumer │  │   consumer   │                  │
│ _bronze  │  │   _silver    │◄─────────────────┘
│ (Docker) │  │   (Docker)   │  (reads silver_population.csv)
└──────────┘  └──────┬───────┘
      │              │
      ▼              ▼
data/bronze/    data/silver/
(immutable      silver_911_calls.csv
 archive)       silver_crime_reports.csv      [TODO: Phase 2]
                silver_population.csv
                       │
                       ▼
        ┌──────────────────────────────────┐
        │  gold_warehouse_dag              │
        │  (Airflow DAG — @daily)  [TODO]  │
        │  Hive External Tables            │
        │  → dim_neighborhood, dim_date    │  [TODO: Phase 2]
        │  → fact_crime, fact_911          │
        │  → gold_per_capita_crime_rate    │
        │  → gold_calls_vs_crime           │
        └──────────────────────────────────┘
                       │
                       ▼
        ┌──────────────────────────────────┐
        │  DATA PRODUCT             [TODO] │
        │  Dashboard / API          Phase 3│
        └──────────────────────────────────┘
```

**เครื่องมือ Big Data ที่ใช้:**
| เครื่องมือ | Layer | บทบาท |
|-----------|-------|-------|
| **Apache Kafka** | Bronze | Streaming ingestion จาก Socrata API แบบ real-time |
| **Apache Hive** | Gold | Star Schema + HiveQL analytics queries |

---

## Project Structure

```
.
├── bronze-dag/                              # [Bronze] Airflow DAG (static data)
│   └── ingestion_seattle_population_dag.py     # ACS Population (yearly, @yearly)
│
├── bronze-streaming/                         # [Bronze] Kafka Streaming (real-time data)
│   ├── Dockerfile                              # Shared image สำหรับทุก streaming service
│   ├── kafka_producer.py                       # Poll Socrata API → publish to Kafka
│   └── consumer_bronze.py                      # Consume Kafka → archive to Bronze
│
├── silver/                                  # [Silver] Transform + Clean layer
│   └── consumer_silver.py                      # Consume Kafka → transform → Silver [TODO]
│
├── gold/                                    # [Gold] Data Warehouse (Hive)
│   └── TODO.py                                 # สิ่งที่ต้องทำสำหรับ Gold layer [TODO]
│
├── data/
│   ├── bronze/                              # Raw data (immutable archive)
│   │   ├── seattle_911/*.json                  # via bronze-streaming/consumer_bronze (flat)
│   │   ├── spd_crime/*.json                    # via bronze-streaming/consumer_bronze (flat)
│   │   └── seattle_population/*.csv            # via bronze-dag Airflow DAG
│   ├── silver/                              # Cleaned data [TODO]
│   ├── gold/                                # Data warehouse [TODO]
│   ├── raw_csv/                             # Seed CSVs (tracked in git)
│   └── state/                               # Watermark + seen-IDs state files
│
├── logs/                                    # Airflow execution logs
├── plugins/                                 # Custom Airflow operators
├── docker-compose.yml                       # All services (Airflow + Kafka + Hive)
├── .env                                     # Environment variables (secrets)
├── .gitignore
├── requirements.txt
└── README.md

```

---

## Setup Instructions

### Prerequisites
- Docker Desktop (Windows/Mac) หรือ Docker Engine (Linux)
- Python 3.12+ (for development/testing)

### 1. Clone Repository
```powershell
git clone <repository-url>
cd ITDS344_Group7_Project
```

### 2. ตั้งค่า Environment Variables
```powershell
# แก้ไข .env file ใส่ API token ของคุณ
notepad .env
```

```ini
# ใน .env file
SOCRATA_APP_TOKEN=your_token_here   # ดูวิธีสมัครด้านล่าง
AIRFLOW_UID=50000
```

**วิธีขอ Socrata App Token:**
1. ไปที่ https://data.seattle.gov
2. Sign In (สร้าง account ใหม่)
3. My Profile → App Tokens → Create New Token

### 3. ดาวน์โหลด Population CSV
```powershell
# ดาวน์โหลดจาก data.seattle.gov/d/3nzs-xvkv → Export → CSV
# บันทึกไปที่
data/raw_csv/seattle_neighborhoods_acs.csv
```

### 4. รัน Services ทั้งหมด
```powershell
# First-time setup (สร้าง Airflow DB + admin user)
docker compose up airflow-init

# Start all services
# (Airflow, Kafka, Zookeeper, kafka-producer, kafka-consumer-bronze)
docker compose up -d

# ตรวจสอบว่าทุก service รันอยู่
docker compose ps
```

### 5. ตรวจสอบ Kafka Streaming
```powershell
# ดู logs ของ producer (ควรเห็น "Published X records")
docker compose logs -f kafka-producer

# ดู logs ของ bronze consumer (ควรเห็น "Wrote X records")
docker compose logs -f kafka-consumer-bronze

# ตรวจสอบไฟล์ที่ถูกสร้างใน Bronze
Get-ChildItem data/bronze -Recurse -Filter *.json | Select-Object FullName, Length
```

### 6. Trigger Population DAG (manual)
ใน Airflow Web UI (http://localhost:8080):
1. Toggle เปิด DAG `ingestion_seattle_population`
2. คลิก ▶ (Trigger DAG)

หรือ CLI:
```powershell
docker compose exec airflow-scheduler airflow dags trigger ingestion_seattle_population
```

---

## Services Overview

### Kafka Streaming Services (Docker — รันตลอด 24/7)

| Service | รันแบบ | หน้าที่ | Poll Interval |
|---------|--------|--------|---------------|
| `kafka-producer` | continuous loop | Poll Socrata API → publish to Kafka | 911: 5 นาที / Crime: 1 ชม. |
| `kafka-consumer-bronze` | continuous loop | Consume Kafka → archive JSON → Bronze | real-time |
| `kafka-consumer-silver` | continuous loop | Consume Kafka → transform → Silver | real-time \[TODO\] |

**Features:**
- ✅ Watermark tracking — `data/state/kafka_911_state.json` / `kafka_crime_state.json`
- ✅ Idempotency — seen-IDs state file (`seen_911_ids.json` / `seen_crime_ids.json`), manual Kafka offset commit หลัง write สำเร็จ
- ✅ Data Quality — missing fields check, in-batch dedup, cross-run dedup
- ✅ Auto-restart on crash (`restart: always`)
- ✅ Offset pagination (50k records/page)

### Airflow DAGs

| DAG ID | Schedule | Description |
|--------|----------|-------------|
| `ingestion_seattle_population` | `@yearly` | Copy ACS CSV → Bronze → Silver |
| `gold_warehouse_dag` | `@daily` | Hive Star Schema → Gold tables \[TODO\] |

**Features:**
- ✅ Retry 3 ครั้ง
- ✅ Idempotency check
- ✅ Logging ครบทุก task

---

## Technical Features Implemented

### Bronze Layer — ✅ เสร็จแล้ว

| Feature | Status | Implementation |
|---------|--------|----------------|
| **Real-time Streaming** | ✅ | Kafka Producer polls Socrata API ทุก 5 นาที |
| **Incremental Load** | ✅ | Watermark state files + `$where` timestamp filter |
| **Idempotency** | ✅ | seen-IDs state file (incident_number / report_number) + manual Kafka offset commit |
| **Data Quality** | ✅ | Missing fields check, in-batch dedup, cross-run dedup |
| **Flat Storage** | ✅ | Bronze เก็บ JSON flat ใน `data/bronze/{topic}/` ไม่ partition ตามวันที่ |
| **Batch Pagination** | ✅ | 50k records/page + offset loop |
| **Error Handling** | ✅ | try/except + Docker `restart: always` |
| **Logging** | ✅ | Python `logging` module ทุก service |
| **Multiple Data Formats** | ✅ | JSON (API via Kafka) + CSV (static file via Airflow) |
| **Big Data Tool: Kafka** | ✅ | Streaming backbone สำหรับ 911 + Crime data |

### Silver Layer — 🚧 TODO
- [x] `consumer_silver.py` — โครงสร้าง + TODO comments พร้อมแล้ว
- [ ] Implement: deduplication (by report_number / incident_number)
- [ ] Implement: null drop + schema validation
- [ ] Implement: standardize timestamps + neighborhood names
- [ ] Implement: append to Silver CSV
- [ ] Add `kafka-consumer-silver` service ใน docker-compose.yml

### Gold Layer (Hive) — 📋 TODO
- [ ] Hive External Tables ชี้ที่ Silver CSV
- [ ] Star Schema: dim_neighborhood, dim_date
- [ ] Fact Tables: fact_crime_reports, fact_911_incidents
- [ ] Gold Aggregates: per-capita crime rate, calls vs crime
- [ ] **Big Data Tool: Hive** — HiveQL analytics

### Orchestration — ✅ Running
- ✅ Kafka streaming (continuous, Docker services)
- ✅ Airflow scheduler (Population DAG @yearly)
- [ ] Gold DAG (Airflow @daily) — TODO
- [ ] Monitoring & alerting

---

## Current Status

**Phase 2: Data Ingestion & Warehouse**  
Progress: **50% Complete**

- [x] ✅ Bronze Layer — Kafka streaming (911 + Crime) + Airflow (Population)
- [x] ✅ Real-time ingestion via Kafka Producer/Consumer
- [x] ✅ Incremental watermark tracking
- [x] ✅ Logging & error handling
- [ ] 🚧 Silver Layer — consumer_silver.py + transformations
- [ ] 📋 Gold Layer — Hive Star Schema
- [ ] 📋 Data Product — Dashboard/API

**Bronze Layer Statistics:**
```
data/bronze/seattle_911/        — 8,742 records (2.2 MB JSON)
data/bronze/spd_crime/          — 183,089 records (110.46 MB JSON)
data/bronze/seattle_population/ — 95 neighborhoods (0.074 MB CSV)
```

---

## Troubleshooting

### Permission Error ใน logs/
```powershell
# แก้ไข permission
docker compose exec -u root airflow-scheduler chown -R airflow:root /opt/airflow/logs /opt/airflow/data
docker compose restart airflow-scheduler airflow-webserver
```

### DAG ไม่แสดงใน UI
```powershell
# ตรวจสอบ syntax error
docker compose logs airflow-scheduler | grep -i "error\|broken"

# Force refresh DAGs
docker compose restart airflow-scheduler
```

### ลืม admin password
```powershell
# สร้าง user ใหม่
docker compose exec airflow-scheduler airflow users create \
  --username admin2 --password admin2 --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

---

## Team

**Group 7**
- Sitta Silakhett 6687054
- Kittikhun Puangsuwan 6680759
- Yanaphat Jumpaburee 6687112 

---

## License

Academic project for ITDS344 — Mahidol University ICT  
Data sources: City of Seattle Open Data Portal (Public Domain)