# ITDS344 Group 7 — Seattle Public Safety Analytics Pipeline

**End-to-End Data Engineering Project**  
ITDS344 Data Engineering and Infrastructures (Semester 2/2568)

---

## Project Overview

ระบบ Data Engineering แบบ Medallion Architecture (Bronze → Silver → Gold) สำหรับวิเคราะห์ Public Safety ของเมือง Seattle โดยรวมข้อมูล 911 Calls, Crime Reports และ ACS Demographics เข้าด้วยกัน

**Business Goals:**
- วิเคราะห์ Crime Trends และ Hotspots ตามย่านและช่วงเวลา
- คำนวณอัตราอาชญากรรมต่อประชากร (per-capita crime rate)
- เปรียบเทียบความถี่ 911 calls กับจำนวน crime reports
- สนับสนุน data-driven decision making สำหรับการจัดสรรทรัพยากร

---

## Data Sources

| # | Dataset | Source | Type | Records (ปัจจุบัน) |
|---|---------|--------|------|-------------------|
| 1 | Seattle Real-Time Fire 911 Calls | [data.seattle.gov](https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj) | Socrata API | ~282,000 (all pass DQ) |
| 2 | SPD Crime Data 2008-Present | [data.seattle.gov](https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5) | Socrata API | ~184,000 raw → ~170,000 clean |
| 3 | Seattle Neighborhoods ACS (2024) | `data/raw_csv/seattle_neighborhoods_acs.csv` | CSV | 87 neighborhoods |

**Note:** Crime data ลดลงหลัง DQ filtering (~14K rows dropped: invalid offense_category, neighborhood, etc.)

---

## Architecture

ระบบแบ่งเป็น 3 Pipeline อิสระ ตาม 3 แหล่งข้อมูล แต่ละ Pipeline ครอบคลุม Bronze → Silver → Gold ในตัวเอง

```
──────────────────── Pipeline 1: Seattle 911 (every 5 min) ────────────────────
Socrata API (911) ──► bronze_ingest_911 ──► silver_transform_911
  (streaming also:       [bronze.seattle_911]   [silver.silver_911_clean]
Kafka → consumer)              │
                               ▼
                    gold_fact_911_calls
                               ▼
                    gold_agg_911_by_hour_day

**Tasks: 4** (bronze → silver → fact → agg)  
**Duration: ~30-40 sec/run**

──────────────────── Pipeline 2: SPD Crime (every 60 min) ─────────────────────
Socrata API (crime) ──► bronze_ingest_crime ──► silver_transform_crime
  (streaming also:        [bronze.spd_crime]     [silver.silver_crime_clean]
Kafka → consumer)              │                 (with data quality filters)
                               ▼
          ┌── gold_dim_location ──┬── gold_dim_offense ──┐
          └────────────────────────┴──────────────────────┘
                               ▼
                    gold_fact_crime_events
                               ▼
          ┌── gold_agg_crime_by_category ────────────────┐
          └── gold_agg_crime_per_capita (uses dim_demographics)

**Tasks: 6** (bronze → silver → [2 dims] → fact → [2 aggs])  
**Duration: ~90-120 sec/run**

**Data Quality Rules:**
- Skip rows with invalid offense_category: UNKNOWN, NOT_A_CRIME, REDACTED, OOJ, "-", or values starting with "99"
- Skip rows with invalid neighborhood: UNKNOWN, "-", REDACTED, OOJ, or values starting with "99"
- is_shooting: True only if shooting_type_group contains "Shots Fired" or "Shooting" (case-insensitive)

──────────────── Pipeline 3: Seattle Population (run @once on deploy) ─────────
CSV ──► validate_csv ──► bronze_load_population ──► silver_transform_population
  [data/raw_csv/]   [bronze.seattle_population]   [silver.silver_population_clean]
                               ▼
                    gold_dim_demographics
                   (ใช้โดย pipeline 2 — agg_crime_per_capita)

**Tasks: 4** (validate → bronze → silver → gold)  
**Duration: ~5-10 sec** (87 neighborhoods only)  
**Schedule: @once** (runs once on deploy, re-trigger manually when CSV updates)
```

**Streaming Path (Kafka):** Kafka Producer สำรองดึงข้อมูล 911 + Crime จาก Socrata แบบ real-time
ส่งผ่าน Kafka → Consumer เข้า MongoDB bronze collections เดียวกัน (upsert idempotent)

| Service | Port | Login | Purpose |
|---------|------|-------|---------|
| Airflow UI | http://localhost:8080 | admin/admin | Monitor DAG pipelines |
| Mongo Express | http://localhost:8081 | admin/admin | Browse MongoDB collections |
| MongoDB | localhost:27017 | - | Direct connection |
| Kafka | localhost:9092 | - | Message broker |
| Streamlit Dashboard | http://localhost:8501 | - | Analytics dashboard (run separately) |

---

## Project Structure

```
ITDS344_Group7_Project/
├── dags/
│   ├── dag_seattle_911.py          # Pipeline 1: 911  Bronze→Silver→Gold (every 5 min, 4 tasks)
│   ├── dag_spd_crime.py            # Pipeline 2: Crime Bronze→Silver→Gold (every 60 min, 6 tasks)
│   └── dag_seattle_population.py   # Pipeline 3: Pop  Bronze→Silver→Gold (@once, 4 tasks)
├── dashboard/
│   ├── dashboard.py                # Streamlit analytics dashboard
│   ├── requirements.txt            # Dashboard dependencies (streamlit, plotly, pymongo)
│   └── assets/
│       └── style.css               # Custom CSS styling
├── kafka/
│   ├── kafka_producer.py           # Streaming: Poll Socrata API → Kafka topics
│   ├── consumer_bronze.py          # Streaming: Kafka → MongoDB bronze (upsert)
│   └── Dockerfile
├── data/
│   ├── raw_csv/
│   │   └── seattle_neighborhoods_acs.csv  # ✅ tracked in git
│   └── state/
│       ├── kafka_911_state.json    # Kafka consumer state for 911
│       └── kafka_crime_state.json  # Kafka consumer state for crime
├── docker-compose.yml
├── .env                            # ❌ NOT in git — ต้องสร้างเอง (ดูด้านล่าง)
├── requirements.txt
├── DATA_STRUCTURE.md               # รายละเอียด schema ทุก collection + NIBRS reference
└── README.md
```

---

## Quick Start (สำหรับ clone แล้วรันครั้งแรก)

### Prerequisites
- **Docker Desktop** (Windows/Mac) หรือ Docker Engine (Linux) — version 24+
- RAM แนะนำ **≥ 8 GB** (Airflow + MongoDB + Kafka ใช้ ~4–5 GB)
- ไม่ต้องติดตั้ง Python, MongoDB, Kafka เพราะทุกอย่างอยู่ใน Docker

---

### Step 1 — Clone & เข้า folder

```powershell
git clone <repository-url>
cd ITDS344_Group7_Project
```

---

### Step 2 — สร้าง `.env` file

`.env` ไม่อยู่ใน git เพราะเก็บ secret — ต้องสร้างเอง:

```powershell
# Windows PowerShell
Copy-Item .env.example .env   # ถ้ามี .env.example
# หรือสร้างใหม่เลย
```

เนื้อหาของ `.env` (copy ไปวาง แล้วแก้ token):

```ini
# Socrata App Token — ดูวิธีสมัครด้านล่าง (REQUIRED for production rate limits)
SOCRATA_APP_TOKEN=ใส่_token_ของคุณ

# Airflow
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.

# PostgreSQL (Airflow metadata DB)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Admin
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# MongoDB
MONGO_URI=mongodb://mongo:27017
```

**วิธีขอ Socrata App Token (ฟรี):**
1. ไปที่ https://data.seattle.gov → Sign In
2. My Profile → App Tokens → Create New App Token
3. Copy token มาใส่ใน `.env`

> ⚠️ ถ้าไม่มี token ก็ยังรันได้ แต่ Socrata จะ rate-limit ที่ ~100 req/hr

---

### Step 3 — ตรวจสอบ CSV มีอยู่แล้ว

```powershell
Test-Path data\raw_csv\seattle_neighborhoods_acs.csv
# ควรได้ True — ไฟล์นี้ track ใน git อยู่แล้ว
```

---

### Step 4 — รัน Docker

```powershell
# ครั้งแรก: build images + start ทุก service
docker compose -f docker-compose.yml up --build -d

# ดูว่าทุก container healthy ไหม (รอ ~2 นาที)
docker ps

# หรือดู log airflow-init (ควรจบด้วย exit 0)
docker compose logs airflow-init
```

> **Linux users:** ถ้า Airflow volume permission error ให้รัน `echo "AIRFLOW_UID=$(id -u)" >> .env` ก่อน

---

### Step 5 — เปิด UI และตรวจสอบ DAGs

เข้า **http://localhost:8080** (admin / admin)

ควรเห็น 3 DAGs สถานะ **ON** (ไม่ต้อง toggle):

| DAG | Schedule | หมายเหตุ |
|-----|----------|----------|
| `seattle_population_pipeline` | `@once` | รันอัตโนมัติครั้งแรก ✅ (ต้องเสร็จก่อน pipeline อื่น) |
| `seattle_911_pipeline` | ทุก 5 นาที | รันอัตโนมัติ ✅ |
| `spd_crime_pipeline` | ทุก 60 นาที | รันอัตโนมัติ ✅ |

> **สำคัญ:** `seattle_population_pipeline` จะรันอัตโนมัติ 1 ครั้งเมื่อ deploy (`@once`)
> Pipeline นี้สร้าง `dim_demographics` ที่ `spd_crime_pipeline` ใช้สำหรับคำนวณ `agg_crime_per_capita`

**ถ้าต้องการ trigger ด้วยมือ** (เช่น วาง CSV ใหม่):

```powershell
# Trigger population pipeline (วาง CSV ใหม่ก่อน แล้วค่อย trigger)
docker exec itds344_group7_project-airflow-scheduler-1 `
  airflow dags trigger seattle_population_pipeline

# Trigger 911 pipeline ทันที (ไม่ต้องรอ schedule)
docker exec itds344_group7_project-airflow-scheduler-1 `
  airflow dags trigger seattle_911_pipeline

# Trigger crime pipeline ทันที
docker exec itds344_group7_project-airflow-scheduler-1 `
  airflow dags trigger spd_crime_pipeline
```

---

### Step 6 — ตรวจสอบข้อมูล

เข้า **http://localhost:8081** (Mongo Express) แล้วดู database:

| MongoDB DB | Collection | Expected Records | Notes |
|------------|-----------|------------------|-------|
| `bronze` | `seattle_911` | ~282,000 | Raw 911 dispatch data |
| `bronze` | `spd_crime` | ~184,000 | Raw crime reports |
| `bronze` | `seattle_population` | 87 | ACS demographics by neighborhood |
| `silver` | `silver_911_clean` | ~282,000 | Cleaned 911 data |
| `silver` | `silver_crime_clean` | ~170,000 | Cleaned crime data (with DQ filters) |
| `gold` | `fact_crime_events` | ~170,000 | Crime fact table (optimized fields) |
| `gold` | `fact_911_calls` | ~282,000 | 911 calls fact table |
| `gold` | `dim_location` | 283 | Location dimension (precinct/sector/beat/neighborhood) |
| `gold` | `dim_offense` | 61 | Offense dimension (NIBRS codes) |
| `gold` | `dim_demographics` | 87 | Demographics dimension (used by per-capita calc) |
| `gold` | `agg_911_by_hour_day` | 168 | 911 calls by hour × day (24 × 7) |
| `gold` | `agg_crime_by_offense_category` | 84 | Crime counts by category × month |
| `gold` | `agg_crime_per_capita` | 87 | Crime rate per 10K population by neighborhood |

หรือตรวจสอบผ่าน terminal:

```powershell
docker exec itds344_group7_project-mongo-1 mongosh --quiet --eval "
  use('bronze');
  print('911:', db.seattle_911.countDocuments());
  print('crime:', db.spd_crime.countDocuments());
  print('pop:', db.seattle_population.countDocuments());
  use('silver');
  print('silver_911:', db.silver_911_clean.countDocuments());
  print('silver_crime:', db.silver_crime_clean.countDocuments());
  use('gold');
  print('fact_crime:', db.fact_crime_events.countDocuments());
  print('fact_911:', db.fact_911_calls.countDocuments());
"
```

---

## Useful Commands

```powershell
# หยุดทุก service (เก็บ data ไว้)
docker compose -f docker-compose.yml down

# หยุดและลบ data ทั้งหมด (reset สมบูรณ์)
docker compose -f docker-compose.yml down -v

# ดู logs แบบ live
docker compose logs -f airflow-scheduler
docker compose logs -f kafka-producer
docker compose logs -f kafka-consumer-bronze

# รัน DAG task เดี่ยวๆ (debug)
docker exec itds344_group7_project-airflow-scheduler-1 `
  airflow tasks test silver_transform transform_911_to_silver 2026-04-24

# เข้า MongoDB shell
docker exec -it itds344_group7_project-mongo-1 mongosh
```

---

## Troubleshooting

| อาการ | สาเหตุ | วิธีแก้ |
|-------|--------|--------|
| DAG ไม่ขึ้นใน Airflow | DAG file มี syntax error | `docker compose logs airflow-scheduler` |
| Silver ว่างทั้งที่ bronze มีข้อมูล | Bronze ยัง ingest ไม่เสร็จ | รอ 2 นาที แล้ว trigger silver อีกครั้ง |
| `service "mongo" is not running` | รัน command ผิด directory | ต้องรัน `docker compose` จาก folder ที่มี `docker-compose.yml` |
| Permission denied (Linux) | AIRFLOW_UID ไม่ตรง | `echo "AIRFLOW_UID=$(id -u)" >> .env` แล้ว `docker compose up -d` |
| Port 8080 ถูกใช้อยู่ | มี service อื่น | แก้ `AIRFLOW_WEBSERVER_PORT` ใน `.env` |

---

## Data Schema & Dashboard Guide

ดูรายละเอียด schema ทุก collection และคำแนะนำ Dashboard ได้ที่ [DATA_STRUCTURE.md](DATA_STRUCTURE.md)

---

## 📊 Dashboard Access

ระบบมี **Interactive Analytics Dashboard** สร้างด้วย Streamlit เพื่อ visualize ข้อมูลแบบ real-time

### เปิดใช้งาน Dashboard

1. **ติดตั้ง Dependencies:**
   ```powershell
   cd dashboard
   pip install -r requirements.txt
   ```

2. **รัน Streamlit Server:**
   ```powershell
   streamlit run dashboard.py
   ```

3. **เข้าใช้งาน:** เปิด Browser ไปที่ **http://localhost:8501**

### Dashboard Features

Dashboard แบ่งเป็น **6 หัวข้อหลัก**:

#### 1. **Overview (KPI Cards)**
- Total Crimes (YTD)
- Total 911 Calls (YTD)
- Total Shooting Incidents
- Police Dispatched

#### 2. **Performance Metrics (Gauge Charts)**
- Shooting Rate (% of crimes involving shootings)
- Police Response Rate (% calls dispatched to police)
- Average Crime Rate per 10,000 Population

#### 3. **Crime Against Category**
- การกระทำความผิดแบ่งตามเหยื่อ: PERSON / PROPERTY / SOCIETY
- แสดงทั้ง count และ percentage ในรูปแบบ 3×2 grid

#### 4. **Crime Trends by NIBRS Category**
- แยกตาม NIBRS classification (Group A/B, Crime Against)
- Stacked bar charts แสดง distribution

#### 5. **Interactive Map Visualization**
- **Heat map:** 911 call density (background layer)
- **Bubble map:** Crime count by neighborhood (foreground layer)
- เลือกดูแยกได้ว่าจะเห็น Crime / 911 Calls / หรือทั้งคู่

#### 6. **911 Dispatch Heatmap**
- วันในสัปดาห์ (Monday-Sunday) × ชั่วโมงในวัน (0-23)
- สีเข้มขึ้น = ความถี่การโทรมากขึ้น
- ช่วงเวลาที่ยุ่งที่สุด: **Friday 14:00-16:00**

### Time Filters

Dashboard รองรับการกรองข้อมูลตามเวลา 4 รูปแบบ:
- **Today:** ข้อมูลวันนี้อย่างเดียว
- **Specific Date:** เลือกวันที่ต้องการ
- **Month:** เลือกเดือน (เช่น April 2026)
- **Year:** ข้อมูลทั้งปี (เช่น 2026)

### Data Refresh

- Dashboard ดึงข้อมูลจาก **MongoDB Gold Layer** (localhost:27017)
- ใช้ `@st.cache_data(ttl=300)` — cache อายุ 5 นาที
- กด **R** (refresh) ใน browser เพื่อโหลดข้อมูลใหม่

### Requirements

- **MongoDB:** ต้อง running อยู่ที่ `localhost:27017`
- **Python:** 3.9+
- **RAM:** ~500 MB สำหรับ Streamlit server
- **Browser:** Chrome, Firefox, Edge (รองรับ modern browsers)

---**Business Goals:**
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