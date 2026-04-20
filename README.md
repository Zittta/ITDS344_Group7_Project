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

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Raw Data - Immutable)                             │
├─────────────────────────────────────────────────────────────────┤
│ • seattle_911/*.json          — Fire 911 calls (JSON from API)  │
│ • spd_crime/*.json            — Crime reports (JSON from API)   │
│ • seattle_population/*.csv    — ACS demographics (CSV)          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ SILVER LAYER (Cleaned & Transformed)          [TODO: Phase 2]  │
├─────────────────────────────────────────────────────────────────┤
│ • Deduplication (drop_duplicates by primary key)                │
│ • Data Quality checks (null/outlier/schema validation)          │
│ • Standardize timestamps, geocodes, categories                  │
│ • Merge incremental batches → single consolidated table         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ GOLD LAYER (Data Warehouse)                   [TODO: Phase 2]  │
├─────────────────────────────────────────────────────────────────┤
│ • Fact Tables: fact_911_incidents, fact_crime_reports           │
│ • Dimension Tables: dim_date, dim_location, dim_neighborhood    │
│ • Star Schema optimized for analytics queries                   │
│ • SCD Type 2 for slowly changing dimensions                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ DATA PRODUCT                                  [TODO: Phase 3]   │
├─────────────────────────────────────────────────────────────────┤
│ • Dashboard: Crime Trends, Hotspot Maps, Per-capita Rates       │
│ • API: Real-time neighborhood safety scores                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
.
├── dags/                              # Airflow DAGs
│   ├── ingestion_seattle_911_dag.py      # 911 calls ingestion (hourly)
│   ├── ingestion_spd_crime_dag.py        # Crime reports ingestion (daily)
│   └── ingestion_seattle_population_dag.py # ACS population (yearly)
│
├── data/
│   ├── bronze/                        # Raw data (immutable)
│   │   ├── seattle_911/YYYY/MM/DD/*.json
│   │   ├── spd_crime/YYYY/MM/DD/*.json
│   │   └── seattle_population/YYYY/*.csv
│   ├── silver/                        # Cleaned data [TODO]
│   ├── gold/                          # Data warehouse [TODO]
│   ├── raw_csv/                       # Manually downloaded CSVs
│   └── state/                         # Incremental tracking state files
│
├── logs/                              # Airflow execution logs
├── plugins/                           # Custom Airflow operators [if needed]
├── docker-compose.yml                 # Airflow Docker setup
├── .env                               # Environment variables (secrets)
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

### 4. รัน Airflow
```powershell
# First-time setup
docker compose up airflow-init

# Start services
docker compose up -d

# เปิด Web UI
start http://localhost:8080
# (user: admin / pass: admin)
```

### 5. Trigger DAGs
ใน Airflow Web UI:
1. Toggle เปิด DAG ทั้ง 3 (ปุ่มสีน้ำเงินทางซ้าย)
2. คลิก ▶ (Trigger DAG) เพื่อรันทันที

หรือ CLI:
```powershell
docker compose exec airflow-scheduler airflow dags trigger ingestion_seattle_911
docker compose exec airflow-scheduler airflow dags trigger ingestion_spd_crime
docker compose exec airflow-scheduler airflow dags trigger ingestion_seattle_population
```

---

## DAGs Overview

| DAG ID | Schedule | Description | Incremental Strategy |
|--------|----------|-------------|---------------------|
| `ingestion_seattle_911` | `@hourly` | ดึง 911 calls จาก Socrata API | Timestamp-based (`datetime > last_state`) |
| `ingestion_spd_crime` | `@daily` | ดึง crime reports + pagination | Timestamp-based (`report_date_time > last_state`) |
| `ingestion_seattle_population` | `@yearly` | Copy ACS CSV → Bronze | Full load (static data) |

**ทั้งหมดมี:**
- ✅ Retry 3 ครั้ง (delay 5-10 นาที)
- ✅ Idempotency check (ไม่ overwrite ไฟล์ซ้ำ)
- ✅ State file tracking (เก็บ watermark timestamp)
- ✅ Logging ครบทุก step

---

## Technical Features Implemented

### Data Ingestion (Phase 2) — ✅ เสร็จแล้ว

| Feature | Status | Implementation |
|---------|--------|----------------|
| **Incremental Load** | ✅ | State file tracking + WHERE clause filtering |
| **Idempotency** | ✅ | `if os.path.exists(filepath): skip write` |
| **Batch Processing** | ✅ | 50k records/page + offset pagination |
| **Error Handling** | ✅ | `retries: 3`, `retry_delay: 5-10 min` |
| **Logging** | ✅ | Python `logging` module ทุก task |
| **Multiple Data Formats** | ✅ | JSON (API) + CSV (manual file) |

### Data Transformation (Phase 2) — 🚧 In Progress
- [ ] Deduplication & Merge
- [ ] Data Quality checks (3+ rules)
- [ ] Schema standardization
- [ ] Silver layer consolidated tables

### Data Warehouse (Phase 2) — 📋 Planned
- [ ] Star Schema (Fact + Dimension tables)
- [ ] SCD Type 2 for dimensions
- [ ] Business metrics aggregation
- [ ] Gold layer analytics-ready tables

### Orchestration (Phase 3) — ✅ Automated
- ✅ Airflow scheduler (cron-based)
- ✅ Dependency management (task1 >> task2)
- [ ] Monitoring & alerting
- [ ] E2E testing

---

## Current Status

**Phase 2: Data Ingestion & Warehouse**  
Progress: **35% Complete**

- [x] ✅ Bronze Layer — 3 data sources ingestion
- [x] ✅ Incremental load mechanism
- [x] ✅ Logging & error handling
- [ ] 🚧 Silver Layer — Transformation
- [ ] 📋 Gold Layer — Data Warehouse
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
- [ชื่อสมาชิก 1]
- [ชื่อสมาชิก 2]
- [ชื่อสมาชิก 3]

---

## License

Academic project for ITDS344 — Mahidol University ICT  
Data sources: City of Seattle Open Data Portal (Public Domain)