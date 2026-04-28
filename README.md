# ITDS344 Group 7 — Seattle Public Safety Analytics Pipeline

**End-to-End Data Engineering Project**  
ITDS344 Data Engineering and Infrastructures (Semester 2/2568)

---

## Project Overview

ระบบ Data Engineering แบบ Medallion Architecture (Bronze, Silver, Gold) สำหรับวิเคราะห์ Public Safety ของเมือง Seattle โดยรวมข้อมูล 911 Calls, Crime Reports และ ACS Demographics เข้าด้วยกัน

**Business Goals:**
- วิเคราะห์ Crime Trends และ Hotspots ตามย่านและช่วงเวลา
- คำนวณอัตราอาชญากรรมต่อประชากร (per-capita crime rate)
- เปรียบเทียบความถี่ 911 calls กับจำนวน crime reports
- สนับสนุน data-driven decision making สำหรับการจัดสรรทรัพยากร

---

## Data Sources

| # | Dataset | Source | Type | Records |
|---|---------|--------|------|---------|
| 1 | Seattle Real-Time Fire 911 Calls | [data.seattle.gov](https://data.seattle.gov/Public-Safety/Seattle-Real-Time-Fire-911-Calls/kzjm-xkqj) | Socrata API | ~282,000 |
| 2 | SPD Crime Data 2008-Present | [data.seattle.gov](https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5) | Socrata API | ~184,000 raw / ~170,000 after DQ |
| 3 | Seattle Neighborhoods ACS (2024) | `data/raw_csv/seattle_neighborhoods_acs.csv` | CSV | 87 neighborhoods |

---

## Architecture

ระบบแบ่งเป็น 3 Pipeline อิสระ ตาม 3 แหล่งข้อมูล แต่ละ Pipeline ครอบคลุม Bronze, Silver, Gold ในตัวเอง

```
Pipeline 1: Seattle 911 (every 5 min, 3 Airflow tasks)
  Kafka consumer_bronze.py  →  bronze.seattle_911
  silver_transform_911      →  silver.silver_911_clean
  gold_fact_911_calls       →  gold.fact_911_calls         (with neighborhood enrichment)
  gold_agg_911_by_hour_day  →  gold.agg_911_by_hour_day

Pipeline 2: SPD Crime (every 60 min, 10 Airflow tasks)
  Kafka consumer_bronze.py  →  bronze.spd_crime
  silver_transform_crime    →  silver.silver_crime_clean
  [gold_dim_location, gold_dim_offense, gold_dim_neighborhood]
  gold_fact_crime_events    →  gold.fact_crime_events
  [gold_agg_crime_by_category, gold_agg_crime_per_capita,
   gold_agg_crime_trend_monthly, gold_agg_911_per_capita,
   gold_agg_neighborhood_safety_profile]

Pipeline 3: Population (run @once on deploy, 4 Airflow tasks)
  validate_csv → bronze_load_population → silver_transform_population
  gold_dim_demographics  (consumed by Pipeline 2)
```

**First-run order** — run in this sequence to ensure dependencies are satisfied:

1. `seattle_population_pipeline` — creates `dim_demographics`
2. `spd_crime_pipeline` — creates `dim_neighborhood` and cross-dataset aggregations
3. `seattle_911_pipeline` — enriches `fact_911_calls.neighborhood_name`
4. `spd_crime_pipeline` (second run) — `agg_neighborhood_safety_profile` will include 911 data

**Services**

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| Airflow | http://localhost:8080 | admin / admin | DAG monitoring and triggering |
| Mongo Express | http://localhost:8081 | admin / admin | Browse MongoDB collections |
| MongoDB | localhost:27017 | — | Direct database access |
| Kafka | localhost:9092 | — | Message broker |
| Streamlit | http://localhost:8501 | — | Analytics dashboard (run locally) |

---

## Project Structure

```
ITDS344_Group7_Project/
├── dags/
│   ├── dag_seattle_911.py          # Pipeline 1: 911  (every 5 min, 3 tasks)
│   ├── dag_spd_crime.py            # Pipeline 2: Crime (every 60 min, 10 tasks)
│   └── dag_seattle_population.py   # Pipeline 3: Population (@once, 4 tasks)
├── dashboard/
│   ├── dashboard.py                # Streamlit analytics dashboard
│   └── requirements.txt            # Dashboard-only dependencies
├── kafka/
│   ├── kafka_producer.py           # Streaming: Poll Socrata API → Kafka topics
│   ├── consumer_bronze.py          # Streaming: Kafka → MongoDB bronze (upsert)
│   └── Dockerfile                  # Python 3.12 image for Kafka services
├── data/
│   └── raw_csv/
│       └── seattle_neighborhoods_acs.csv   # ACS population CSV (tracked in git)
├── Dockerfile                      # Airflow worker image
├── docker-compose.yml
├── requirements.txt                # Airflow worker dependencies (pymongo, requests)
├── .env                            # Secrets — NOT in git
├── DATA_STRUCTURE.md               # Full schema reference for all collections
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine (Linux) — version 24+
- Minimum 8 GB RAM recommended (Airflow + MongoDB + Kafka use ~4–5 GB)
- Python 3.9+ installed locally (for running the dashboard)

---

### Step 1 — Clone and enter the project directory

```powershell
git clone <repository-url>
cd ITDS344_Group7_Project
```

---

### Step 2 — Create the `.env` file

The `.env` file is not tracked in git. Create it manually:

```ini
# Socrata App Token (free — required for production rate limits)
SOCRATA_APP_TOKEN=your_token_here

# Airflow
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.

# PostgreSQL (Airflow metadata DB)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow admin account
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# MongoDB
MONGO_URI=mongodb://mongo:27017
```

To get a free Socrata App Token: go to https://data.seattle.gov → Sign In → My Profile → App Tokens → Create New App Token.  
The system works without a token but Socrata will rate-limit requests to ~100 req/hr.

> Linux users: run `echo "AIRFLOW_UID=$(id -u)" >> .env` before starting.

---

### Step 3 — Verify the ACS population CSV exists

```powershell
Test-Path data\raw_csv\seattle_neighborhoods_acs.csv
# Expected: True  (file is tracked in git)
```

---

### Step 4 — Start Docker services

```powershell
# Build images and start all services
docker compose up --build -d

# Check that all containers are healthy (allow ~2 minutes)
docker ps

# Optional: confirm airflow-init exited cleanly
docker compose logs airflow-init
```

---

### Step 5 — Verify DAGs and trigger pipelines

Open Airflow at **http://localhost:8080** (admin / admin).  
Three DAGs should be visible and enabled:

| DAG | Schedule | Notes |
|-----|----------|-------|
| `seattle_population_pipeline` | `@once` | Runs automatically on first deploy |
| `seattle_911_pipeline` | every 5 min | Runs automatically |
| `spd_crime_pipeline` | every 60 min | Runs automatically |

To trigger manually in the correct order:

```powershell
docker exec itds344_group7_project-airflow-scheduler-1 `
  airflow dags trigger seattle_population_pipeline

# Wait for completion, then:
docker exec itds344_group7_project-airflow-scheduler-1 `
  airflow dags trigger spd_crime_pipeline

# Wait for completion, then:
docker exec itds344_group7_project-airflow-scheduler-1 `
  airflow dags trigger seattle_911_pipeline

# Run crime pipeline once more to populate 911 data in safety profile:
docker exec itds344_group7_project-airflow-scheduler-1 `
  airflow dags trigger spd_crime_pipeline
```

---

### Step 6 — Verify MongoDB collections

Open Mongo Express at **http://localhost:8081** or run:

```powershell
docker exec itds344_group7_project-mongo-1 mongosh --quiet --eval "
  use('bronze');
  print('911:', db.seattle_911.countDocuments());
  print('crime:', db.spd_crime.countDocuments());
  use('silver');
  print('silver_911:', db.silver_911_clean.countDocuments());
  print('silver_crime:', db.silver_crime_clean.countDocuments());
  use('gold');
  print('fact_crime:', db.fact_crime_events.countDocuments());
  print('fact_911:', db.fact_911_calls.countDocuments());
  print('dim_neighborhood:', db.dim_neighborhood.countDocuments());
  print('safety_profile:', db.agg_neighborhood_safety_profile.countDocuments());
"
```

Expected collection counts:

| Database | Collection | Expected |
|----------|-----------|---------|
| bronze | seattle_911 | ~282,000 |
| bronze | spd_crime | ~184,000 |
| bronze | seattle_population | 87 |
| silver | silver_911_clean | ~282,000 |
| silver | silver_crime_clean | ~170,000 |
| silver | silver_population_clean | 87 |
| gold | fact_crime_events | ~170,000 |
| gold | fact_911_calls | ~282,000 |
| gold | dim_location | ~283 |
| gold | dim_offense | ~61 |
| gold | dim_demographics | 87 |
| gold | dim_neighborhood | 87 |
| gold | agg_911_by_hour_day | 168 |
| gold | agg_crime_by_offense_category | ~84 |
| gold | agg_crime_per_capita | 87 |
| gold | agg_crime_trend_monthly | >500 |
| gold | agg_911_per_capita | 87 |
| gold | agg_neighborhood_safety_profile | 87 |

---

### Step 7 — Run the dashboard

The dashboard runs locally against MongoDB on `localhost:27017`.

```powershell
cd dashboard
streamlit run dashboard.py
```

Open **http://localhost:8501** in a browser.

---

## Dashboard

Seven analytics sections:

| Section | Collections Used | Description |
|---------|-----------------|-------------|
| KPI Summary | fact_crime_events, fact_911_calls | Filterable by day / month / year |
| Neighborhood Safety Profile | agg_neighborhood_safety_profile | Crime rate, 911 rate, poverty correlation scatter |
| Safety Map | agg_neighborhood_safety_profile | Bubble map — select crime / 911 / shooting rate |
| Crime Trend Monthly | agg_crime_trend_monthly | Line chart by neighborhood x category |
| Crime vs 911 per Capita | agg_crime_per_capita, agg_911_per_capita | Side-by-side per-10K comparison |
| Crime Categories & NIBRS | agg_crime_by_offense_category, dim_offense | Category bar chart, Group A/B pie |
| 911 Dispatch Heatmap | agg_911_by_hour_day | Hour x day-of-week volume heatmap |

Data is cached for 5 minutes (`@st.cache_resource`). Press **R** in the browser to force a refresh.

---

## Useful Commands

```powershell
# Stop all services (preserves data volumes)
docker compose down

# Stop all services and delete all data (full reset)
docker compose down -v

# Tail live logs
docker compose logs -f airflow-scheduler
docker compose logs -f kafka-producer
docker compose logs -f kafka-consumer-bronze

# Open MongoDB shell
docker exec -it itds344_group7_project-mongo-1 mongosh
```

---

## Troubleshooting

| Symptom | Cause | Resolution |
|---------|-------|-----------|
| DAG not visible in Airflow | Syntax error in DAG file | `docker compose logs airflow-scheduler` |
| Silver empty despite bronze having data | Bronze ingestion still in progress | Wait 2 minutes, then re-trigger silver |
| `service "mongo" is not running` | Wrong working directory | Run `docker compose` from the project root |
| Permission denied (Linux) | AIRFLOW_UID mismatch | `echo "AIRFLOW_UID=$(id -u)" >> .env` then `docker compose up -d` |
| Port 8080 already in use | Conflicting service | Change `AIRFLOW_WEBSERVER_PORT` in `.env` |
| `agg_neighborhood_safety_profile` has 0 in 911 columns | 911 pipeline ran before crime pipeline | Re-trigger `spd_crime_pipeline` after `seattle_911_pipeline` completes |

---

## Data Schema

Full schema documentation for all 18 collections (Bronze, Silver, Gold), DQ rules, and NIBRS reference:  
**[DATA_STRUCTURE.md](DATA_STRUCTURE.md)**

---

## Technical Features

| Layer | Feature | Status | Detail |
|-------|---------|--------|--------|
| Bronze | Real-time streaming | Done | Kafka Producer polls Socrata API every 5 min |
| Bronze | Incremental watermark | Done | MongoDB `bronze.watermarks` — initialised to 2024-01-01 |
| Bronze | Idempotency | Done | Upsert by `incident_number` / `offense_id` |
| Bronze | DQ: Schema | Done | datetime parse check, required field null check |
| Bronze | DQ: Range | Done | lat/lon WGS84 boundary warn, future datetime drop |
| Silver | DQ: Null drop | Done | Drop records missing key fields |
| Silver | DQ: Future date | Done | Drop records with future `report_date_time` / `datetime` |
| Silver | DQ: Cleaning | Done | Drop UNKNOWN / REDACTED / OOJ offense_category and neighborhood |
| Silver | Deduplication | Done | MongoDB `UpdateOne` upsert per pipeline run |
| Gold | Star Schema | Done | dim_location, dim_offense, dim_neighborhood, dim_demographics |
| Gold | Fact tables | Done | fact_crime_events, fact_911_calls (with neighborhood enrichment) |
| Gold | Cross-dataset analytics | Done | agg_neighborhood_safety_profile joins Crime + 911 + Population |
| Gold | Per-capita rates | Done | agg_crime_per_capita, agg_911_per_capita per 10K residents |
| Gold | Trend analysis | Done | agg_crime_trend_monthly: neighborhood x category x month |
| Streaming | Kafka | Done | Topics: bronze_911_calls, bronze_crime_reports |
| Orchestration | Airflow | Done | 3 DAGs, retry 3x, max_active_runs=1 |

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