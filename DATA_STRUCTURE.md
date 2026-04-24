# Data Structure & Dashboard Guide
**ITDS344 Group 7 — Seattle Public Safety Data Lake**

---

## Architecture Overview

```
Socrata API ──► Kafka Producer ──► [bronze_911_calls / bronze_crime_reports topics]
                                         │
                                         ▼
Airflow DAGs ──────────────────────────────────────────────────────────────────
  bronze_api_ingest    ──► MongoDB  bronze  (raw API snapshots)
  bronze_population    ──► MongoDB  bronze  (static ACS CSV)
  silver_transform     ──► MongoDB  silver  (cleaned & typed)
  gold_analytics       ──► MongoDB  gold    (Star Schema + Aggregations)
```

| Layer  | MongoDB DB | Purpose                          | Refresh        |
|--------|------------|----------------------------------|----------------|
| Bronze | `bronze`   | Raw data, immutable, full fidelity | every 5 min   |
| Silver | `silver`   | Typed, validated, deduplicated   | every 10 min   |
| Gold   | `gold`     | Star Schema + pre-aggregated views | every 30 min |

---

## Bronze Layer

### `bronze.seattle_911`
**Source:** Seattle 911 Dispatch API (Socrata)  
**Volume:** ~282,382 documents  
**Unique Key:** `incident_number`

| Field             | Type     | Description                              |
|-------------------|----------|------------------------------------------|
| `incident_number` | String   | Unique dispatch ID (e.g. `F240000002`)   |
| `datetime`        | String   | Dispatch datetime (ISO string from API)  |
| `type`            | String   | Event type (e.g. `Auto Fire Alarm`)      |
| `address`         | String   | Street address of incident               |
| `latitude`        | String   | Lat (string from API)                    |
| `longitude`       | String   | Lon (string from API)                    |
| `report_location` | Object   | GeoJSON Point `{type, coordinates[]}`    |
| `_source`         | String   | `"seattle_911"`                          |
| `_ingested_at`    | ISODate  | Timestamp written to MongoDB             |
| `_last_seen_at`   | ISODate  | Last time this record was seen in API    |

---

### `bronze.spd_crime`
**Source:** Seattle Police Department Crime Reports API (Socrata)  
**Volume:** ~184,298 documents  
**Unique Key:** `offense_id`

| Field                          | Type    | Description                             |
|--------------------------------|---------|-----------------------------------------|
| `offense_id`                   | String  | Unique offense ID                       |
| `report_number`                | String  | Police report number                    |
| `report_date_time`             | String  | Date/time report was filed (ISO string) |
| `offense_date`                 | String  | Date/time offense occurred (ISO string) |
| `offense_category`             | String  | High-level category (e.g. `THEFT`)      |
| `offense_sub_category`         | String  | Sub-category (e.g. `THEFT OFFENSES`)    |
| `nibrs_offense_code`           | String  | FBI NIBRS code (e.g. `13B`)             |
| `nibrs_offense_code_description` | String | Human-readable NIBRS description      |
| `nibrs_crime_against_category` | String  | `PERSON` / `PROPERTY` / `SOCIETY`      |
| `nibrs_group_a_b`              | String  | NIBRS Group A or B                      |
| `shooting_type_group`          | String  | Shooting classification or `-`          |
| `neighborhood`                 | String  | Seattle neighborhood name               |
| `precinct`                     | String  | Police precinct                         |
| `sector`                       | String  | Patrol sector                           |
| `beat`                         | String  | Patrol beat                             |
| `block_address`                | String  | Block-level address (privacy masked)    |
| `latitude`                     | String  | Lat (string from API)                   |
| `longitude`                    | String  | Lon (string from API)                   |
| `reporting_area`               | String  | SPD reporting area code                 |
| `census_block_2020`            | String  | 2020 census block ID                    |
| `_source`                      | String  | `"spd_crime"`                           |
| `_ingested_at`                 | ISODate | Timestamp written to MongoDB            |
| `_last_seen_at`                | ISODate | Last time this record was seen in API   |

---

### `bronze.seattle_population`
**Source:** ACS 5-Year Estimates CSV (`seattle_neighborhoods_acs.csv`)  
**Volume:** 87 neighborhoods  
**Unique Key:** `(neighborhood_name, acs_year)`

Contains **100+ demographic fields** from US Census ACS including:  
- `total_population`, `Total Housing Units`, `Total Households`
- Race/ethnicity breakdowns (White, Black, Hispanic, Asian, etc.)
- Income brackets, poverty rates, educational attainment
- Housing unit types, commute modes, employment rates
- Age groups (Children under 18, Working Age, 65+)
- `acs_year` (2024), `ACS Vinatage` (5Y24)

---

## Silver Layer

> **DQ Rules applied:** required fields non-null, datetimes parse correctly, lat/lon within valid WGS84 range, in-batch deduplication on business key.

### `silver.silver_911_clean`
**Volume:** 282,382 documents  
**Unique Key:** `event_id`

| Field                  | Type    | Transformation from Bronze                        |
|------------------------|---------|---------------------------------------------------|
| `event_id`             | String  | = `incident_number`                               |
| `call_datetime`        | ISODate | `datetime` string → parsed ISODate (UTC)          |
| `event_type`           | String  | `type` → `.strip().upper()`                       |
| `is_police_sent`       | Boolean | derived: True if event_type contains `police/spd/officer` |
| `address`              | String  | stripped whitespace                               |
| `latitude`             | Float   | cast from string; None if outside `[-90, 90]`     |
| `longitude`            | Float   | cast from string; None if outside `[-180, 180]`   |
| `_source`              | String  | `"seattle_911"`                                   |
| `_bronze_ingested_at`  | ISODate | copied from bronze `_ingested_at`                 |
| `_silver_processed_at` | ISODate | timestamp of silver transform run                 |
| `_created_at`          | ISODate | first time upserted into silver                   |

---

### `silver.silver_crime_clean`
**Volume:** 184,298 documents  
**Unique Key:** `offense_id`

| Field                          | Type    | Transformation from Bronze                        |
|--------------------------------|---------|---------------------------------------------------|
| `offense_id`                   | String  | unchanged                                         |
| `report_number`                | String  | unchanged                                         |
| `report_date_time`             | ISODate | string → parsed ISODate (UTC)                     |
| `offense_date`                 | ISODate | string → parsed ISODate (UTC)                     |
| `offense_category`             | String  | `.strip().upper()`                                |
| `offense_sub_category`         | String  | `.strip()`                                        |
| `nibrs_offense_code`           | String  | unchanged                                         |
| `nibrs_offense_code_description` | String | unchanged                                       |
| `nibrs_crime_against_category` | String  | unchanged                                         |
| `nibrs_group`                  | String  | = `nibrs_group_a_b`                               |
| `is_shooting`                  | Boolean | derived: True if `shooting_type_group` is not `-` or blank |
| `neighborhood`                 | String  | `.strip().upper()`                                |
| `precinct`                     | String  | `.strip().upper()`                                |
| `sector`                       | String  | `.strip().upper()`                                |
| `beat`                         | String  | unchanged                                         |
| `block_address`                | String  | unchanged                                         |
| `latitude`                     | Float   | cast; None if outside valid range                 |
| `longitude`                    | Float   | cast; None if outside valid range                 |
| `reporting_area`               | String  | unchanged                                         |
| `_source`                      | String  | `"spd_crime"`                                     |
| `_bronze_ingested_at`          | ISODate | copied from bronze                                |
| `_silver_processed_at`         | ISODate | timestamp of silver transform run                 |
| `_created_at`                  | ISODate | first time upserted                               |

---

### `silver.silver_population_clean`
**Volume:** 87 neighborhoods  
**Unique Key:** `(neighborhood_name, acs_year)`

| Field                    | Type    | Note                                              |
|--------------------------|---------|---------------------------------------------------|
| `neighborhood_name`      | String  | unchanged from CSV                                |
| `acs_year`               | Integer | e.g. `2024`                                       |
| `total_population`       | Integer | validated > 0                                     |
| `total_housing_units`    | Integer |                                                   |
| `median_age`             | Float   |                                                   |
| `median_household_income`| Float   | Per Capita Income (proxy, from `Per Capita Income` field) |
| `poverty_pct`            | Float   | Computed: families below poverty / total families × 100 |
| `black_pct`              | Float   | Computed: `Not Hispanic or Latino Black or African American alone` / `Total Population` × 100 |
| `hispanic_pct`           | Float   | Computed: `Hispanic or Latino (of any race)` / `Total Population` × 100 |
| `white_pct`              | Float   | Computed: `Not Hispanic or Latino White alone` / `Total Population` × 100 |
| `male_pct`               | Float   | Computed: `Male` / `Total Population` × 100 |
| `female_pct`             | Float   | Computed: `Female` / `Total Population` × 100 |
| `_source`                | String  | `"seattle_population_csv"`                        |
| `_silver_processed_at`   | ISODate | timestamp of transform                            |

---

## Gold Layer

### Fact Tables

#### `gold.fact_crime_events`
**Grain:** 1 row per offense event  
**Volume:** 184,298 documents

| Field                  | Type    | Description                                      |
|------------------------|---------|--------------------------------------------------|
| `offense_id`           | String  | PK — links to silver_crime_clean                 |
| `time_id`              | Integer | FK → `dim_time.time_id` (format: `YYYYMMDDHH`)  |
| `location_id`          | String  | FK → `dim_location.location_id` (MD5 hash)       |
| `offense_dim_id`       | String  | FK → `dim_offense.offense_dim_id` (MD5 hash)     |
| `report_number`        | String  | Police report number                             |
| `report_date_time`     | ISODate | Full report timestamp                            |
| `offense_date`         | ISODate | When offense occurred                            |
| `offense_category`     | String  | Denormalized for fast filtering                  |
| `neighborhood`         | String  | Denormalized for fast filtering                  |
| `is_shooting`          | Boolean | Shooting flag                                    |
| `_gold_loaded_at`      | ISODate | ETL load timestamp                               |

---

#### `gold.fact_911_calls`
**Grain:** 1 row per 911 dispatch call  
**Volume:** 282,382 documents

| Field                | Type    | Description                                        |
|----------------------|---------|----------------------------------------------------|
| `event_id`           | String  | PK — links to silver_911_clean                     |
| `time_id`            | Integer | FK → `dim_time.time_id` (format: `YYYYMMDDHH`)    |
| `event_type_id`      | String  | FK → `dim_event_type.event_type_id` (MD5 hash)     |
| `call_datetime`      | ISODate | Full dispatch timestamp                            |
| `event_type`         | String  | Denormalized for fast filtering                    |
| `address`            | String  | Street address                                     |
| `latitude`           | Float   |                                                    |
| `longitude`          | Float   |                                                    |
| `is_police_sent`     | Boolean | Whether police were dispatched                     |
| `_gold_loaded_at`    | ISODate | ETL load timestamp                                 |

---

### Dimension Tables

#### `gold.dim_time`
**Volume:** 20,267 unique hour-slots

| Field        | Type    | Example        |
|--------------|---------|----------------|
| `time_id`    | Integer | `2024010100`   |
| `date`       | String  | `"2024-01-01"` |
| `year`       | Integer | `2024`         |
| `month`      | Integer | `1`            |
| `day`        | Integer | `1`            |
| `hour`       | Integer | `0`            |
| `day_of_week`| String  | `"Monday"`     |
| `is_weekend` | Boolean | `false`        |

---

#### `gold.dim_location`
**Volume:** 283 unique locations

| Field           | Type   | Description                     |
|-----------------|--------|---------------------------------|
| `location_id`   | String | MD5 hash of precinct+sector+beat+neighborhood |
| `neighborhood`  | String | Seattle neighborhood            |
| `precinct`      | String | Police precinct                 |
| `sector`        | String | Patrol sector                   |
| `beat`          | String | Patrol beat                     |
| `reporting_area`| String | SPD reporting area code         |
| `latitude`      | Float  | Representative lat for location |
| `longitude`     | Float  | Representative lon              |

---

#### `gold.dim_offense`
**Volume:** 61 unique offense types

| Field               | Type   | Description            |
|---------------------|--------|------------------------|
| `offense_dim_id`    | String | MD5 hash key           |
| `offense_code`      | String | FBI NIBRS code         |
| `offense_description`| String | NIBRS description     |
| `offense_category`  | String | High-level category    |
| `offense_sub_category`| String | Sub-category          |
| `nibrs_group`       | String | Group A or B           |
| `crime_against`     | String | PERSON / PROPERTY / SOCIETY |

---

#### `gold.dim_event_type`
**Volume:** 145 unique event types

| Field                | Type    | Description                            |
|----------------------|---------|----------------------------------------|
| `event_type_id`      | String  | MD5 hash key                           |
| `event_type`         | String  | Dispatch event type (uppercased)       |
| `police_required_flag`| Boolean| True if type implies police response  |

---

#### `gold.dim_demographics`
**Volume:** 87 neighborhoods

| Field                    | Type    | Description                        |
|--------------------------|---------|------------------------------------|
| `neighborhood_name`      | String  | PK                                 |
| `acs_year`               | Integer | ACS vintage year                   |
| `total_population`       | Integer |                                    |
| `total_housing_units`    | Integer |                                    |
| `median_age`             | Float   |                                    |
| `median_household_income`| Float   | Per Capita Income proxy            |
| `poverty_pct`            | Float   | Families below poverty %           |
| `black_pct`              | Float   | Black/AA population %              |
| `hispanic_pct`           | Float   | Hispanic population %              |
| `white_pct`              | Float   | White (non-Hispanic) population %  |
| `male_pct`               | Float   | Male population %                  |
| `female_pct`             | Float   | Female population %                |

---

### Aggregation Collections (pre-computed)

#### `gold.agg_crime_by_neighborhood_month`
**Volume:** 1,685 documents

| Field            | Type    | Description                         |
|------------------|---------|-------------------------------------|
| `neighborhood`   | String  | Seattle neighborhood                |
| `year`           | Integer |                                     |
| `month`          | Integer |                                     |
| `crime_count`    | Integer | Total offenses in that month        |
| `shooting_count` | Integer | Shootings in that month             |

---

#### `gold.agg_crime_by_offense_category`
**Volume:** 84 documents

| Field              | Type    | Description                        |
|--------------------|---------|------------------------------------|
| `offense_category` | String  |                                    |
| `year`             | Integer |                                    |
| `month`            | Integer |                                    |
| `crime_count`      | Integer | Total offenses in category × month |

---

#### `gold.agg_911_by_hour_day`
**Volume:** 168 documents (24 hours × 7 days)

| Field         | Type    | Description                                 |
|---------------|---------|---------------------------------------------|
| `hour`        | Integer | 0–23                                        |
| `day_of_week` | Integer | 0=Monday … 6=Sunday                         |
| `call_count`  | Integer | Total 911 calls in this hour × day slot     |

---

#### `gold.agg_crime_per_capita`
**Volume:** 87 neighborhoods

| Field                | Type    | Description                                     |
|----------------------|---------|-------------------------------------------------|
| `neighborhood_name`  | String  | PK                                              |
| `total_population`   | Integer | From dim_demographics                           |
| `total_crimes`       | Integer | Total offense count                             |
| `crime_rate_per_10k` | Float   | `(total_crimes / total_population) × 10,000`   |

---

## Idempotency (Duplicate Prevention)

All three layers are protected against duplicate data. Verified on 2026-04-25:

| Layer | Key Field | Mechanism | Duplicate Count |
|-------|-----------|-----------|----------------|
| Bronze `seattle_911` | `incident_number` | `$setOnInsert` upsert | **0** |
| Bronze `spd_crime` | `offense_id` | `$setOnInsert` upsert | **0** |
| Silver `silver_911_clean` | `event_id` | `UpdateOne` upsert | **0** |
| Silver `silver_crime_clean` | `offense_id` | `UpdateOne` upsert | **0** |
| Gold `fact_crime_events` | `offense_id` | `UpdateOne` upsert | **0** |
| Gold `fact_911_calls` | `event_id` | `UpdateOne` upsert | **0** |

**How it works:**
- Bronze consumer uses `{"$setOnInsert": {...}, "$set": {"_last_seen_at": ...}}` — re-running the Kafka consumer only updates `_last_seen_at`, never creates duplicates
- Silver and Gold use `UpdateOne(..., upsert=True)` with business key filter — idempotent across any number of DAG re-runs
- Watermarks in `<db>.watermarks` collection ensure each layer only processes new records, preventing re-processing on normal runs

> **Safe to re-run:** Triggering any DAG multiple times is always safe. No data corruption or duplication occurs.

---

## Pipeline Status (Verified 2026-04-25)

| Collection | Documents | Status |
|-----------|-----------|--------|
| `bronze.seattle_911` | 282,384 | ✅ Healthy |
| `bronze.spd_crime` | 184,298 | ✅ Healthy |
| `bronze.seattle_population` | 87 | ✅ Healthy |
| `silver.silver_911_clean` | 282,384 | ✅ Healthy |
| `silver.silver_crime_clean` | 184,298 | ✅ Healthy |
| `silver.silver_population_clean` | 87 | ✅ Healthy |
| `gold.fact_crime_events` | 184,298 | ✅ Healthy |
| `gold.fact_911_calls` | 282,383 | ✅ Healthy |
| `gold.dim_time` | 20,267 | ✅ Healthy |
| `gold.dim_location` | 283 | ✅ Healthy |
| `gold.dim_offense` | 61 | ✅ Healthy |
| `gold.dim_event_type` | 145 | ✅ Healthy |
| `gold.dim_demographics` | 87 | ✅ Healthy |
| `gold.agg_crime_by_neighborhood_month` | 1,685 | ✅ Healthy |
| `gold.agg_crime_by_offense_category` | 84 | ✅ Healthy |
| `gold.agg_911_by_hour_day` | 168 | ✅ Healthy |
| `gold.agg_crime_per_capita` | 87 | ✅ Healthy |

---

## Known Data Quality Issues

| # | Layer  | Issue                                          | Status | Notes |
|---|--------|------------------------------------------------|--------|-------|
| 1 | Silver | `silver_population_clean` — pct fields all null | ✅ **Fixed** | Computed from raw counts: e.g. `black_pct = Black_count / total_population * 100` |
| 2 | Gold   | `agg_crime_per_capita` — `total_crimes = 0`   | ✅ **Fixed** | Added 63-entry neighborhood name mapping (SPD UPPERCASE → ACS Title Case) in gold DAG |
| 3 | Bronze | `report_location` GeoJSON not carried to silver | ⚠️ Open | `lat`/`lon` floats are available; carrying full GeoJSON needs `2dsphere` index in silver |

---

## Dashboard Recommendations

### Dashboard 1 — Crime Hotspot Map 🗺️
**Collection:** `gold.fact_crime_events` + `gold.dim_location`  
**Viz type:** Interactive choropleth / pin map

**Charts to build:**
- Heat map of crime density by neighborhood (color = crime count)
- Top 10 highest-crime neighborhoods bar chart
- Monthly crime trend line per neighborhood
- Shooting incidents overlay (filter `is_shooting = true`)

**Key query:**
```js
db.agg_crime_by_neighborhood_month.find({year: 2024}).sort({crime_count: -1})
```

---

### Dashboard 2 — Crime Category Trends 📊
**Collection:** `gold.agg_crime_by_offense_category` + `gold.dim_offense`  
**Viz type:** Stacked bar / line chart

**Charts to build:**
- Monthly crime trend by category (THEFT, VIOLENT CRIME, PROPERTY CRIME, etc.)
- Offense category donut chart (proportion of total)
- NIBRS Group A vs B split
- PERSON vs PROPERTY vs SOCIETY crime-against breakdown

**Key query:**
```js
db.agg_crime_by_offense_category.find({year: 2024}).sort({month:1, crime_count:-1})
```

---

### Dashboard 3 — 911 Dispatch Heatmap ⏰
**Collection:** `gold.agg_911_by_hour_day`  
**Viz type:** Heatmap (7 days × 24 hours)

**Charts to build:**
- Hour × Day-of-Week call volume heatmap (reveals peak patterns)
- Police dispatch rate: `is_police_sent = true` % over time
- Top 10 most common event types (bar chart from `dim_event_type`)
- Weekly call volume trend

**Key query:**
```js
db.agg_911_by_hour_day.find().sort({day_of_week:1, hour:1})
```

---

### Dashboard 4 — Public Safety KPI Summary 📋
**Collections:** All gold collections  
**Viz type:** KPI cards + gauges

**Metrics to display:**
| KPI | Query |
|-----|-------|
| Total crimes YTD | `fact_crime_events` count with year filter |
| Total 911 calls YTD | `fact_911_calls` count with year filter |
| % calls requiring police | `fact_911_calls` where `is_police_sent = true` |
| Total shootings | `fact_crime_events` where `is_shooting = true` |
| Most dangerous neighborhood | `agg_crime_by_neighborhood_month` top 1 |
| Busiest dispatch hour | `agg_911_by_hour_day` top 1 |

---

### Dashboard 5 — Demographics & Crime Correlation 👥
**Collections:** `gold.agg_crime_per_capita` + `gold.dim_demographics`  
**Viz type:** Scatter plot + bar chart

**Charts to build:**
- Crime rate per 10,000 residents by neighborhood (bar chart sorted desc)
- Scatter: `crime_rate_per_10k` vs `median_age`
- Scatter: `crime_rate_per_10k` vs `poverty_pct`
- Population vs crime count bubble chart

---

## Quick Access

| Service       | URL                      | Credentials   |
|---------------|--------------------------|---------------|
| Airflow UI    | http://localhost:8080    | admin / admin |
| Mongo Express | http://localhost:8081    | admin / admin |

### Run full pipeline manually
```bash
# 1. Bronze (API + Population)
docker exec itds344_group7_project-airflow-scheduler-1 airflow dags trigger bronze_api_ingest
docker exec itds344_group7_project-airflow-scheduler-1 airflow dags trigger bronze_population_ingest

# 2. Silver (after bronze finishes ~2 min)
docker exec itds344_group7_project-airflow-scheduler-1 airflow dags trigger silver_transform

# 3. Gold (after silver finishes ~2 min)
docker exec itds344_group7_project-airflow-scheduler-1 airflow dags trigger gold_analytics
```
