# Data Structure & Dashboard Guide
**ITDS344 Group 7 — Seattle Public Safety Data Lake**

---

## Architecture Overview

ระบบแบ่งเป็น **3 End-to-End Pipeline** แยกต่อแหล่งข้อมูล แต่ละ pipeline ครอบคลุม Bronze → Silver → Gold ในตัวเอง

```
Pipeline 1: dag_seattle_911      (every 5 min, 4 tasks)
  Socrata 911 API ──► bronze_ingest_911 ──► silver_transform_911
  + Kafka streaming                  ──► gold_fact_911_calls
                                     ──► gold_agg_911_by_hour_day

Pipeline 2: dag_spd_crime        (every 60 min, 6 tasks)
  Socrata Crime API ──► bronze_ingest_crime ──► silver_transform_crime (with DQ filters)
  + Kafka streaming           ──► gold_dim_location, gold_dim_offense
                              ──► gold_fact_crime_events
                              ──► gold_agg_crime_by_category / per_capita

Pipeline 3: dag_seattle_population  (@once on deploy, 4 tasks)
  ACS CSV ──► validate_csv ──► bronze_load_population ──► silver_transform_population
         ──► gold_dim_demographics  (ใช้โดย pipeline 2 — agg_crime_per_capita)
```

| Layer  | MongoDB DB | Purpose                            | Refresh                  |
|--------|------------|------------------------------------|--------------------------|
| Bronze | `bronze`   | Raw data, immutable, full fidelity | 911: 5 min / Crime: 60 min / Pop: @once |
| Silver | `silver`   | Typed, validated, deduplicated     | ตาม Bronze ของแต่ละ source |
| Gold   | `gold`     | Star Schema + pre-aggregated views | ตาม Silver ของแต่ละ source |

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
| `address`              | String  | stripped whitespace                               |
| `latitude`             | Float   | cast from string; None if outside `[-90, 90]`     |
| `longitude`            | Float   | cast from string; None if outside `[-180, 180]`   |
| `_source`              | String  | `"seattle_911"`                                   |
| `_bronze_ingested_at`  | ISODate | copied from bronze `_ingested_at`                 |
| `_silver_processed_at` | ISODate | timestamp of silver transform run                 |
| `_created_at`          | ISODate | first time upserted into silver                   |

**DQ Rules:**
- `offense_category`, `neighborhood`: Skip rows with UNKNOWN, NOT_A_CRIME, REDACTED, OOJ, `-`, empty, or values starting with `99`

---

### `silver.silver_crime_clean`
**Volume:** ~170,000 documents (reduced from ~184K after DQ filtering)  
**Unique Key:** `offense_id`

**Data Quality Filters Applied:**
- Drop rows with invalid `offense_category`: UNKNOWN, NOT_A_CRIME, REDACTED, OOJ, "-", empty, or starting with "99"
- Drop rows with invalid `neighborhood`: UNKNOWN, "-", REDACTED, OOJ, empty, or starting with "99"
- Approximately 14,000 rows filtered out (~7.6% of raw data)

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
| `is_shooting`                  | Boolean | derived: True if `shooting_type_group` contains "Shots Fired" or "Shooting" (case-insensitive) |
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
**Volume:** ~170K documents (after DQ filtering - reduced from ~184K in bronze)

| Field                  | Type    | Description                                      |
|------------------------|---------|--------------------------------------------------|
| `offense_id`           | String  | PK — links to silver_crime_clean                 |
| `time_id`              | Integer | Time key (format: `YYYYMMDDHH`) for time-based queries |
| `location_id`          | String  | FK → `dim_location.location_id` (MD5 hash)       |
| `offense_dim_id`       | String  | FK → `dim_offense.offense_dim_id` (MD5 hash)     |
| `report_date_time`     | ISODate | Full report timestamp                            |
| `offense_category`     | String  | Denormalized for fast filtering                  |
| `neighborhood`         | String  | Denormalized for fast filtering                  |
| `is_shooting`          | Boolean | Shooting flag (True for "Shots Fired"/"Shooting")|
| `_silver_processed_at` | ISODate | Watermark timestamp from silver                  |
| `_gold_loaded_at`      | ISODate | ETL load timestamp                               |

**Optimized for:** Dashboard queries using neighborhood, offense_category, is_shooting filters  
**DQ Impact:** ~14K rows filtered out during silver transform (~7.6% of raw data)

---

#### `gold.fact_911_calls`
**Grain:** 1 row per 911 dispatch call  
**Volume:** ~282,000 documents

| Field                  | Type    | Description                                      |
|------------------------|---------|--------------------------------------------------|
| `event_id`             | String  | PK — links to silver_911_clean                   |
| `time_id`              | Integer | Time key (format: `YYYYMMDDHH`) for time-based queries |
| `event_type_id`        | String  | MD5 hash of event_type (for potential future joins) |
| `call_datetime`        | ISODate | Full dispatch timestamp                          |
| `event_type`           | String  | Denormalized for fast filtering                  |
| `address`              | String  | Street address                                   |
| `latitude`             | Float   | Dispatch location latitude                       |
| `longitude`            | Float   | Dispatch location longitude                      |
| `_silver_processed_at` | ISODate | Watermark timestamp from silver                  |
| `_gold_loaded_at`      | ISODate | ETL load timestamp                               |

**Optimized for:** Dashboard map visualization and time-series heatmap  
**Note:** time_id and event_type_id are computed fields; no dimension joins required in current implementation

---

### Dimension Tables

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

## Pipeline Status (Updated 2026-04-27)

| Collection | Documents | Status | Notes |
|-----------|-----------|--------|-------|
| `bronze.seattle_911` | ~282,000 | ✅ Healthy | Raw 911 dispatch data |
| `bronze.spd_crime` | ~184,000 | ✅ Healthy | Raw crime reports |
| `bronze.seattle_population` | 87 | ✅ Healthy | ACS demographics |
| `silver.silver_911_clean` | ~282,000 | ✅ Healthy | All pass DQ rules |
| `silver.silver_crime_clean` | ~170,000 | ✅ Healthy | **~14K filtered out (DQ)** |
| `silver.silver_population_clean` | 87 | ✅ Healthy | Computed percentages |
| `gold.fact_crime_events` | ~170,000 | ✅ Healthy | Optimized fields |
| `gold.fact_911_calls` | ~282,000 | ✅ Healthy | Complete call history |
| `gold.dim_location` | 283 | ✅ Healthy | Precinct/sector/beat/neighborhood |
| `gold.dim_offense` | 61 | ✅ Healthy | NIBRS offense types |
| `gold.dim_demographics` | 87 | ✅ Healthy | ACS by neighborhood |
| `gold.agg_crime_by_offense_category` | 84 | ✅ Healthy | Category × month |
| `gold.agg_911_by_hour_day` | 168 | ✅ Healthy | 24h × 7 days heatmap |
| `gold.agg_crime_per_capita` | 87 | ✅ Healthy | Rate per 10K population |

**Total Collections:** 14 (Bronze: 3, Silver: 3, Gold: 8)  
**Removed Collections:** dim_time, dim_event_type, agg_crime_by_neighborhood_month (not used by dashboard)

---

## Known Data Quality Issues & Resolutions

| # | Layer  | Issue                                          | Status | Resolution |
|---|--------|------------------------------------------------|--------|------------|
| 1 | Silver | `silver_population_clean` — pct fields all null | ✅ **Fixed** | Computed from raw counts: e.g. `black_pct = Black_count / total_population * 100` |
| 2 | Gold   | `agg_crime_per_capita` — `total_crimes = 0`   | ✅ **Fixed** | Added 63-entry neighborhood name mapping (SPD UPPERCASE → ACS Title Case) |
| 3 | Silver | Crime data contains invalid categories (NOT_A_CRIME, UNKNOWN, etc.) | ✅ **Fixed** | DQ filters drop ~14K rows (~7.6%) with invalid offense_category or neighborhood |
| 4 | Silver | `is_shooting` flag incorrectly set for "-" values | ✅ **Fixed** | Changed logic to regex match "Shots Fired" or "Shooting" only |
| 5 | Gold   | Unused dimension tables (dim_time, dim_event_type) | ✅ **Fixed** | Removed from pipelines; dashboard uses denormalized fields |
| 3 | Bronze | `report_location` GeoJSON not carried to silver | ⚠️ Open | `lat`/`lon` floats are available; carrying full GeoJSON needs `2dsphere` index in silver |

---

## Dashboard Recommendations

### Dashboard 1 — Crime Hotspot Map 🗺️
**Collection:** `gold.fact_crime_events` + `gold.dim_location`  
**Viz type:** Interactive choropleth / pin map

**Charts to build:**
- Heat map of crime density by neighborhood (color = crime count)
- Top 10 highest-crime neighborhoods bar chart
- Shooting incidents overlay (filter `is_shooting = true`)
- Crime bubbles sized by count per neighborhood

**Key query:**
```js
// Aggregate crime by neighborhood from fact table
db.fact_crime_events.aggregate([
  {$match: {neighborhood: {$ne: ""}}},
  {$group: {_id: "$neighborhood", crime_count: {$sum: 1}}},
  {$sort: {crime_count: -1}}
])
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
- Top 10 most common event types (bar chart from `fact_911_calls.event_type`)
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
| Total shootings | `fact_crime_events` where `is_shooting = true` |
| Crime rate per 10K | Average from `agg_crime_per_capita` |
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
| Streamlit Dashboard | http://localhost:8501  | - (run separately) |

### Run pipelines manually
```bash
# Trigger 911 pipeline (every 5 min normally)
docker exec itds344_group7_project-airflow-scheduler-1 \
  airflow dags trigger seattle_911_pipeline

# Trigger crime pipeline (every 60 min normally)
docker exec itds344_group7_project-airflow-scheduler-1 \
  airflow dags trigger spd_crime_pipeline

# Trigger population pipeline (@once normally)
docker exec itds344_group7_project-airflow-scheduler-1 \
  airflow dags trigger seattle_population_pipeline
```

### Start Streamlit Dashboard
```bash
cd dashboard
pip install -r requirements.txt
streamlit run dashboard.py
# Open http://localhost:8501 in browser
```

---

## NIBRS Crime Classification Reference

**NIBRS** = National Incident-Based Reporting System (FBI's standardized crime reporting framework)

### Group A Offenses (Serious Crimes)
Major crimes requiring detailed incident reporting, victim information, and full investigation:

**Violent Crimes:**
- Murder and Nonnegligent Manslaughter
- Rape
- Robbery
- Aggravated Assault
- Kidnapping/Abduction

**Property Crimes:**
- Burglary
- Larceny-Theft
- Motor Vehicle Theft
- Arson

**Other Serious Offenses:**
- Drug/Narcotic Violations
- Weapon Law Violations
- Fraud (Credit Card, Wire, Embezzlement)
- Counterfeiting/Forgery
- Extortion/Blackmail
- Pornography/Obscene Material

**Characteristics:**
- ✅ Full incident details collected
- ✅ Victim demographics recorded
- ✅ Property loss tracked
- ✅ Relationship to offender documented
- ✅ Used for national crime statistics (UCR)

---

### Group B Offenses (Less Serious Crimes)
Minor crimes that only require arrest data (not incident data):

**Common Group B Offenses:**
- Disorderly Conduct
- DUI (Driving Under the Influence)
- Liquor Law Violations
- Drunkenness
- Trespass of Real Property
- Curfew/Loitering Violations
- Runaway
- Bad Checks (Non-Fraud)

**Characteristics:**
- ⚠️ **Only reported when arrest is made** (no incident without arrest)
- ⚠️ Minimal detail collected (arrestee info only)
- ⚠️ No victim information required
- ⚠️ Summary statistics only

---

### Crime Against Categories

Crimes are also classified by what they target:

| Category | Description | Examples |
|----------|-------------|----------|
| **PERSON** | Crimes against individuals | Assault, Rape, Murder, Kidnapping |
| **PROPERTY** | Crimes against belongings | Burglary, Theft, Vandalism, Arson |
| **SOCIETY** | Crimes against public order | Drug Violations, Weapons, Prostitution |

---

### In Seattle Crime Data

**Fields in `gold.dim_offense`:**
- `nibrs_group`: `"A"` or `"B"`
- `crime_against`: `"PERSON"` / `"PROPERTY"` / `"SOCIETY"`
- `offense_code`: FBI NIBRS code (e.g. `13B` = Shoplifting)
- `offense_description`: Human-readable description

**Usage in Dashboard:**
```python
# Filter Group A serious crimes
serious_crimes = crime_df[crime_df["nibrs_group"] == "A"]

# Filter crimes against persons
violent_crimes = crime_df[crime_df["crime_against"] == "PERSON"]
```

**Example NIBRS Codes:**
- `09A` - Murder and Nonnegligent Manslaughter (Group A, Person)
- `11A` - Rape (Group A, Person)
- `13B` - Shoplifting (Group A, Property)
- `23A` - Larceny-Theft (Group A, Property)
- `35A` - Drug/Narcotic Violations (Group A, Society)
- `90C` - Disorderly Conduct (Group B, Society)

> **Source:** FBI NIBRS User Manual v3.0 (2019)
