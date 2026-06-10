# Uber End-to-End Data Engineering Project (Azure + Databricks)

A production-style **real-time data engineering pipeline** on **Azure** and **Databricks**, modeled after an Uber-style ride platform. The design uses **two complementary Bronze ingestion paths**:

- **Azure Data Factory (ADF)** — lands the **initial bulk load** and **mapping tables** (`map_cities`, `map_vehicle_types`, etc.) into the Bronze layer via ADLS Gen2.
- **Databricks Lakeflow Pipeline (SDP)** — streams **live ride events** directly from **Azure Event Hubs** (Kafka API) into Delta tables.

Downstream, SDP handles Silver enrichment, SCD logic, and the Gold **STAR schema**.

---

## Architecture Diagram

![Uber Data Engineering Architecture](./ScreenShot/uber_data_engineering_architecture_v3.svg)

Broader Azure design: producers → Event Hubs, with **ADF** feeding initial load and reference data into Bronze (ADLS), and **SDP** consuming the live Event Hub stream in parallel. Both paths converge in Silver/Gold transformation inside Databricks.

---

## Databricks Lakeflow Pipeline (SDP)

![Databricks SDP Pipeline](./ScreenShot/Pipeline.png)

Visual DAG of the **Databricks Lakeflow Pipeline** (`uber_rides_ingest`) in the workspace UI. It shows how streaming tables and flows connect across the medallion layers — from `rides_raw` and `stg_rides` through `silver_obt` to the Gold **STAR schema** dimension and fact tables.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Event streaming** | Azure Event Hubs | Kafka-compatible real-time ingestion |
| **Producer** | Python + `azure-eventhub` | Generate and publish synthetic ride events |
| **Batch orchestration** | Azure Data Factory (ADF) | Initial load + mapping tables → ADLS Bronze |
| **Storage** | ADLS Gen2 + Delta Lake | Bronze, Silver, and Gold medallion zones |
| **Streaming pipeline** | Databricks Lakeflow Pipeline (SDP) | Event Hub → Bronze streaming + Silver/Gold DAG |
| **Processing** | PySpark Structured Streaming | Continuous read from Kafka / Delta |
| **Modeling** | STAR schema + Auto CDC | SCD Type 1 & 2 dimensions + fact table |
| **Enrichment** | SQL + Jinja2 | Metadata-driven One Big Table (OBT) joins |

---

## Ingestion Architecture (Dual Path)

| Path | Source | Tool | Bronze targets |
|---|---|---|---|
| **Batch / reference** | `Dataset_Initial/` JSON files + bulk ride history | **ADF** → ADLS Gen2 | `bulk_rides`, `map_cities`, `map_vehicle_types`, `map_vehicle_makes`, `map_payment_methods`, `map_ride_statuses`, `map_cancellation_reasons` |
| **Real-time streaming** | Live ride events | **SDP** ← Event Hub (Kafka) | `rides_raw` |

ADF handles one-time and periodic batch loads into the lake. SDP reads the Event Hub stream continuously — no ADF hop for live events.

## Pipeline Overview

| Stage | Component | Location |
|---|---|---|
| 1. Produce events | Event Hub publisher | `Event_hub_Publisher/` |
| 2a. Bronze (batch) | ADF → ADLS → mapping tables + `bulk_rides` | ADF pipelines + `explorations/bronze_adls.py` |
| 2b. Bronze (stream) | Event Hub → `rides_raw` | `transformations/ingest.py` |
| 3. Silver staging | Parse JSON + merge bulk & stream → `stg_rides` | `transformations/silver.py` |
| 4. Silver OBT | Join `stg_rides` with bronze lookups → `silver_obt` | `transformations/silver_obt.sql` |
| 5. Gold model | STAR schema (dims + fact) | `transformations/model.py` |

See `ScreenShot/Pipeline.png` for the Databricks SDP pipeline DAG (Bronze stream through Gold).

---

## Project Structure

```
Azure_DE_UBER_Stream/
│
├── Event_hub_Publisher/              # Synthetic ride event producer
│   ├── connection.py                 # Publishes JSON events to Event Hub
│   ├── data.py                       # Faker-based ride generator
│   ├── files_array.json
│   └── pyproject.toml
│
├── Dataset_Initial/                  # Reference / dimension seed data
│   ├── map_cities.json
│   ├── map_vehicle_types.json
│   ├── map_vehicle_makes.json
│   ├── map_payment_methods.json
│   ├── map_ride_statuses.json
│   └── map_cancellation_reasons.json
│
├── uber_rides_ingest/                # Databricks Lakeflow Pipeline (DLT)
│   ├── transformations/
│   │   ├── ingest.py                 # Event Hub → rides_raw (Kafka stream)
│   │   ├── silver.py                 # rides_raw → stg_rides (parsed JSON)
│   │   ├── silver_obt.sql            # stg_rides + lookups → silver_obt
│   │   └── model.py                  # silver_obt → STAR schema tables
│   └── explorations/                 # Dev / validation notebooks
│       ├── event_hub.py              # Test Kafka connection to Event Hub
│       ├── bronze_adls.py            # Load mapping JSONs into uber.bronze.*
│       ├── silver.py                 # Inspect parsed ride schema
│       └── silver_obt.py             # Jinja-driven OBT SQL prototype
│
├── ScreenShot/
│   ├── uber_data_engineering_architecture_v3.svg
│   └── Pipeline.png
│
└── README.md
```

---

## Component Details

### 1. Event Hub Publisher (`Event_hub_Publisher/`)

Generates realistic Uber ride confirmation JSON and publishes to Azure Event Hubs.

| File | Role |
|---|---|
| `data.py` | Builds ride records with passenger, driver, vehicle, fare, and FK fields |
| `connection.py` | Sends events via `EventHubProducerClient` |

```bash
cd Event_hub_Publisher
# .env: CONNECTION_STRING, EVENT_HUBNAME
uv sync
python connection.py
```

### 2. Reference Mappings (`Dataset_Initial/`)

JSON lookup tables ingested by **ADF** into ADLS Gen2, then materialized as `uber.bronze.map_*` Delta tables (via `bronze_adls.py` or ADF copy activity):

- Cities, vehicle types/makes, payment methods, ride statuses, cancellation reasons

Foreign keys in each ride event (`vehicle_type_id`, `pickup_city_id`, etc.) align with these tables and are joined in `silver_obt.sql`.

### 3. Azure Data Factory (ADF)

ADF orchestrates **batch ingestion into the Bronze layer**:

- **Initial load** — historical ride data landed as `bulk_rides` in Bronze (consumed by the `rides_bulk` append flow in `silver.py`)
- **Mapping tables** — `Dataset_Initial/` JSON files copied from ADLS into `uber.bronze.map_*` tables

Live streaming events bypass ADF and are read directly by SDP from Event Hub.

### 4. Databricks Pipeline (`uber_rides_ingest/`)

Lakeflow Pipeline built with `@dp.table`, `@dp.append_flow`, and `create_auto_cdc_flow`.

#### Bronze (streaming) — `ingest.py`

- Reads **directly from Event Hub** using the Kafka protocol (`kafka.bootstrap.servers`, SASL_SSL) — no ADF in this path
- Writes streaming Delta table `rides_raw` with the raw JSON payload in a `rides` column

#### Silver — `silver.py`

- Defines `rides_schema` and parses JSON from `rides_raw`
- Creates streaming table `stg_rides` with two append flows that merge both ingestion paths:
  - **`rides_stream`** — live events from `rides_raw` (Event Hub via SDP)
  - **`rides_bulk`** — initial/historical load from `bulk_rides` (ADF → ADLS Bronze)

#### Silver OBT — `silver_obt.sql`

- Builds `silver_obt` (One Big Table) by joining `stg_rides` with all bronze mapping tables
- Applies a **3-minute watermark** on `booking_timestamp` for late-arriving events
- Denormalizes ride facts with vehicle, payment, city, and status attributes

#### Gold — `model.py`

Derives a STAR schema from `silver_obt` using Databricks **Auto CDC**:

| Table | SCD Type | Key attributes |
|---|---|---|
| `dim_passenger` | Type 1 | passenger_id, name, email, phone |
| `dim_driver` | Type 1 | driver_id, name, rating, license |
| `dim_vehicle` | Type 1 | vehicle_id, make, type, model, plate |
| `dim_payment` | Type 1 | payment_method_id, method, card flags |
| `dim_booking` | Type 1 | ride_id, locations, timestamps, status |
| `dim_location` | Type 2 | pickup_city_id, city, region (versioned) |
| `fact` | Type 1 | ride measures: distance, fare, duration, rating |

---

## Data Flow

```
Event_hub_Publisher (synthetic JSON)
    ↓
Azure Event Hubs
    │
    ├─ [real-time] ──► SDP ingest.py (Kafka) ──► rides_raw ────┐
                                                   (bronze)    │                         
                                                               │         
 Github Repo                                                   │
    └─ [batch via ADF] ──► ADLS Gen2 Bronze                    │
              ├── bulk_rides ──────────────────────────────────┤
              └── map_*.json (mapping tables)                  │
                                                               ▼
                                              SDP silver.py → stg_rides
                                                              │
                              silver_obt.sql (joins map_* tables)
                                                              ▼
                                                         silver_obt
                                                              ▼
                                              model.py → dim_* + fact (Gold)
```

---

## How to Run

### Step 1 — Publish test events

1. Create an Event Hubs namespace and hub in Azure.
2. Add `Event_hub_Publisher/.env`:

   ```env
   CONNECTION_STRING=Endpoint=sb://...
   EVENT_HUBNAME=your-event-hub-name
   ```

3. Run `python connection.py` to publish ride events.

### Step 2 — Ingest initial load & mapping tables (ADF)

1. Upload `Dataset_Initial/` JSON files and any bulk ride history to ADLS Gen2.
2. Deploy ADF pipelines to copy data into the Bronze layer:
   - Mapping tables → `uber.bronze.map_*`
   - Historical rides → `uber.bronze.bulk_rides`
3. Alternatively, run `explorations/bronze_adls.py` in Databricks to load mapping JSONs from ADLS into Delta tables.

### Step 3 — Deploy the Databricks SDP pipeline

1. In Databricks, create a **Lakeflow Pipeline** targeting `uber_rides_ingest/transformations/` (see `ScreenShot/Pipeline.png` for the expected DAG).
2. Set pipeline configuration for Event Hub access (recommended: Databricks secrets instead of hardcoded connection strings):

   ```python
   spark.conf.set("connection_string", dbutils.secrets.get("uber", "eventhub-conn-str"))
   ```

3. Update `EH_NAMESPACE`, `EH_NAME`, and connection settings in `ingest.py` to match your environment.
4. Start the pipeline — SDP will stream from Event Hub into `rides_raw`, merge with ADF-loaded `bulk_rides` in `stg_rides`, then build `silver_obt` and the Gold dimension/fact tables.

### Step 4 — Validate

Use the exploration notebooks to inspect intermediate tables:

- `event_hub.py` — verify Kafka stream connectivity
- `silver.py` — check parsed ride schema
- `silver_obt.py` — prototype and debug OBT joins (Jinja template)

---

## Sample Event Fields

Each ride confirmation JSON includes:

- **Keys:** `ride_id`, `passenger_id`, `driver_id`, `vehicle_id`, location IDs
- **Dimension FKs:** `vehicle_type_id`, `payment_method_id`, `ride_status_id`, `pickup_city_id`, etc.
- **Measures:** `distance_miles`, `duration_minutes`, `total_fare`, `tip_amount`, `surge_multiplier`
- **Timestamps:** `booking_timestamp`, `pickup_timestamp`, `dropoff_timestamp`

---

## Key Concepts Covered

- Hybrid ingestion — ADF for batch/reference data, SDP for real-time Event Hub streaming
- Event-driven architecture with Azure Event Hubs (Kafka API)
- Medallion architecture — Bronze / Silver / Gold on Delta Lake
- Databricks Lakeflow Pipeline (SDP) with streaming tables and append flows
- Azure Data Factory orchestration for initial load and dimension seeding
- Metadata-driven SQL generation (Jinja2 for OBT joins)
- Slowly Changing Dimensions — Auto CDC Type 1 and Type 2
- Dimensional modeling — STAR schema with fact and dimension tables


---

## License

Educational use. Fork and extend as needed.
