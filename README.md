# Uber End-to-End Data Engineering Project (Azure + Databricks)

A production-style **real-time data engineering pipeline** on **Azure** and **Databricks**, modeled after an Uber-style ride platform. Data flows from synthetic event producers through Event Hubs and ADF into a lakehouse, then through **Databricks Lakeflow Pipelines** (DLT) for streaming transformation, dimensional modeling, and a consumption-ready **STAR schema**.

---

## Architecture Diagram

![Uber Data Engineering Architecture](./ScreenShot/uber_data_engineering_architecture_v3.svg)

End-to-end view: producers → Event Hubs → ADF → ADLS Gen2 (Bronze) → Databricks streaming & DLT → Silver/Gold dimensional model.

---

## Azure Data Factory Pipeline

![ADF Ingestion Pipeline](./ScreenShot/Pipeline.png)

ADF orchestrates ingestion from **Azure Event Hubs** into **ADLS Gen2** (raw/Bronze zone), landing events and reference JSON files before Databricks picks them up.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Event streaming** | Azure Event Hubs | Kafka-compatible real-time ingestion |
| **Producer** | Python + `azure-eventhub` | Generate and publish synthetic ride events |
| **Orchestration** | Azure Data Factory (ADF) | Automate Event Hub → ADLS ingestion |
| **Storage** | ADLS Gen2 + Delta Lake | Bronze, Silver, and Gold medallion zones |
| **Pipeline framework** | Databricks Lakeflow Pipelines (DLT) | Declarative streaming tables and flows |
| **Processing** | PySpark Structured Streaming | Continuous read from Kafka / Delta |
| **Modeling** | STAR schema + Auto CDC | SCD Type 1 & 2 dimensions + fact table |
| **Enrichment** | SQL + Jinja2 | Metadata-driven One Big Table (OBT) joins |

---

## Pipeline Overview

| Stage | Component | Location |
|---|---|---|
| 1. Produce events | Event Hub publisher | `Event_hub_Publisher/` |
| 2. Land in lake | ADF → ADLS Gen2 | See `ScreenShot/Pipeline.png` |
| 3. Bronze ingest | Kafka stream from Event Hub | `uber_rides_ingest/transformations/ingest.py` |
| 4. Silver staging | JSON parse + bulk/stream merge | `uber_rides_ingest/transformations/silver.py` |
| 5. Silver OBT | Join rides with dimension lookups | `uber_rides_ingest/transformations/silver_obt.sql` |
| 6. Gold model | STAR schema (dims + fact) | `uber_rides_ingest/transformations/model.py` |

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

JSON lookup tables seeded into `uber.bronze.*` and joined during Silver enrichment:

- Cities, vehicle types/makes, payment methods, ride statuses, cancellation reasons

Foreign keys in each ride event (`vehicle_type_id`, `pickup_city_id`, etc.) align with these tables.

### 3. Databricks Pipeline (`uber_rides_ingest/`)

Lakeflow Pipeline built with `@dp.table`, `@dp.append_flow`, and `create_auto_cdc_flow`.

#### Bronze — `ingest.py`

- Reads from Event Hub using the **Kafka protocol** (`kafka.bootstrap.servers`, SASL_SSL)
- Writes a streaming Delta table `rides_raw` with the raw JSON payload in a `rides` column

#### Silver — `silver.py`

- Defines `rides_schema` and parses JSON from `rides_raw`
- Creates streaming table `stg_rides` with two append flows:
  - **`rides_stream`** — live events from `rides_raw`
  - **`rides_bulk`** — initial/historical load from `bulk_rides`

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
    ↓
ADF → ADLS Gen2 (Bronze: raw JSON + mapping files)
    ↓
DLT ingest.py — Kafka stream → rides_raw
    ↓
DLT silver.py — JSON parse → stg_rides
    ↓
DLT silver_obt.sql — joins → silver_obt
    ↓
DLT model.py — Auto CDC → dim_* + fact (Gold / STAR)
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

### Step 2 — Ingest to the lake (ADF)

1. Deploy the ADF pipeline (see screenshot above).
2. Configure linked services for Event Hub and ADLS Gen2.
3. Confirm raw events and mapping JSONs land under the Bronze path.

### Step 3 — Seed bronze reference tables (Databricks)

1. Open `uber_rides_ingest/explorations/bronze_adls.py`.
2. Point the ADLS URLs at your mapping JSON files (or upload `Dataset_Initial/` to your lake).
3. Run the notebook to create `uber.bronze.map_*` tables.

### Step 4 — Deploy the DLT pipeline

1. In Databricks, create a **Lakeflow Pipeline** targeting `uber_rides_ingest/transformations/`.
2. Set pipeline configuration for Event Hub access (recommended: Databricks secrets instead of hardcoded connection strings):

   ```python
   spark.conf.set("connection_string", dbutils.secrets.get("uber", "eventhub-conn-str"))
   ```

3. Update `EH_NAMESPACE`, `EH_NAME`, and connection settings in `ingest.py` to match your environment.
4. Start the pipeline — it will materialize `rides_raw` → `stg_rides` → `silver_obt` → dimension and fact tables.

### Step 5 — Validate

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

- Event-driven architecture with Azure Event Hubs (Kafka API)
- Medallion architecture — Bronze / Silver / Gold on Delta Lake
- Databricks Lakeflow Pipelines (DLT) with streaming tables and append flows
- Metadata-driven SQL generation (Jinja2 for OBT joins)
- Slowly Changing Dimensions — Auto CDC Type 1 and Type 2
- Dimensional modeling — STAR schema with fact and dimension tables

---

## References

- [Full Tutorial by Ansh Lamba](https://www.youtube.com/watch?v=5KIbhHo6GJA)
- [Original Code Repository](https://github.com/anshlambagit)
- [Data Engineer Roadmap](https://github.com/anshlambagit/Data_Engineer_Roadmap)

---

## Acknowledgements

Built following the **Uber End-To-End Data Engineering Project (2026)** tutorial by [Ansh Lamba](https://www.youtube.com/@anshlambajsr). Credit for the original pipeline design and architecture goes to the author.

---

## License

Educational use. Fork and extend as needed.
