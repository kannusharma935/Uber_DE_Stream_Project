# 🚗 Uber End-to-End Data Engineering Project (Azure + Databricks)

A complete, production-style **real-time data engineering pipeline** built on **Azure** and **Databricks**, simulating an Uber-like ride data platform. The project covers everything from event ingestion to transformation, streaming, dimensional modeling, and a consumption-ready STAR schema.

---

## 📐 Architecture Diagram

![Uber Data Engineering Architecture](ScreenShot/uber_data_engineering_architecture_v3.svg)

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Event Streaming** | Azure Event Hubs | Kafka-compatible real-time event ingestion |
| **Orchestration** | Azure Data Factory (ADF) | Pipeline scheduling & ingestion automation |
| **Storage** | Azure Data Lake Storage Gen2 | Raw, Silver, and Gold data zones |
| **Processing** | Apache Spark / PySpark | Distributed data transformation |
| **Streaming** | Spark Structured Streaming | Real-time stream processing |
| **Compute** | Azure Databricks | Managed Spark platform |
| **Modeling** | STAR Schema | Dimensional data modeling for analytics |
| **SCD Handling** | PySpark SCD Logic | Slowly Changing Dimensions (Type 1/2) |

---

## 📦 Project Components

### 1. 🔌 Data Ingestion — WebApp → Azure Event Hub
- A **web application** generates real-time Uber-like ride events.
- Events are published to **Azure Event Hubs**, which provides a Kafka-compatible interface for high-throughput ingestion.

### 2. 🏭 Azure Data Factory — Ingestion Pipelines
- **ADF pipelines** are built to move raw data from Event Hubs into **Azure Data Lake Gen2**.
- Pipelines are parameterized and designed for reusability across multiple data sources.

### 3. ⚡ PySpark Structured Streaming — Real-Time Processing
- Databricks notebooks consume the streaming data using **Spark Structured Streaming**.
- A **metadata-driven streaming framework** is implemented to handle multiple tables/entities dynamically without duplicating code.

### 4. 🔄 Slowly Changing Dimensions (SCD)
- SCD logic is implemented in PySpark to handle historical changes in dimension data.
- Supports **Type 1** (overwrite) and **Type 2** (versioning with effective dates) patterns.

### 5. ⭐ STAR Schema Data Model
- Final data is modeled into a **STAR schema** with:
  - **Fact Table**: Trip/transaction-level events
  - **Dimension Tables**: Driver, Rider, Location, Time, Payment, etc.
- Optimized for analytical querying and BI consumption.

---

## 🗂️ Project Structure

```
uber-data-engineering/
│
├── ingestion/
│   ├── webapp_to_eventhub/        # Web app → Event Hub producer scripts
│   └── adf_pipelines/             # Azure Data Factory pipeline JSONs
│
├── streaming/
│   ├── pyspark_structured_streaming.py   # Base streaming notebook
│   └── metadata_driven_streaming.py      # Dynamic multi-table streaming
│
├── transformations/
│   ├── scd_type1.py               # SCD Type 1 logic
│   └── scd_type2.py               # SCD Type 2 logic
│
├── data_modeling/
│   ├── star_schema.py             # STAR schema creation
│   ├── fact_trips.py              # Fact table logic
│   └── dim_*.py                   # Dimension table scripts
│
├── config/
│   └── metadata_config.json       # Metadata config for streaming framework
│
└── README.md
```

---

## 🚀 How to Run

### Prerequisites
- Azure subscription (free tier works)
- Azure Databricks workspace
- Azure Data Factory instance
- Azure Event Hubs namespace
- Azure Data Lake Storage Gen2

### Setup Steps

1. **Provision Azure Resources**
   - Create an Azure Event Hub namespace and hub
   - Create an ADLS Gen2 storage account with hierarchical namespace enabled
   - Set up Azure Databricks workspace
   - Set up Azure Data Factory

2. **Configure the Web App Producer**
   - Update Event Hub connection strings in the config
   - Run the webapp producer to start sending events

3. **Deploy ADF Pipelines**
   - Import the ADF pipeline JSON templates
   - Configure linked services for Event Hub and ADLS

4. **Run Databricks Notebooks**
   - Mount ADLS to Databricks
   - Run structured streaming notebooks
   - Execute SCD transformation scripts
   - Build and populate the STAR schema

---

## 🧠 Key Concepts Covered

- ✅ Apache Kafka fundamentals (via Azure Event Hubs)
- ✅ Azure Event Hub architecture and setup
- ✅ Azure Data Factory — linked services, datasets, pipelines
- ✅ Databricks workspace navigation and cluster setup
- ✅ PySpark DataFrame & Streaming APIs
- ✅ Metadata-driven pipeline design pattern
- ✅ Slowly Changing Dimensions (SCD Type 1 & 2)
- ✅ Dimensional data modeling (STAR Schema)
- ✅ Data Lakehouse architecture (Bronze / Silver / Gold)

---

## 📊 Data Flow Summary

```
Raw Events (WebApp)
    ↓
Azure Event Hub (Kafka-compatible streaming)
    ↓
Azure Data Factory (Ingestion & orchestration)
    ↓
ADLS Gen2 — Raw/Bronze Zone
    ↓
Databricks + PySpark Structured Streaming
    ↓
ADLS Gen2 — Silver Zone (Cleaned & Enriched)
    ↓
SCD Transformations (Type 1 / Type 2)
    ↓
STAR Schema — Gold Zone (Analytics-Ready)
```

---

## 📚 References

- 📺 [Full Tutorial by Ansh Lamba](https://www.youtube.com/watch?v=5KIbhHo6GJA)
- 📁 [Original Code Repository](https://github.com/anshlambagit)
- 🗺️ [Data Engineer Roadmap](https://github.com/anshlambagit/Data_Engineer_Roadmap)

---

## 🙌 Acknowledgements

Project built following the **Uber End-To-End Data Engineering Project (2026)** tutorial by [Ansh Lamba](https://www.youtube.com/@anshlambajsr). All credit for the original project design and architecture goes to the author.

---

## 📄 License

This project is for educational purposes. Feel free to fork and build upon it.
