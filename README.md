# Mobility Analytics GIS Pipeline

> **TL;DR**  
> An automated, production-style pipeline that ingests mobility trip data,
> builds rolling analytics in SQL Server, and publishes ArcGIS Online layers
> using Python, ArcGIS Pro, and enterprise GIS best practices.

---

## 🚍 Overview

This repository demonstrates an end-to-end GIS and data engineering pipeline
designed for transit and mobility analytics. The system automates the full
workflow from raw trip data ingestion to enterprise GIS publishing, minimizing
manual intervention while improving reliability and scalability.

The pipeline is modular, incremental, and designed to operate safely in
enterprise environments where data volumes are large and GIS publishing can
be fragile.

---

## 🏗 Architecture (High Level)

```text
Raw Trip Data
     ↓
SQL Server (Raw + Clean Tables)
     ↓
Rolling Aggregates & Materialized GIS Tables
     ↓
ArcGIS Pro (FGDB Staging)
     ↓
ArcGIS Online (Hosted Feature Layers)
```

---

## ⚙️ Pipeline Modules

### Module 1 — Raw Data Ingest  
**ingest_portal_to_sql_raw.py**

- Downloads new trip data files incrementally
- Cleans coordinate fields
- Computes distance metrics (Euclidean, Manhattan)
- Spatially joins start/end points to Census Block Groups
- Appends results to:

```text
mobility.raw_leg_trips
```

---

### Module 2 — Transform & Aggregate  
**transform_incremental_clean.py**

- Incrementally reads from mobility.raw_leg_trips
- Treats trip identifiers as strings to preserve integrity
- Computes deterministic row hashes to prevent duplication
- Inserts only new records into:

```text
mobility.LegTrips_Clean
```

- Executes a SQL stored procedure to rebuild rolling analytics and GIS outputs:

```text
mobility.usp_Refresh_Rolling31_Aggregates
```

---

### Module 3 — Publish to ArcGIS Online  
**publish_agol_overwrite.py**

- Copies materialized GIS tables from SDE to a local File Geodatabase
- Builds feature classes using:
  - XYToLine (origin–destination flows)
  - XYTableToPoint (transfer hotspots)
- Overwrites existing hosted feature layers in ArcGIS Online
- Supports DRY_RUN mode for safe testing

---

## ▶️ How to Run

### Run the full pipeline
```bash
python src/main.py
```

### Run modules individually
```bash
python src/ingest_portal_to_sql_raw.py
python src/transform_incremental_clean.py
python src/publish_agol_overwrite.py
```

---

## 🧠 Why Materialized ArcGIS Tables?

Publishing directly from SQL views or query layers can cause instability in
ArcGIS Pro and ArcPy workflows.

This pipeline intentionally:
1. Aggregates data in SQL Server
2. Writes results to materialized tables
3. Stages data locally before feature creation

This approach improves:
- Publishing reliability
- Automation repeatability
- Performance for large datasets

---

## 🔒 Configuration & Security

All credentials, paths, and service names are provided via environment variables.

A complete template is included at:

```text
examples/example_env.template
```

No proprietary data or credentials are stored in this repository.

---

## 🎯 Intended Use

This project is designed to showcase:
- Enterprise GIS automation
- SQL + Python ETL pipelines
- Incremental data processing
- ArcGIS Pro and ArcGIS Online integration

The patterns demonstrated here are broadly applicable to transit agencies,
planning organizations, and enterprise GIS teams.

