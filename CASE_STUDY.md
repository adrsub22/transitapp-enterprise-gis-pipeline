# Portfolio Case Study  
## Automated Mobility Data → Enterprise GIS Publishing Pipeline

---

## 🚍 Overview

This project demonstrates a production-ready GIS and data engineering pipeline
that automates the ingestion, transformation, and publishing of mobility trip
data into ArcGIS Online.

The system was designed to support transit planning and operations workflows
where data is large, frequently updated, and must be published reliably to
enterprise GIS platforms.

---

## ❗ Problem

Transit agencies increasingly rely on detailed trip-level mobility data, but
common challenges include:

- Continuously growing datasets
- Fragile GIS publishing workflows
- Duplicate records caused by rolling reprocessing
- Manual ArcGIS Pro steps that do not scale
- Difficulty automating ArcGIS Online updates reliably

These challenges often result in outdated dashboards and time-intensive
maintenance.

---

## 💡 Solution

I designed and implemented a modular pipeline that:

- Ingests new trip data incrementally
- Prevents duplication using deterministic hashing
- Aggregates analytics in SQL for performance and consistency
- Uses materialized tables to stabilize GIS automation
- Automatically overwrites ArcGIS Online hosted layers

The result is a system that can run unattended and reliably refresh GIS outputs.

---

## 🏗 Architecture

```text
Raw Trip Data
     ↓
mobility.raw_leg_trips
     ↓
mobility.LegTrips_Clean
     ↓
Rolling SQL Aggregates
     ↓
Materialized GIS Tables (SDE)
     ↓
Local FGDB Staging (ArcGIS Pro)
     ↓
ArcGIS Online Hosted Layers
```

---

## 🔑 Key Technical Decisions

- **Incremental processing**  
  Rolling time windows with overlap ensure late-arriving data is captured.

- **Idempotent upserts**  
  SHA-256 row hashing prevents duplicate inserts during reprocessing.

- **Materialized GIS outputs**  
  Avoids publishing instability associated with SQL views and query layers.

- **Local staging for ArcPy**  
  Improves reliability of geoprocessing and overwrite publishing workflows.

- **Environment-driven configuration**  
  All paths, credentials, and service names are externalized.

---

## 📊 Outputs

- Daily origin–destination flow lines by route
- Transfer hotspot point layers
- Walk access and egress line layers
- Automatically refreshed ArcGIS Online dashboards and maps

---

## 🛠 Tools & Technologies

- Python (pandas, GeoPandas, ArcPy)
- SQL Server (stored procedures, window functions, hashing)
- ArcGIS Pro
- ArcGIS Online
- Enterprise Geodatabases (SDE)

---

## 🎯 Impact

This pipeline replaces manual GIS publishing with a fully automated workflow
that:

- Reduces analyst time spent on repetitive updates
- Improves data consistency and reliability
- Scales to millions of trip records
- Supports planning, operations, and public-facing use cases

---

## 🚦 Why This Matters

This project reflects how modern transit agencies can combine data engineering
and enterprise GIS automation to deliver timely insights while reducing
operational overhead.

The patterns demonstrated here are directly applicable to enterprise GIS
environments supporting transit and mobility analytics.
