# transitapp-enterprise-gis-pipeline
Pull Transit App O-D data from portal to database to AGOL

End-to-End Pipeline Overview

This repository demonstrates a production-style GIS and data engineering pipeline that ingests raw mobility trip data, transforms it into analytics-ready tables, and automatically publishes geospatial layers to ArcGIS Online.

The pipeline is intentionally modular and designed to be:

Incremental (safe to run daily)

Idempotent (re-processing overlapping data does not create duplicates)

GIS-stable (avoids common ArcGIS Pro and ArcPy automation pitfalls)

Enterprise-ready (SQL Server + SDE + AGOL overwrite workflows)

Pipeline Modules
Module 1 — Raw Data Ingest

ingest_portal_to_sql_raw.py

Downloads new CSV files from a secure portal

Cleans coordinate fields and computes:

Euclidean distance

Manhattan distance

Spatially joins trip start/end points to Census Block Groups

Appends results to:

mobility.raw_leg_trips


This table acts as an immutable, queryable “raw but spatially enriched” data layer.

Module 2 — Incremental Transform & Aggregation

transform_incremental_clean.py

Incrementally reads from mobility.raw_leg_trips

Normalizes datatypes (IDs treated as strings)

Computes a deterministic row hash for deduplication

Inserts only new records into:

mobility.LegTrips_Clean


Executes:

mobility.usp_Refresh_Rolling31_Aggregates


This stored procedure:

Rebuilds rolling 31-day analytics tables

Creates materialized ArcGIS-ready tables optimized for publishing

Module 3 — Automated ArcGIS Online Publishing

publish_agol_overwrite.py

Copies materialized SDE tables to a local File Geodatabase (staging)

Builds feature classes using:

XYToLine (origin–destination flows)

XYTableToPoint (transfer hotspots)

Overwrites existing Hosted Feature Layers in ArcGIS Online

This approach avoids instability commonly encountered when publishing directly from:

SQL views

Query layers

Live database connections

A DRY_RUN option allows safe testing without overwriting live services.

Orchestrating the Pipeline
Main Entry Point

main.py

Runs all modules in sequence with guardrails:

Ingest raw data

Transform + refresh rolling aggregates

Publish hosted layers (optional)

Execution logic includes:

Skip publishing if no new data is available

Disable publishing entirely with environment flags

Clean error handling between stages

Running the Pipeline
Run everything
python src/main.py

Run individual modules
python src/ingest_portal_to_sql_raw.py
python src/transform_incremental_clean.py
python src/publish_agol_overwrite.py

Environment Configuration

All configuration is externalized via environment variables.
A full template is provided at:

examples/example_env.template


Key categories include:

Portal credentials

SQL Server connection info

ArcGIS Pro / SDE paths

AGOL hosted service names

Safety flags (DRY_RUN, PUBLISH_ENABLED)

No credentials or agency-specific identifiers are hard-coded.

Why Materialized ArcGIS Tables?

ArcGIS automation can be unreliable when publishing from:

SQL views

Query layers

Complex joins

This pipeline intentionally:

Aggregates data in SQL

Writes results into materialized tables

Stages them locally before feature creation

This pattern:

Improves publishing stability

Enables repeatable automation

Reduces ArcPy runtime failures

Produces predictable ObjectIDs for overwrite workflows

Intended Use

This project is designed to showcase:

Enterprise GIS automation

SQL + Python ETL pipelines

Incremental data processing

ArcGIS Pro and ArcGIS Online integration

All code has been generalized for public sharing and does not expose proprietary data or systems.
