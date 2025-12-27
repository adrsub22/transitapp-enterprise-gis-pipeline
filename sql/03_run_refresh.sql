/* =====================================================================
   FILE: sql/03_run_refresh.sql
   PURPOSE:
     - Convenience script for refreshing + validating outputs.
   ===================================================================== */

-- Ensure SQL Server can re-bind the module correctly
EXEC sp_refreshsqlmodule 'transitapp.usp_Refresh_Rolling31_Aggregates';
GO

-- Run refresh (example: last 31 days; optional region filter)
EXEC transitapp.usp_Refresh_Rolling31_Aggregates
    @days_back = 31,
    @region_prefix = NULL;   -- e.g. '48029' to scope to a region
GO

-- Quick validations (row counts + recent dates)
SELECT TOP 10 * 
FROM transitapp.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31
ORDER BY trip_date DESC;

SELECT COUNT(*) AS n_rows
FROM transitapp.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31;

SELECT MIN(trip_date) AS min_date, MAX(trip_date) AS max_date
FROM transitapp.ArcGIS_Walk_Egress_Daily_Rolling31;
