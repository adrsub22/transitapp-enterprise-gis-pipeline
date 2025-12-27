/* =====================================================================
   FILE: sql/03_run_refresh.sql
   PURPOSE:
     - Convenience script: bind, execute, and validate outputs
   ===================================================================== */

EXEC sp_refreshsqlmodule 'mobility.usp_Refresh_Rolling31_Aggregates';
GO

-- Example run: last 31 days, no region filter
EXEC mobility.usp_Refresh_Rolling31_Aggregates
    @days_back = 31,
    @region_prefix = NULL;
GO

-- Basic checks
SELECT MIN(trip_date) AS min_date, MAX(trip_date) AS max_date, COUNT(*) AS n_rows
FROM mobility.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31;

SELECT COUNT(*) AS n_rows
FROM mobility.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31;

SELECT TOP 10 *
FROM mobility.ArcGIS_Walk_Egress_Daily_Rolling31
ORDER BY trip_date DESC;

