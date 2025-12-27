/* =====================================================================
   FILE: sql/01_schema_arcgis_tables.sql
   PURPOSE:
     - One-time schema setup for ArcGIS-friendly "materialized" tables.
     - Adds a stable hash key used for hotspot grouping (safe for joins).
     - Creates simple tables designed to be consumed by ArcGIS Pro / ArcPy
       without query-layer driver quirks (e.g., GetCount, XY tools).

   NOTES:
     - Database/table names are illustrative.
     - Provider/service labels are generalized to avoid agency-specific detail.
   ===================================================================== */

-- OPTIONAL (keep commented in public repo)
-- USE <YOUR_DATABASE>;
-- GO

/* ---------------------------------------------------------------
   1) One-time cleanup: drop deprecated column (if it exists)
   --------------------------------------------------------------- */
IF OBJECT_ID('transitapp.Walk_AccessEgress_Daily','U') IS NOT NULL
BEGIN
    IF COL_LENGTH('transitapp.Walk_AccessEgress_Daily', 'service_name') IS NOT NULL
    BEGIN
        PRINT 'Dropping deprecated column transitapp.Walk_AccessEgress_Daily.service_name ...';
        ALTER TABLE transitapp.Walk_AccessEgress_Daily DROP COLUMN service_name;
    END
END
GO

/* ---------------------------------------------------------------
   2) Ensure hotspot_hash exists (Route-level hotspots)
   WHY:
     - Hashing produces a stable, compact key for grouping/joining
       across refreshes (good for GIS publishing + dashboards).
   --------------------------------------------------------------- */
IF OBJECT_ID('transitapp.Transfer_Hotspots_Daily_Route','U') IS NOT NULL
BEGIN
    IF COL_LENGTH('transitapp.Transfer_Hotspots_Daily_Route', 'hotspot_hash') IS NULL
    BEGIN
        PRINT 'Adding hotspot_hash to transitapp.Transfer_Hotspots_Daily_Route ...';
        ALTER TABLE transitapp.Transfer_Hotspots_Daily_Route
        ADD hotspot_hash varbinary(32) NULL;
    END

    PRINT 'Backfilling hotspot_hash on transitapp.Transfer_Hotspots_Daily_Route ...';
    UPDATE transitapp.Transfer_Hotspots_Daily_Route
    SET hotspot_hash = HASHBYTES('SHA2_256', CONVERT(varbinary(max), hotspot_key))
    WHERE hotspot_hash IS NULL;

    BEGIN TRY
        ALTER TABLE transitapp.Transfer_Hotspots_Daily_Route
        ALTER COLUMN hotspot_hash varbinary(32) NOT NULL;
    END TRY
    BEGIN CATCH
        PRINT 'NOTE: Could not enforce NOT NULL on Route.hotspot_hash (existing NULLs?)';
    END CATCH
END
GO

/* ---------------------------------------------------------------
   3) Ensure hotspot_hash exists (RoutePair-level hotspots)
   --------------------------------------------------------------- */
IF OBJECT_ID('transitapp.Transfer_Hotspots_Daily_RoutePair','U') IS NOT NULL
BEGIN
    IF COL_LENGTH('transitapp.Transfer_Hotspots_Daily_RoutePair', 'hotspot_hash') IS NULL
    BEGIN
        PRINT 'Adding hotspot_hash to transitapp.Transfer_Hotspots_Daily_RoutePair ...';
        ALTER TABLE transitapp.Transfer_Hotspots_Daily_RoutePair
        ADD hotspot_hash varbinary(32) NULL;
    END

    PRINT 'Backfilling hotspot_hash on transitapp.Transfer_Hotspots_Daily_RoutePair ...';
    UPDATE transitapp.Transfer_Hotspots_Daily_RoutePair
    SET hotspot_hash = HASHBYTES('SHA2_256', CONVERT(varbinary(max), hotspot_key))
    WHERE hotspot_hash IS NULL;

    BEGIN TRY
        ALTER TABLE transitapp.Transfer_Hotspots_Daily_RoutePair
        ALTER COLUMN hotspot_hash varbinary(32) NOT NULL;
    END TRY
    BEGIN CATCH
        PRINT 'NOTE: Could not enforce NOT NULL on RoutePair.hotspot_hash (existing NULLs?)';
    END CATCH
END
GO

/* ---------------------------------------------------------------
   4) ArcGIS "materialized" tables (stable for ArcGIS Pro / ArcPy)
   WHY:
     - Avoids query-layer and ODBC driver edge cases.
     - Ensures a numeric OID field for publishing workflows.
   --------------------------------------------------------------- */

-- 4a) Flows (Route) for ArcGIS
IF OBJECT_ID('transitapp.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31','U') IS NULL
BEGIN
    PRINT 'Creating transitapp.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31 ...';
    CREATE TABLE transitapp.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31 (
        oid INT NOT NULL,
        trip_date date NOT NULL,
        route_short_name nvarchar(50) NULL,
        Origin_BG varchar(12) NULL,
        Dest_BG varchar(12) NULL,
        service_group nvarchar(50) NULL,
        provider_name nvarchar(200) NULL,
        leg_count int NULL,
        trip_count int NULL,
        transfer_trip_count int NULL,
        avg_manhattan_mi float NULL,
        avg_euclidean_mi float NULL,
        avg_leg_minutes float NULL,
        mean_start_lon float NULL,
        mean_start_lat float NULL,
        mean_end_lon float NULL,
        mean_end_lat float NULL,
        CONSTRAINT PK_ArcGIS_Flow_BG_BG_Daily_Route_Rolling31 PRIMARY KEY CLUSTERED (oid)
    );
END
GO

-- 4b) Transfer Hotspots (Route) for ArcGIS
IF OBJECT_ID('transitapp.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31','U') IS NULL
BEGIN
    PRINT 'Creating transitapp.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31 ...';
    CREATE TABLE transitapp.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31 (
        oid INT NOT NULL,
        trip_date date NOT NULL,
        route_short_name nvarchar(50) NULL,
        from_route_short_name nvarchar(50) NULL,
        to_route_short_name nvarchar(50) NULL,
        transfer_type nvarchar(50) NULL,
        hotspot_key nvarchar(500) NULL,
        hotspot_hash varbinary(32) NULL,
        provider_name nvarchar(200) NULL,
        start_stop_name nvarchar(200) NULL,
        transfer_events int NULL,
        transfer_trips int NULL,
        avg_leg_minutes float NULL,
        mean_lon float NULL,
        mean_lat float NULL,
        CONSTRAINT PK_ArcGIS_TP_Daily_Route_Rolling31 PRIMARY KEY CLUSTERED (oid)
    );
END
GO

-- 4c) Walk Egress for ArcGIS (EGRESS only is used for dashboarding here)
IF OBJECT_ID('transitapp.ArcGIS_Walk_Egress_Daily_Rolling31','U') IS NULL
BEGIN
    PRINT 'Creating transitapp.ArcGIS_Walk_Egress_Daily_Rolling31 ...';
    CREATE TABLE transitapp.ArcGIS_Walk_Egress_Daily_Rolling31 (
        oid INT NOT NULL,
        trip_date date NOT NULL,
        walk_type nvarchar(10) NOT NULL,          -- ACCESS / EGRESS
        related_service nvarchar(50) NULL,
        related_route nvarchar(50) NULL,
        stop_key nvarchar(500) NULL,
        stop_name nvarchar(200) NULL,
        provider_name nvarchar(200) NULL,
        walk_leg_count int NULL,
        trip_count int NULL,
        avg_walk_minutes float NULL,
        avg_walk_euclidean_mi float NULL,
        avg_walk_manhattan_mi float NULL,
        mean_start_lon float NULL,
        mean_start_lat float NULL,
        mean_end_lon float NULL,
        mean_end_lat float NULL,
        CONSTRAINT PK_ArcGIS_Walk_Egress_Rolling31 PRIMARY KEY CLUSTERED (oid)
    );
END
GO
