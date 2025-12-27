/* =====================================================================
   FILE: sql/01_arcgis_materialized_tables.sql
   PURPOSE:
     - ArcGIS-friendly "materialized" tables for publishing workflows
     - Avoids query-layer/driver edge cases (GetCount, XY tools, etc.)
     - Uses a stable integer OID for feature layer publishing
   ===================================================================== */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mobility')
    EXEC('CREATE SCHEMA mobility');
GO

/* 1) One-time cleanup: remove deprecated service_name (if present) */
IF OBJECT_ID('mobility.Walk_AccessEgress_Daily','U') IS NOT NULL
BEGIN
    IF COL_LENGTH('mobility.Walk_AccessEgress_Daily', 'service_name') IS NOT NULL
    BEGIN
        PRINT 'Dropping deprecated column mobility.Walk_AccessEgress_Daily.service_name ...';
        ALTER TABLE mobility.Walk_AccessEgress_Daily DROP COLUMN service_name;
    END
END
GO

/* 2) Ensure hotspot_hash exists on route-level hotspot table */
IF OBJECT_ID('mobility.Transfer_Hotspots_Daily_Route','U') IS NOT NULL
BEGIN
    IF COL_LENGTH('mobility.Transfer_Hotspots_Daily_Route', 'hotspot_hash') IS NULL
    BEGIN
        PRINT 'Adding hotspot_hash to mobility.Transfer_Hotspots_Daily_Route ...';
        ALTER TABLE mobility.Transfer_Hotspots_Daily_Route
        ADD hotspot_hash varbinary(32) NULL;
    END

    PRINT 'Backfilling hotspot_hash on mobility.Transfer_Hotspots_Daily_Route ...';
    UPDATE mobility.Transfer_Hotspots_Daily_Route
    SET hotspot_hash = HASHBYTES('SHA2_256', CONVERT(varbinary(max), hotspot_key))
    WHERE hotspot_hash IS NULL;

    -- Best effort NOT NULL enforcement (safe to fail in public template)
    BEGIN TRY
        ALTER TABLE mobility.Transfer_Hotspots_Daily_Route
        ALTER COLUMN hotspot_hash varbinary(32) NOT NULL;
    END TRY
    BEGIN CATCH
        PRINT 'NOTE: Could not enforce NOT NULL on Transfer_Hotspots_Daily_Route.hotspot_hash';
    END CATCH
END
GO

/* 3) Ensure hotspot_hash exists on route-pair hotspot table */
IF OBJECT_ID('mobility.Transfer_Hotspots_Daily_RoutePair','U') IS NOT NULL
BEGIN
    IF COL_LENGTH('mobility.Transfer_Hotspots_Daily_RoutePair', 'hotspot_hash') IS NULL
    BEGIN
        PRINT 'Adding hotspot_hash to mobility.Transfer_Hotspots_Daily_RoutePair ...';
        ALTER TABLE mobility.Transfer_Hotspots_Daily_RoutePair
        ADD hotspot_hash varbinary(32) NULL;
    END

    PRINT 'Backfilling hotspot_hash on mobility.Transfer_Hotspots_Daily_RoutePair ...';
    UPDATE mobility.Transfer_Hotspots_Daily_RoutePair
    SET hotspot_hash = HASHBYTES('SHA2_256', CONVERT(varbinary(max), hotspot_key))
    WHERE hotspot_hash IS NULL;

    BEGIN TRY
        ALTER TABLE mobility.Transfer_Hotspots_Daily_RoutePair
        ALTER COLUMN hotspot_hash varbinary(32) NOT NULL;
    END TRY
    BEGIN CATCH
        PRINT 'NOTE: Could not enforce NOT NULL on Transfer_Hotspots_Daily_RoutePair.hotspot_hash';
    END CATCH
END
GO

/* 4) Create ArcGIS materialized tables */

-- 4a) Flows (Route) ArcGIS table
IF OBJECT_ID('mobility.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31','U') IS NULL
BEGIN
    PRINT 'Creating mobility.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31 ...';
    CREATE TABLE mobility.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31 (
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

-- 4b) Transfer Hotspots (Route) ArcGIS table
IF OBJECT_ID('mobility.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31','U') IS NULL
BEGIN
    PRINT 'Creating mobility.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31 ...';
    CREATE TABLE mobility.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31 (
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

-- 4c) Walk Egress ArcGIS table
IF OBJECT_ID('mobility.ArcGIS_Walk_Egress_Daily_Rolling31','U') IS NULL
BEGIN
    PRINT 'Creating mobility.ArcGIS_Walk_Egress_Daily_Rolling31 ...';
    CREATE TABLE mobility.ArcGIS_Walk_Egress_Daily_Rolling31 (
        oid INT NOT NULL,
        trip_date date NOT NULL,
        walk_type nvarchar(10) NOT NULL,          -- ACCESS/EGRESS; dashboard often uses EGRESS
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

