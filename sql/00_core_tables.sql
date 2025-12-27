/* =====================================================================
   FILE: sql/00_core_tables.sql
   PURPOSE:
     - Core aggregate tables refreshed by mobility.usp_Refresh_Rolling31_Aggregates
     - Public-safe, generalized schema definitions
     - Designed for downstream ArcGIS materialization/publishing

   NOTES:
     - Origin_BG/Dest_BG assume 12-char GEOID (Census Block Group GEOID)
     - hotspot_hash is SHA2_256 => VARBINARY(32)
   ===================================================================== */

-- OPTIONAL: create schema if needed
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mobility')
    EXEC('CREATE SCHEMA mobility');
GO

/* ---------------------------------------------------------------
   1) Flow_BG_BG_Daily
   Daily BG->BG travel pattern summaries (non-route)
   --------------------------------------------------------------- */
IF OBJECT_ID('mobility.Flow_BG_BG_Daily','U') IS NULL
BEGIN
    CREATE TABLE mobility.Flow_BG_BG_Daily (
        trip_date              date          NOT NULL,
        Origin_BG              varchar(12)    NOT NULL,
        Dest_BG                varchar(12)    NOT NULL,

        service_group          nvarchar(50)   NULL,   -- FIXED_ROUTE, DEMAND_RESPONSE, etc.
        provider_name          nvarchar(200)  NULL,   -- normalized provider label

        leg_count              int           NULL,
        trip_count             int           NULL,
        transfer_trip_count    int           NULL,

        avg_manhattan_mi       float         NULL,
        avg_euclidean_mi       float         NULL,
        avg_leg_minutes        float         NULL,

        mean_start_lon         float         NULL,
        mean_start_lat         float         NULL,
        mean_end_lon           float         NULL,
        mean_end_lat           float         NULL
    );

    CREATE INDEX IX_Flow_BG_BG_Daily_Date
        ON mobility.Flow_BG_BG_Daily (trip_date);
END
GO

/* ---------------------------------------------------------------
   2) Flow_BG_BG_Daily_Route
   Daily BG->BG travel pattern summaries paired to route labels
   --------------------------------------------------------------- */
IF OBJECT_ID('mobility.Flow_BG_BG_Daily_Route','U') IS NULL
BEGIN
    CREATE TABLE mobility.Flow_BG_BG_Daily_Route (
        trip_date              date          NOT NULL,
        route_short_name       nvarchar(50)   NULL,

        Origin_BG              varchar(12)    NOT NULL,
        Dest_BG                varchar(12)    NOT NULL,

        service_group          nvarchar(50)   NULL,
        provider_name          nvarchar(200)  NULL,

        leg_count              int           NULL,
        trip_count             int           NULL,
        transfer_trip_count    int           NULL,

        avg_manhattan_mi       float         NULL,
        avg_euclidean_mi       float         NULL,
        avg_leg_minutes        float         NULL,

        mean_start_lon         float         NULL,
        mean_start_lat         float         NULL,
        mean_end_lon           float         NULL,
        mean_end_lat           float         NULL
    );

    CREATE INDEX IX_Flow_BG_BG_Daily_Route_DateRoute
        ON mobility.Flow_BG_BG_Daily_Route (trip_date, route_short_name);
END
GO

/* ---------------------------------------------------------------
   3) Transfer_Hotspots_Daily
   Daily transfer hotspot summaries (network-wide)
   --------------------------------------------------------------- */
IF OBJECT_ID('mobility.Transfer_Hotspots_Daily','U') IS NULL
BEGIN
    CREATE TABLE mobility.Transfer_Hotspots_Daily (
        trip_date          date           NOT NULL,
        hotspot_key        nvarchar(500)  NOT NULL,  -- stop name or coordinate-derived key
        provider_name      nvarchar(200)  NULL,
        start_stop_name    nvarchar(200)  NULL,

        transfer_events    int            NULL,
        transfer_trips     int            NULL,

        mean_lon           float          NULL,
        mean_lat           float          NULL,
        avg_leg_minutes    float          NULL
    );

    CREATE INDEX IX_Transfer_Hotspots_Daily_Date
        ON mobility.Transfer_Hotspots_Daily (trip_date);
END
GO

/* ---------------------------------------------------------------
   4) Transfer_Hotspots_Daily_Route
   Daily transfer hotspots paired with route + from/to context
   --------------------------------------------------------------- */
IF OBJECT_ID('mobility.Transfer_Hotspots_Daily_Route','U') IS NULL
BEGIN
    CREATE TABLE mobility.Transfer_Hotspots_Daily_Route (
        trip_date             date           NOT NULL,
        route_short_name      nvarchar(50)   NULL,
        transfer_type         nvarchar(50)   NULL,

        hotspot_key           nvarchar(500)  NOT NULL,
        hotspot_hash          varbinary(32)  NULL,

        provider_name         nvarchar(200)  NULL,
        start_stop_name       nvarchar(200)  NULL,

        from_route_short_name nvarchar(50)   NULL,
        to_route_short_name   nvarchar(50)   NULL,

        transfer_events       int            NULL,
        transfer_trips        int            NULL,

        mean_lon              float          NULL,
        mean_lat              float          NULL,
        avg_leg_minutes       float          NULL
    );

    CREATE INDEX IX_Transfer_Hotspots_Daily_Route_Date
        ON mobility.Transfer_Hotspots_Daily_Route (trip_date);

    CREATE INDEX IX_Transfer_Hotspots_Daily_Route_Hash
        ON mobility.Transfer_Hotspots_Daily_Route (hotspot_hash);
END
GO

/* ---------------------------------------------------------------
   5) Transfer_Hotspots_Daily_RoutePair
   Daily transfer hotspots with explicit from/to route pairing
   --------------------------------------------------------------- */
IF OBJECT_ID('mobility.Transfer_Hotspots_Daily_RoutePair','U') IS NULL
BEGIN
    CREATE TABLE mobility.Transfer_Hotspots_Daily_RoutePair (
        trip_date             date           NOT NULL,
        transfer_type         nvarchar(50)   NULL,

        hotspot_key           nvarchar(500)  NOT NULL,
        hotspot_hash          varbinary(32)  NULL,

        provider_name         nvarchar(200)  NULL,
        start_stop_name       nvarchar(200)  NULL,

        from_route_short_name nvarchar(50)   NULL,
        to_route_short_name   nvarchar(50)   NULL,

        transfer_events       int            NULL,
        transfer_trips        int            NULL,

        mean_lon              float          NULL,
        mean_lat              float          NULL,
        avg_leg_minutes       float          NULL
    );

    CREATE INDEX IX_Transfer_Hotspots_Daily_RoutePair_Date
        ON mobility.Transfer_Hotspots_Daily_RoutePair (trip_date);

    CREATE INDEX IX_Transfer_Hotspots_Daily_RoutePair_Hash
        ON mobility.Transfer_Hotspots_Daily_RoutePair (hotspot_hash);
END
GO

/* ---------------------------------------------------------------
   6) BG_Summary_Daily
   Daily origin-side summary of trip/transfer activity by block group
   --------------------------------------------------------------- */
IF OBJECT_ID('mobility.BG_Summary_Daily','U') IS NULL
BEGIN
    CREATE TABLE mobility.BG_Summary_Daily (
        trip_date               date         NOT NULL,
        Origin_BG               varchar(12)   NOT NULL,

        legs                    int          NULL,
        transfer_legs           int          NULL,
        trips                   int          NULL,
        trips_with_transfer     int          NULL,
        pct_trips_with_transfer float        NULL,   -- 0..1 share
        avg_leg_minutes         float        NULL
    );

    CREATE INDEX IX_BG_Summary_Daily_Date
        ON mobility.BG_Summary_Daily (trip_date);
END
GO

/* ---------------------------------------------------------------
   7) Walk_AccessEgress_Daily
   Daily walk legs categorized as ACCESS vs EGRESS
   --------------------------------------------------------------- */
IF OBJECT_ID('mobility.Walk_AccessEgress_Daily','U') IS NULL
BEGIN
    CREATE TABLE mobility.Walk_AccessEgress_Daily (
        trip_date              date          NOT NULL,
        walk_type              nvarchar(10)  NOT NULL,   -- ACCESS / EGRESS
        related_service        nvarchar(50)  NULL,
        related_route          nvarchar(50)  NULL,

        stop_key               nvarchar(500) NULL,
        stop_name              nvarchar(200) NULL,
        provider_name          nvarchar(200) NULL,

        walk_leg_count         int           NULL,
        trip_count             int           NULL,

        avg_walk_minutes       float         NULL,
        avg_walk_euclidean_mi  float         NULL,
        avg_walk_manhattan_mi  float         NULL,

        mean_start_lon         float         NULL,
        mean_start_lat         float         NULL,
        mean_end_lon           float         NULL,
        mean_end_lat           float         NULL
    );

    CREATE INDEX IX_Walk_AccessEgress_Daily_DateType
        ON mobility.Walk_AccessEgress_Daily (trip_date, walk_type);
END
GO

/* ---------------------------------------------------------------
   OPTIONAL: mobility.LegTrips_Clean (skeleton)
   --------------------------------------------------------------- */
-- IF OBJECT_ID('mobility.LegTrips_Clean','U') IS NULL
-- BEGIN
--     CREATE TABLE mobility.LegTrips_Clean (
--         user_trip_id             nvarchar(100) NOT NULL,
--         trip_date                date          NOT NULL,
--         start_time_utc           datetime2(0)  NULL,
--         end_time_utc             datetime2(0)  NULL,
--
--         start_longitude          float         NULL,
--         start_latitude           float         NULL,
--         end_longitude            float         NULL,
--         end_latitude             float         NULL,
--
--         service_name             nvarchar(200) NULL,
--         route_short_name         nvarchar(50)  NULL,
--         mode                     nvarchar(50)  NULL,
--
--         start_stop_name          nvarchar(200) NULL,
--         end_stop_name            nvarchar(200) NULL,
--
--         Origin_BG                varchar(12)   NULL,
--         Dest_BG                  varchar(12)   NULL,
--
--         manhattan_distance_mi    float         NULL,
--         euclidean_distance_mi    float         NULL
--     );
--
--     CREATE INDEX IX_LegTrips_Clean_Date
--         ON mobility.LegTrips_Clean (trip_date);
-- END
-- GO
