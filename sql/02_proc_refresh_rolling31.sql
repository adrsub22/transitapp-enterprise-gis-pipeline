/* =====================================================================
   FILE: sql/02_proc_refresh_rolling31.sql
   PURPOSE:
     - Refresh rolling 31-day aggregates used for GIS publishing.
     - Detect transfers using window functions (LAG/LEAD).
     - Build:
         (a) flow tables (BG->BG)
         (b) transfer hotspot tables
         (c) walk access/egress summaries
     - Materialize ArcGIS-ready tables with stable OID fields.

   NOTES:
     - Provider/service names generalized.
     - Region filter parameterized (instead of hard-coded county prefix).
   ===================================================================== */

CREATE OR ALTER PROCEDURE transitapp.usp_Refresh_Rolling31_Aggregates
    @days_back int = 31,
    @region_prefix varchar(5) = NULL  -- e.g., county/state prefix; NULL disables filter
AS
BEGIN
    SET NOCOUNT ON;
    SET ANSI_WARNINGS OFF; -- suppress "Null value eliminated by aggregate" for AVG

    DECLARE @start_date date = DATEADD(DAY, -@days_back, CAST(GETDATE() AS date));
    DECLARE @end_date_excl date = CAST(GETDATE() AS date);

    /* ============================================================
       Stage: Build a temp table with transfer flags per leg
       - base: filtered leg-level records
       - ordered: identify previous/next legs per trip using window fxns
       - flags: classify transfers + build standardized route labels
       ============================================================ */

    IF OBJECT_ID('tempdb..#flags') IS NOT NULL DROP TABLE #flags;

    ;WITH base AS (
        SELECT
            l.user_trip_id,
            l.trip_date,
            l.start_time_utc,
            l.end_time_utc,
            l.start_longitude, l.start_latitude,
            l.end_longitude,   l.end_latitude,
            l.service_name,
            l.route_short_name,
            l.mode,
            l.start_stop_name,
            l.end_stop_name,
            l.Origin_BG,
            l.Dest_BG,
            l.manhattan_distance_mi,
            l.euclidean_distance_mi,

            /* Group services/providers into categories suitable for reporting */
            CASE
                WHEN l.service_name = 'AGENCY_FIXED_ROUTE' THEN 'FIXED_ROUTE'
                WHEN l.service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
                WHEN LOWER(l.mode) = 'walk' THEN 'WALK'
                WHEN LOWER(l.mode) IN ('bike','bikeshare') THEN 'BIKE'
                WHEN LOWER(l.mode) = 'scooter' THEN 'SCOOTER'
                WHEN LOWER(l.mode) = 'rideshare' THEN 'RIDESHARE'
                WHEN LOWER(l.mode) = 'transit' THEN 'OTHER_TRANSIT'
                ELSE 'OTHER'
            END AS service_group,

            /* Provider normalization (keep conservative for public-safe sample) */
            CASE
                WHEN l.service_name IN ('AGENCY_FIXED_ROUTE','AGENCY_DEMAND_RESPONSE','BIKESHARE','SCOOTER_VENDOR','RIDESHARE_VENDOR')
                    THEN l.service_name
                ELSE '(UNKNOWN)'
            END AS provider_name,

            /* Duration in minutes for each leg (null-safe) */
            CASE
                WHEN l.start_time_utc IS NOT NULL
                 AND l.end_time_utc   IS NOT NULL
                 AND l.end_time_utc >= l.start_time_utc
                THEN DATEDIFF(SECOND, l.start_time_utc, l.end_time_utc) / 60.0
                ELSE NULL
            END AS leg_minutes

        FROM transitapp.LegTrips_Clean l
        WHERE l.trip_date >= @start_date
          AND l.trip_date <  @end_date_excl
          AND l.Origin_BG IS NOT NULL
          AND l.Dest_BG   IS NOT NULL
          AND LEN(l.Origin_BG) = 12
          AND LEN(l.Dest_BG)   = 12
          AND (@region_prefix IS NULL OR (l.Origin_BG LIKE @region_prefix + '%' AND l.Dest_BG LIKE @region_prefix + '%'))
    ),
    ordered AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.user_trip_id, b.trip_date
                ORDER BY b.start_time_utc, b.end_time_utc
            ) AS leg_seq,

            LAG(b.service_name)     OVER (PARTITION BY b.user_trip_id, b.trip_date ORDER BY b.start_time_utc, b.end_time_utc) AS prev_service_name,
            LAG(b.route_short_name) OVER (PARTITION BY b.user_trip_id, b.trip_date ORDER BY b.start_time_utc, b.end_time_utc) AS prev_route_short_name,
            LAG(b.end_stop_name)    OVER (PARTITION BY b.user_trip_id, b.trip_date ORDER BY b.start_time_utc, b.end_time_utc) AS prev_end_stop_name,

            LEAD(b.service_name)     OVER (PARTITION BY b.user_trip_id, b.trip_date ORDER BY b.start_time_utc, b.end_time_utc) AS next_service_name,
            LEAD(b.route_short_name) OVER (PARTITION BY b.user_trip_id, b.trip_date ORDER BY b.start_time_utc, b.end_time_utc) AS next_route_short_name,
            LEAD(b.start_stop_name)  OVER (PARTITION BY b.user_trip_id, b.trip_date ORDER BY b.start_time_utc, b.end_time_utc) AS next_start_stop_name
        FROM base b
    )
    SELECT
        o.*,

        /* Standardize "to route" label for reporting */
        CASE
            WHEN o.service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            WHEN o.service_name = 'AGENCY_FIXED_ROUTE' THEN COALESCE(NULLIF(o.route_short_name,''), '(UNKNOWN_ROUTE)')
            WHEN LOWER(o.mode) = 'walk' THEN 'WALK'
            ELSE COALESCE(NULLIF(o.route_short_name,''), '(NA)')
        END AS to_route_short_name_label,

        /* Standardize "from route" label for reporting */
        CASE
            WHEN o.prev_service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            WHEN o.prev_service_name = 'AGENCY_FIXED_ROUTE' THEN COALESCE(NULLIF(o.prev_route_short_name,''), '(UNKNOWN_ROUTE)')
            WHEN o.prev_service_name IS NULL THEN NULL
            ELSE COALESCE(NULLIF(o.prev_route_short_name,''), '(NA)')
        END AS from_route_short_name_label,

        /* Transfer detection logic */
        CASE
            WHEN o.prev_service_name IS NULL THEN 0
            WHEN o.prev_service_name = 'AGENCY_FIXED_ROUTE' AND o.service_name = 'AGENCY_DEMAND_RESPONSE' THEN 1
            WHEN o.prev_service_name = 'AGENCY_DEMAND_RESPONSE' AND o.service_name = 'AGENCY_FIXED_ROUTE' THEN 1
            WHEN o.prev_service_name = 'AGENCY_FIXED_ROUTE' AND o.service_name = 'AGENCY_FIXED_ROUTE'
                 AND ISNULL(o.prev_route_short_name,'') <> ISNULL(o.route_short_name,'') THEN 1
            ELSE 0
        END AS is_transfer_leg,

        /* Transfer type classification */
        CASE
            WHEN o.prev_service_name IS NULL THEN NULL
            WHEN o.prev_service_name = 'AGENCY_FIXED_ROUTE' AND o.service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'FIXED_TO_DEMAND'
            WHEN o.prev_service_name = 'AGENCY_DEMAND_RESPONSE' AND o.service_name = 'AGENCY_FIXED_ROUTE' THEN 'DEMAND_TO_FIXED'
            WHEN o.prev_service_name = 'AGENCY_FIXED_ROUTE' AND o.service_name = 'AGENCY_FIXED_ROUTE'
                 AND ISNULL(o.prev_route_short_name,'') <> ISNULL(o.route_short_name,'') THEN 'ROUTE_CHANGE'
            ELSE 'OTHER'
        END AS transfer_type

    INTO #flags
    FROM ordered o;

    /* ============================================================
       Destination tables: clear then rebuild
       - TRUNCATE is fastest, but may fail w/ FK or permissions.
       - Fallback: DELETE.
       ============================================================ */
    BEGIN TRY
        TRUNCATE TABLE transitapp.Flow_BG_BG_Daily;
        TRUNCATE TABLE transitapp.Transfer_Hotspots_Daily;
        TRUNCATE TABLE transitapp.BG_Summary_Daily;
        TRUNCATE TABLE transitapp.Flow_BG_BG_Daily_Route;
        TRUNCATE TABLE transitapp.Transfer_Hotspots_Daily_Route;
        TRUNCATE TABLE transitapp.Transfer_Hotspots_Daily_RoutePair;
        TRUNCATE TABLE transitapp.Walk_AccessEgress_Daily;
    END TRY
    BEGIN CATCH
        DELETE FROM transitapp.Flow_BG_BG_Daily;
        DELETE FROM transitapp.Transfer_Hotspots_Daily;
        DELETE FROM transitapp.BG_Summary_Daily;
        DELETE FROM transitapp.Flow_BG_BG_Daily_Route;
        DELETE FROM transitapp.Transfer_Hotspots_Daily_Route;
        DELETE FROM transitapp.Transfer_Hotspots_Daily_RoutePair;
        DELETE FROM transitapp.Walk_AccessEgress_Daily;
    END CATCH;

    /* ============================================================
       1) Non-route flows (BG->BG)
       ============================================================ */
    INSERT INTO transitapp.Flow_BG_BG_Daily (
        trip_date, Origin_BG, Dest_BG, service_group, provider_name,
        leg_count, trip_count, transfer_trip_count,
        avg_manhattan_mi, avg_euclidean_mi,
        mean_start_lon, mean_start_lat, mean_end_lon, mean_end_lat,
        avg_leg_minutes
    )
    SELECT
        trip_date,
        Origin_BG,
        Dest_BG,
        service_group,
        provider_name,
        COUNT(*) AS leg_count,
        COUNT(DISTINCT user_trip_id) AS trip_count,
        COUNT(DISTINCT CASE WHEN is_transfer_leg=1 THEN user_trip_id END) AS transfer_trip_count,
        AVG(CAST(manhattan_distance_mi AS float)) AS avg_manhattan_mi,
        AVG(CAST(euclidean_distance_mi AS float)) AS avg_euclidean_mi,
        AVG(CAST(start_longitude AS float)) AS mean_start_lon,
        AVG(CAST(start_latitude  AS float)) AS mean_start_lat,
        AVG(CAST(end_longitude   AS float)) AS mean_end_lon,
        AVG(CAST(end_latitude    AS float)) AS mean_end_lat,
        AVG(CAST(leg_minutes AS float)) AS avg_leg_minutes
    FROM #flags
    GROUP BY trip_date, Origin_BG, Dest_BG, service_group, provider_name;

    /* ============================================================
       2) Non-route transfer hotspots
       - hotspot_key uses stop_name when available, else rounded coords.
       ============================================================ */
    INSERT INTO transitapp.Transfer_Hotspots_Daily (
        trip_date, hotspot_key, provider_name, start_stop_name,
        transfer_events, transfer_trips, mean_lon, mean_lat,
        avg_leg_minutes
    )
    SELECT
        f.trip_date,
        hk.hotspot_key,
        f.provider_name,
        f.start_stop_name,
        COUNT(*) AS transfer_events,
        COUNT(DISTINCT f.user_trip_id) AS transfer_trips,
        AVG(CAST(f.start_longitude AS float)) AS mean_lon,
        AVG(CAST(f.start_latitude  AS float)) AS mean_lat,
        AVG(CAST(f.leg_minutes AS float)) AS avg_leg_minutes
    FROM #flags f
    CROSS APPLY (
        SELECT COALESCE(NULLIF(f.start_stop_name,''), CONCAT('COORD_', ROUND(CAST(f.start_longitude AS float),4), '_', ROUND(CAST(f.start_latitude AS float),4))) AS hotspot_key
    ) hk
    WHERE f.is_transfer_leg = 1
    GROUP BY f.trip_date, hk.hotspot_key, f.provider_name, f.start_stop_name;

    /* ============================================================
       3) BG summary (origin-side rollups)
       ============================================================ */
    INSERT INTO transitapp.BG_Summary_Daily (
        trip_date, Origin_BG,
        legs, transfer_legs, trips, trips_with_transfer, pct_trips_with_transfer,
        avg_leg_minutes
    )
    SELECT
        trip_date,
        Origin_BG,
        COUNT(*) AS legs,
        SUM(CASE WHEN is_transfer_leg=1 THEN 1 ELSE 0 END) AS transfer_legs,
        COUNT(DISTINCT user_trip_id) AS trips,
        COUNT(DISTINCT CASE WHEN is_transfer_leg=1 THEN user_trip_id END) AS trips_with_transfer,
        CASE WHEN COUNT(DISTINCT user_trip_id)=0 THEN 0
             ELSE 1.0 * COUNT(DISTINCT CASE WHEN is_transfer_leg=1 THEN user_trip_id END) / COUNT(DISTINCT user_trip_id)
        END AS pct_trips_with_transfer,
        AVG(CAST(leg_minutes AS float)) AS avg_leg_minutes
    FROM #flags
    GROUP BY trip_date, Origin_BG;

    /* ============================================================
       4) Route-enabled flows (BG->BG + route label)
       ============================================================ */
    INSERT INTO transitapp.Flow_BG_BG_Daily_Route (
        trip_date, route_short_name, Origin_BG, Dest_BG, service_group, provider_name,
        leg_count, trip_count, transfer_trip_count,
        avg_manhattan_mi, avg_euclidean_mi,
        mean_start_lon, mean_start_lat, mean_end_lon, mean_end_lat,
        avg_leg_minutes
    )
    SELECT
        trip_date,
        to_route_short_name_label AS route_short_name,
        Origin_BG,
        Dest_BG,
        service_group,
        provider_name,
        COUNT(*) AS leg_count,
        COUNT(DISTINCT user_trip_id) AS trip_count,
        COUNT(DISTINCT CASE WHEN is_transfer_leg=1 THEN user_trip_id END) AS transfer_trip_count,
        AVG(CAST(manhattan_distance_mi AS float)) AS avg_manhattan_mi,
        AVG(CAST(euclidean_distance_mi AS float)) AS avg_euclidean_mi,
        AVG(CAST(start_longitude AS float)) AS mean_start_lon,
        AVG(CAST(start_latitude  AS float)) AS mean_start_lat,
        AVG(CAST(end_longitude   AS float)) AS mean_end_lon,
        AVG(CAST(end_latitude    AS float)) AS mean_end_lat,
        AVG(CAST(leg_minutes AS float)) AS avg_leg_minutes
    FROM #flags
    GROUP BY trip_date, to_route_short_name_label, Origin_BG, Dest_BG, service_group, provider_name;

    /* ============================================================
       5) Route-enabled transfer hotspots
       - hotspot_hash is used for stable joins / uniqueness.
       ============================================================ */
    INSERT INTO transitapp.Transfer_Hotspots_Daily_Route (
        trip_date, route_short_name, transfer_type,
        hotspot_key, hotspot_hash,
        provider_name, start_stop_name,
        from_route_short_name, to_route_short_name,
        transfer_events, transfer_trips, mean_lon, mean_lat,
        avg_leg_minutes
    )
    SELECT
        f.trip_date,
        COALESCE(f.to_route_short_name_label,'(NA)') AS route_short_name,
        COALESCE(f.transfer_type,'OTHER') AS transfer_type,
        hk.hotspot_key,
        hk.hotspot_hash,
        f.provider_name,
        f.start_stop_name,
        COALESCE(f.from_route_short_name_label,'(NA)') AS from_route_short_name,
        COALESCE(f.to_route_short_name_label,'(NA)')   AS to_route_short_name,
        COUNT(*) AS transfer_events,
        COUNT(DISTINCT f.user_trip_id) AS transfer_trips,
        AVG(CAST(f.start_longitude AS float)) AS mean_lon,
        AVG(CAST(f.start_latitude  AS float)) AS mean_lat,
        AVG(CAST(f.leg_minutes AS float)) AS avg_leg_minutes
    FROM #flags f
    CROSS APPLY (
        SELECT
            COALESCE(NULLIF(f.start_stop_name,''), CONCAT('COORD_', ROUND(CAST(f.start_longitude AS float),4), '_', ROUND(CAST(f.start_latitude AS float),4))) AS hotspot_key,
            HASHBYTES('SHA2_256', CONVERT(varbinary(max),
                COALESCE(NULLIF(f.start_stop_name,''), CONCAT('COORD_', ROUND(CAST(f.start_longitude AS float),4), '_', ROUND(CAST(f.start_latitude AS float),4)))
            )) AS hotspot_hash
    ) hk
    WHERE f.is_transfer_leg = 1
    GROUP BY
        f.trip_date,
        COALESCE(f.to_route_short_name_label,'(NA)'),
        COALESCE(f.transfer_type,'OTHER'),
        hk.hotspot_key,
        hk.hotspot_hash,
        f.provider_name,
        f.start_stop_name,
        COALESCE(f.from_route_short_name_label,'(NA)'),
        COALESCE(f.to_route_short_name_label,'(NA)');

    /* ============================================================
       6) Route-pair transfer hotspots (from/to routes)
       ============================================================ */
    INSERT INTO transitapp.Transfer_Hotspots_Daily_RoutePair (
        trip_date, transfer_type,
        hotspot_key, hotspot_hash,
        provider_name, start_stop_name,
        from_route_short_name, to_route_short_name,
        transfer_events, transfer_trips, mean_lon, mean_lat,
        avg_leg_minutes
    )
    SELECT
        f.trip_date,
        COALESCE(f.transfer_type,'OTHER') AS transfer_type,
        hk.hotspot_key,
        hk.hotspot_hash,
        f.provider_name,
        f.start_stop_name,
        COALESCE(f.from_route_short_name_label,'(NA)') AS from_route_short_name,
        COALESCE(f.to_route_short_name_label,'(NA)')   AS to_route_short_name,
        COUNT(*) AS transfer_events,
        COUNT(DISTINCT f.user_trip_id) AS transfer_trips,
        AVG(CAST(f.start_longitude AS float)) AS mean_lon,
        AVG(CAST(f.start_latitude  AS float)) AS mean_lat,
        AVG(CAST(f.leg_minutes AS float)) AS avg_leg_minutes
    FROM #flags f
    CROSS APPLY (
        SELECT
            COALESCE(NULLIF(f.start_stop_name,''), CONCAT('COORD_', ROUND(CAST(f.start_longitude AS float),4), '_', ROUND(CAST(f.start_latitude AS float),4))) AS hotspot_key,
            HASHBYTES('SHA2_256', CONVERT(varbinary(max),
                COALESCE(NULLIF(f.start_stop_name,''), CONCAT('COORD_', ROUND(CAST(f.start_longitude AS float),4), '_', ROUND(CAST(f.start_latitude AS float),4)))
            )) AS hotspot_hash
    ) hk
    WHERE f.is_transfer_leg = 1
    GROUP BY
        f.trip_date,
        COALESCE(f.transfer_type,'OTHER'),
        hk.hotspot_key,
        hk.hotspot_hash,
        f.provider_name,
        f.start_stop_name,
        COALESCE(f.from_route_short_name_label,'(NA)'),
        COALESCE(f.to_route_short_name_label,'(NA)');

    /* ============================================================
       7) Walk access/egress
       - No service_name column (by design).
       - ACCESS = walk leading into a transit leg
       - EGRESS = walk after a transit leg
       ============================================================ */
    INSERT INTO transitapp.Walk_AccessEgress_Daily (
        trip_date, walk_type, related_service, related_route,
        stop_key, stop_name, provider_name,
        walk_leg_count, trip_count,
        avg_walk_minutes, avg_walk_euclidean_mi, avg_walk_manhattan_mi,
        mean_start_lon, mean_start_lat, mean_end_lon, mean_end_lat
    )
    /* ACCESS */
    SELECT
        f.trip_date,
        'ACCESS' AS walk_type,
        CASE
            WHEN f.next_service_name = 'AGENCY_FIXED_ROUTE' THEN 'FIXED_ROUTE'
            WHEN f.next_service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            ELSE 'OTHER'
        END AS related_service,
        CASE
            WHEN f.next_service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            WHEN f.next_service_name = 'AGENCY_FIXED_ROUTE' THEN COALESCE(NULLIF(f.next_route_short_name,''), '(UNKNOWN_ROUTE)')
            ELSE '(NA)'
        END AS related_route,
        COALESCE(NULLIF(f.next_start_stop_name,''), '(NO_STOP)') AS stop_key,
        f.next_start_stop_name AS stop_name,
        '(UNKNOWN)' AS provider_name,
        COUNT(*) AS walk_leg_count,
        COUNT(DISTINCT f.user_trip_id) AS trip_count,
        AVG(CAST(f.leg_minutes AS float)) AS avg_walk_minutes,
        AVG(CAST(f.euclidean_distance_mi AS float)) AS avg_walk_euclidean_mi,
        AVG(CAST(f.manhattan_distance_mi AS float)) AS avg_walk_manhattan_mi,
        AVG(CAST(f.start_longitude AS float)) AS mean_start_lon,
        AVG(CAST(f.start_latitude  AS float)) AS mean_start_lat,
        AVG(CAST(f.end_longitude   AS float)) AS mean_end_lon,
        AVG(CAST(f.end_latitude    AS float)) AS mean_end_lat
    FROM #flags f
    WHERE LOWER(f.mode) = 'walk'
      AND f.next_service_name IN ('AGENCY_FIXED_ROUTE','AGENCY_DEMAND_RESPONSE')
    GROUP BY
        f.trip_date,
        CASE
            WHEN f.next_service_name = 'AGENCY_FIXED_ROUTE' THEN 'FIXED_ROUTE'
            WHEN f.next_service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            ELSE 'OTHER'
        END,
        CASE
            WHEN f.next_service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            WHEN f.next_service_name = 'AGENCY_FIXED_ROUTE' THEN COALESCE(NULLIF(f.next_route_short_name,''), '(UNKNOWN_ROUTE)')
            ELSE '(NA)'
        END,
        COALESCE(NULLIF(f.next_start_stop_name,''), '(NO_STOP)'),
        f.next_start_stop_name

    UNION ALL

    /* EGRESS */
    SELECT
        f.trip_date,
        'EGRESS' AS walk_type,
        CASE
            WHEN f.prev_service_name = 'AGENCY_FIXED_ROUTE' THEN 'FIXED_ROUTE'
            WHEN f.prev_service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            ELSE 'OTHER'
        END AS related_service,
        CASE
            WHEN f.prev_service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            WHEN f.prev_service_name = 'AGENCY_FIXED_ROUTE' THEN COALESCE(NULLIF(f.prev_route_short_name,''), '(UNKNOWN_ROUTE)')
            ELSE '(NA)'
        END AS related_route,
        COALESCE(NULLIF(f.prev_end_stop_name,''), '(NO_STOP)') AS stop_key,
        f.prev_end_stop_name AS stop_name,
        '(UNKNOWN)' AS provider_name,
        COUNT(*) AS walk_leg_count,
        COUNT(DISTINCT f.user_trip_id) AS trip_count,
        AVG(CAST(f.leg_minutes AS float)) AS avg_walk_minutes,
        AVG(CAST(f.euclidean_distance_mi AS float)) AS avg_walk_euclidean_mi,
        AVG(CAST(f.manhattan_distance_mi AS float)) AS avg_walk_manhattan_mi,
        AVG(CAST(f.start_longitude AS float)) AS mean_start_lon,
        AVG(CAST(f.start_latitude  AS float)) AS mean_start_lat,
        AVG(CAST(f.end_longitude   AS float)) AS mean_end_lon,
        AVG(CAST(f.end_latitude    AS float)) AS mean_end_lat
    FROM #flags f
    WHERE LOWER(f.mode) = 'walk'
      AND f.prev_service_name IN ('AGENCY_FIXED_ROUTE','AGENCY_DEMAND_RESPONSE')
    GROUP BY
        f.trip_date,
        CASE
            WHEN f.prev_service_name = 'AGENCY_FIXED_ROUTE' THEN 'FIXED_ROUTE'
            WHEN f.prev_service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            ELSE 'OTHER'
        END,
        CASE
            WHEN f.prev_service_name = 'AGENCY_DEMAND_RESPONSE' THEN 'DEMAND_RESPONSE'
            WHEN f.prev_service_name = 'AGENCY_FIXED_ROUTE' THEN COALESCE(NULLIF(f.prev_route_short_name,''), '(UNKNOWN_ROUTE)')
            ELSE '(NA)'
        END,
        COALESCE(NULLIF(f.prev_end_stop_name,''), '(NO_STOP)'),
        f.prev_end_stop_name;

    /* ============================================================
       ArcGIS Materialized Tables (Rolling Window)
       - Provide stable OID for publishing + dashboard filtering
       ============================================================ */

    -- Flows (Route)
    TRUNCATE TABLE transitapp.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31;

    INSERT INTO transitapp.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31 (
        oid, trip_date, route_short_name, Origin_BG, Dest_BG, service_group, provider_name,
        leg_count, trip_count, transfer_trip_count,
        avg_manhattan_mi, avg_euclidean_mi, avg_leg_minutes,
        mean_start_lon, mean_start_lat, mean_end_lon, mean_end_lat
    )
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY trip_date, route_short_name, service_group, provider_name, Origin_BG, Dest_BG
        ) AS oid,
        trip_date, route_short_name, Origin_BG, Dest_BG, service_group, provider_name,
        leg_count, trip_count, transfer_trip_count,
        CAST(avg_manhattan_mi AS float),
        CAST(avg_euclidean_mi AS float),
        CAST(avg_leg_minutes AS float),
        CAST(mean_start_lon AS float),
        CAST(mean_start_lat AS float),
        CAST(mean_end_lon AS float),
        CAST(mean_end_lat AS float)
    FROM transitapp.Flow_BG_BG_Daily_Route
    WHERE trip_date >= @start_date
      AND trip_date <  @end_date_excl
      AND mean_start_lon IS NOT NULL AND mean_start_lat IS NOT NULL
      AND mean_end_lon   IS NOT NULL AND mean_end_lat   IS NOT NULL;

    -- Transfer Hotspots (Route)
    TRUNCATE TABLE transitapp.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31;

    INSERT INTO transitapp.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31 (
        oid, trip_date, route_short_name, from_route_short_name, to_route_short_name,
        transfer_type, hotspot_key, hotspot_hash, provider_name, start_stop_name,
        transfer_events, transfer_trips, avg_leg_minutes, mean_lon, mean_lat
    )
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY trip_date, transfer_type, provider_name, hotspot_key, from_route_short_name, to_route_short_name
        ) AS oid,
        trip_date,
        route_short_name,
        from_route_short_name,
        to_route_short_name,
        transfer_type,
        hotspot_key,
        hotspot_hash,
        provider_name,
        start_stop_name,
        transfer_events,
        transfer_trips,
        CAST(avg_leg_minutes AS float),
        CAST(mean_lon AS float),
        CAST(mean_lat AS float)
    FROM transitapp.Transfer_Hotspots_Daily_Route
    WHERE trip_date >= @start_date
      AND trip_date <  @end_date_excl
      AND mean_lon IS NOT NULL
      AND mean_lat IS NOT NULL;

    -- Walk Egress (subset used for dashboarding)
    TRUNCATE TABLE transitapp.ArcGIS_Walk_Egress_Daily_Rolling31;

    INSERT INTO transitapp.ArcGIS_Walk_Egress_Daily_Rolling31 (
        oid, trip_date, walk_type, related_service, related_route,
        stop_key, stop_name, provider_name,
        walk_leg_count, trip_count,
        avg_walk_minutes, avg_walk_euclidean_mi, avg_walk_manhattan_mi,
        mean_start_lon, mean_start_lat, mean_end_lon, mean_end_lat
    )
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY trip_date, walk_type, related_service, related_route, stop_key
        ) AS oid,
        trip_date, walk_type, related_service, related_route,
        stop_key, stop_name, provider_name,
        walk_leg_count, trip_count,
        CAST(avg_walk_minutes AS float),
        CAST(avg_walk_euclidean_mi AS float),
        CAST(avg_walk_manhattan_mi AS float),
        CAST(mean_start_lon AS float),
        CAST(mean_start_lat AS float),
        CAST(mean_end_lon AS float),
        CAST(mean_end_lat AS float)
    FROM transitapp.Walk_AccessEgress_Daily
    WHERE trip_date >= @start_date
      AND trip_date <  @end_date_excl
      AND walk_type = 'EGRESS'
      AND mean_start_lon IS NOT NULL AND mean_start_lat IS NOT NULL
      AND mean_end_lon   IS NOT NULL AND mean_end_lat   IS NOT NULL;

    SET ANSI_WARNINGS ON;
END
GO
