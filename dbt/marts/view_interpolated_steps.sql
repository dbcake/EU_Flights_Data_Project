{{ config(materialized='view') }}
WITH numbers AS (
    SELECT SEQ4() AS n
    FROM TABLE(GENERATOR(ROWCOUNT => 3000))
)
,waypoints_with_steps  AS(
    SELECT
        id
        ,flight_id
        ,type
        ,event_time AS time2
        ,longitude AS lon2
        ,latitude AS lat2
        ,altitude AS alt2
        ,prev_event_time AS time1
        ,prev_longitude AS lon1
        ,prev_latitude AS lat1
        ,prev_altitude AS alt1
        ,6371 * 2 * ASIN(
                SQRT(
                    POWER(SIN((RADIANS(lat2) - RADIANS(lat1)) / 2), 2) +
                    POWER(SIN((RADIANS(lon2) - RADIANS(lon1)) / 2), 2) * COS(RADIANS(lat1)) * COS(RADIANS(lat2)) 
                )
            ) AS distance_km
        ,CEIL(6371 * 2 * ASIN(
                SQRT(
                    POWER(SIN((RADIANS(lat2) - RADIANS(lat1)) / 2), 2) +
                    POWER(SIN((RADIANS(lon2) - RADIANS(lon1)) / 2), 2) * COS(RADIANS(lat1)) * COS(RADIANS(lat2)))
                ) / 0.9204
            ) AS steps
    FROM {{ ref( 'view_events_lagged') }}

) ,interpolated AS (
    SELECT
        id
        ,flight_id
        ,type
        ,time2 AS event_time
        ,lon2 AS longitude
        ,lat2 AS latitude
        ,alt2 AS altitude
        ,distance_km
        ,steps
        ,ARRAY_CONSTRUCT(
            lat1 + (lat2 - lat1) * n.n / steps,
            lon1 + (lon2 - lon1) * n.n / steps,
            alt1 + (alt2 - alt1) * n.n / steps,
            TIMESTAMPADD(SECOND, TIMESTAMPDIFF(SECOND, time1, time2) * n.n / steps, time1)
        )  AS waypoints
    FROM waypoints_with_steps w
    JOIN numbers n ON n.n < w.steps
    WHERE alt1 <= 3000 AND alt2 <=3000 
) 
SELECT 
    id
    ,flight_id
    ,type
    ,event_time
    ,longitude
    ,latitude
    ,altitude
    ,distance_km
    ,steps
    ,waypoints
FROM interpolated
