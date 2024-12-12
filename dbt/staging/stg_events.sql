WITH
    all_events AS (
        SELECT
            id
            ,flight_id
            ,type
            ,TO_TIMESTAMP(event_time) AS event_time
            ,longitude
            ,latitude
            ,altitude
            ,info
        FROM {{ source('sf_source', 'src_events_parquet') }}
    )
SELECT *
FROM all_events