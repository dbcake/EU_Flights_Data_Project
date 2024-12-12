WITH
    all_flights AS (
        SELECT
            ID
            ,UPPER(ADEP) AS ADEP
            ,UPPER(ADES) AS ADES
            ,UPPER(ICAO24) AS ICAO24
            ,UPPER(FLT_ID) AS FLT_ID
            ,TO_TIMESTAMP_NTZ (FIRST_SEEN) AS FIRST_SEEN
            ,TO_TIMESTAMP_NTZ (LAST_SEEN) AS LAST_SEEN
            ,TO_DATE (DOF) AS DOF
        FROM {{ source('sf_source', 'src_flights_parquet') }}
    )
SELECT *
FROM all_flights