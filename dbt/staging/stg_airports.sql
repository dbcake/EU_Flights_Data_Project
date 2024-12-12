WITH
    selection AS (
        SELECT
            id
            ,ident AS icao_code
            ,type
            ,iso_country
            ,name AS airport
            ,latitude_deg AS latitude
            ,longitude_deg AS longitude
            ,elevation_ft AS elevation
            ,municipality
        FROM {{ source('sf_source', 'src_airports_csv') }}
        WHERE continent='EU'
        AND type IN ('small_airport','medium_airport', 'large_airport')
    )

SELECT *
FROM selection