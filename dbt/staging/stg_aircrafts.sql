WITH
    selection AS (
        SELECT
            UPPER(ICAO24) AS ICAO24
            ,UPPER(OPERATORICAO) AS OPERATORICAO
            ,COUNTRY
            ,UPPER(REGISTRATION) AS REGISTRATION
            ,UPPER(MANUFACTURERICAO) AS MANUFACTURERICAO
            ,MANUFACTURERNAME AS MANUFACTURER
            ,UPPER(TYPECODE) AS TYPECODE
        FROM {{ source('sf_source', 'src_aircrafts_csv') }}
    )

SELECT *
FROM selection