WITH
    selection AS (
        SELECT
            a.icao24
            ,a.Operatoricao
            ,a.Country
            ,a.Registration
            ,a.ManufacturerIcao
            ,a.Manufacturer
            ,at.TypeDesignator
            ,at.AircraftType
            ,at.EngineCount
            ,at.EngineType
            ,at.WTC
        FROM {{ ref('stg_aircrafts') }} a
        LEFT JOIN {{ ref('int_aircraft_types') }} at
        ON a.ManufacturerIcao = at.ManufacturerCode
        AND a.TypeCode = at.TypeDesignator
    )
SELECT *
FROM selection