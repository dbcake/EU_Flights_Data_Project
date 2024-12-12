WITH
    selection AS (
        SELECT
            UPPER(ManufacturerCode) AS ManufacturerCode,
            UPPER(Designator) AS TypeDesignator,
            AircraftDescription AS AircraftType,
            WTC,
            EngineType,
            EngineCount
        FROM {{ ref( 'stg_aircraft_types') }}
        WHERE AircraftDescription IN('LandPlane')
        GROUP BY AircraftDescription, Designator, EngineCount, EngineType, ManufacturerCode, WTC
    )

SELECT *
FROM selection