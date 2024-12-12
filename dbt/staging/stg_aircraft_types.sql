WITH
    selection AS (
        SELECT
            AircraftDescription,
            Description,
            Designator,
            CAST(REPLACE(EngineCount,'C', '2') AS INT) AS EngineCount,
            EngineType,
            ManufacturerCode,
            ModelFullName,
            WTC
        FROM {{ source('sf_source', 'src_aircraft_types_csv') }}
    )

SELECT *
FROM selection