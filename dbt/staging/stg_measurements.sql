WITH
    selection AS (
        SELECT
            id
            ,event_id
            ,type
            ,value
        FROM {{ source('sf_source', 'src_measurements_parquet') }}
    )
SELECT *
FROM selection