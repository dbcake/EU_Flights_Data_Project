WITH
    selection AS (
        SELECT
            event_id
            ,type
            ,value
        FROM {{ ref( 'stg_measurements') }}
    ),
    pivoted AS(
        SELECT
            event_id
            ,"'Time Passed (s)'" AS time_passed
            ,"'Distance flown (NM)'" AS distance_flown
        FROM selection
            PIVOT(MAX(value) FOR TYPE IN
        (ANY ORDER BY TYPE))  
    )
SELECT *
FROM pivoted