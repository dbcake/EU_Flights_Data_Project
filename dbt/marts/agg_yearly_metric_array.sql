{{  
    config(
        materialized='incremental',
        on_schema_change='fail',
        incremental_strategy='merge',
        unique_key = ['hex','year_start'],
        merge_update_columns = ['metric_array'],
    )
}}

WITH  grid_path AS (
    SELECT
        flight_id
        ,H3_TRY_GRID_PATH(
                H3_LATLNG_TO_CELL_STRING(LATITUDE, LONGITUDE, 6 ) ,
                H3_LATLNG_TO_CELL_STRING( PREV_LATITUDE, PREV_LONGITUDE, 6)
            ) AS grid_array
    FROM {{ ref('view_events_filtered')}}
    WHERE PREV_LATITUDE IS NOT NULL AND PREV_LONGITUDE IS NOT NULL
    AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
), flattened AS (
    SELECT 
        flight_id
        ,value as h3_id
    FROM grid_path, 
        LATERAL FLATTEN(INPUT => grid_array  ) AS grid_values
) 
, current_period AS(
    SELECT
        h3_id AS hex
        ,( SELECT TO_DATE(DATE_TRUNC(year, MIN(event_time)))  FROM {{ ref('view_events_filtered')}}) AS year_start
        ,COUNT( DISTINCT flight_id) AS count
    FROM flattened
    GROUP BY h3_id
) 
, previous AS (
    {% if is_incremental() %}
        SELECT
            *
        FROM {{ this }}
        WHERE year_start = ( SELECT TO_DATE(DATE_TRUNC(year, MIN(event_time)))  FROM {{ ref('view_events_filtered')}})
    {% endif %}
    {% if not is_incremental() %}
        SELECT 
            * 
        FROM ( SELECT NULL AS HEX, NULL AS year_start, NULL as metric_array )
        WHERE FALSE
    {% endif %}
)
SELECT 
    COALESCE( p.hex, c.hex) AS hex
    ,COALESCE(p.year_start, c.year_start) AS year_start
    ,CASE 
        WHEN p.metric_array IS NULL AND c.count IS NOT NULL
            THEN ARRAY_APPEND(
                FILL_ARRAY(
                    CAST(
                        (
                        SELECT 
                            MAX(ARRAY_SIZE(this.metric_array)) 
                        FROM  previous AS this
                        )
                        AS NUMBER(3, 0 ))
                    )
                , c.count)
        WHEN p.metric_array IS NOT NULL AND c.count IS NOT NULL
            THEN ARRAY_APPEND(p.metric_array, c.count)
        WHEN p.metric_array IS NOT NULL AND c.count IS NULL
            THEN ARRAY_APPEND(p.metric_array, 0)
    END AS metric_array
FROM current_period c
FULL OUTER JOIN previous p
ON c.hex = p.hex

{% if is_incremental() %}

{% endif %}
