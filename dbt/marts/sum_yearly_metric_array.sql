{{ config(materialized='view') }}


WITH transformed AS (
    SELECT 
        replace(hex, '\"') as hex
        ,REDUCE(metric_array, 0, (acc, x) -> acc + x) AS count
    FROM {{ ref( 'agg_yearly_metric_array') }}
), ordered AS(
    SELECT
        *
    FROM transformed
    ORDER BY count DESC
) SELECT
    *
FROM ordered