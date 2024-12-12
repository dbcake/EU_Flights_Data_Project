{{  
    config(
        materialized='incremental',
        on_schema_change='fail',
        incremental_strategy='delete+insert',
        pre_hook="{% if is_incremental() %} DELETE FROM  {{this}} WHERE dof >= ( SELECT MIN(dof) FROM {{ ref( 'int_events')  }} )  AND dof <= ( SELECT MAX(dof) FROM {{ ref( 'int_events')  }}  ) {% endif %} "
    )
}}

WITH joined AS (
    SELECT
        e.*
        ,m.time_passed
        ,m.distance_flown
    FROM {{ ref( 'int_events') }} e
    LEFT JOIN  {{ ref( 'int_measurements') }} m 
    ON e. id = m.event_id
) SELECT * FROM joined

{% if is_incremental() %}

{% endif %}


