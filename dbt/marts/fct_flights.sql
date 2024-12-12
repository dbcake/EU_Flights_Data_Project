{{  
    config(
        materialized='incremental',
        on_schema_change='fail',
        incremental_strategy='delete+insert',
        pre_hook="{% if is_incremental() %} DELETE FROM  {{this}} WHERE DATE_TRUNC(month, dof) = (SELECT DATE_TRUNC(month, MIN(DOF)) FROM {{ ref( 'stg_flights') }}  ) {% endif %} "
    )
}}

WITH selection AS (
    SELECT
        *
    FROM {{ ref( 'stg_flights') }} 
) SELECT * FROM selection

{% if is_incremental() %}

{% endif %}
