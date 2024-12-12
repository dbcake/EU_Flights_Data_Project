WITH joined AS (
    SELECT
        e.*
        ,f.dof
    FROM {{ ref( 'stg_events') }} e
    JOIN {{ ref( 'fct_flights')  }} f
    ON f.id = e.flight_id
)
SELECT * FROM joined

