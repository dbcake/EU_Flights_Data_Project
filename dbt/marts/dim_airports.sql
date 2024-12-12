WITH
    selection AS (
        SELECT
            a.*
            ,c.code AS country_code
            ,c.name AS country
            ,c.continent
        FROM {{ ref('scd_airports') }} a
        JOIN {{ ref('seed_country_codes') }} c
        ON a.iso_country = c.code
        WHERE a.type IN ('small_airport', 'medium_airport', 'large_airport')
        AND a.municipality IS NOT NULL
    )
SELECT *
FROM selection
