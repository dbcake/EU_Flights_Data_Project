{% snapshot scd_airports %}
    {{
        config(
        target_schema='dbcake',
        strategy='check',
        unique_key='id',
        check_cols=['icao_code', 'type', 'airport', 'latitude', 'longitude', 'elevation', 'municipality', 'iso_country'],
        )
    }}
    SELECT
        *
    FROM {{ ref( 'stg_airports') }}
{% endsnapshot %}
