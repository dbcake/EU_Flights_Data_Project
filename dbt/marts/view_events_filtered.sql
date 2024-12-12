{{ config(materialized='view') }}
WITH all_events AS (
    SELECT
        ID
        ,FLIGHT_ID
        ,TYPE
        ,EVENT_TIME
        ,LONGITUDE
        ,LATITUDE
        ,ALTITUDE
        ,PREV_EVENT_TIME
        ,PREV_LONGITUDE
        ,PREV_LATITUDE
        ,PREV_ALTITUDE
    FROM {{ ref( 'view_events_lagged') }}
    WHERE
        type NOT IN (
                'entry-parking_position'
                ,'entry-runway'
                ,'entry-taxiway'
                ,'exit-parking_position'
                ,'exit-runway'
                ,'exit-taxiway'
                -- ,'first-xing-fl100'
                -- ,'first-xing-fl245'
                -- ,'first-xing-fl50'
                -- ,'first-xing-fl70'
                -- ,'first_seen'
                -- ,'landing'
                -- ,'last-xing-fl100'
                -- ,'last-xing-fl245'
                -- ,'last-xing-fl50'
                -- ,'last-xing-fl70'
                -- ,'last_seen'
                -- ,'level-end'
                -- ,'level-start'
                -- ,'take-off'
                -- ,'top-of-climb'
                -- ,'top-of-descent'
        )
) SELECT
    *
FROM all_events