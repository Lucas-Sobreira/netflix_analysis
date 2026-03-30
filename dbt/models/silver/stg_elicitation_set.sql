{{
    config(
        materialized = 'table',
        schema       = '02_silver'
    )
}}

-- stg_elicitation_set: cleaned movie_elicitation_set table
-- Maps source integer code to a descriptive label

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'br_movie_elicitation_set') }}
    WHERE _rescued_data IS NULL

),

cleaned AS (

    SELECT
        CAST(movieId   AS BIGINT)   AS movie_id,
        CAST(month_idx AS INT)      AS month_idx,

        -- Map source group integer to human-readable label
        CASE CAST(source AS INT)
            WHEN 1 THEN 'popularity'
            WHEN 2 THEN 'highly_rated'
            WHEN 3 THEN 'popular_recent'
            WHEN 4 THEN 'trending'
            WHEN 5 THEN 'serendipity'
            ELSE        'unknown'
        END                         AS source_label,

        CAST(
            FROM_UNIXTIME(CAST(tstamp AS BIGINT))
        AS TIMESTAMP)               AS presented_at

    FROM source

)

SELECT *
FROM cleaned
