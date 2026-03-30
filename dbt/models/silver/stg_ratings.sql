{{
    config(
        materialized = 'table',
        schema       = '02_silver'
    )
}}

-- stg_ratings: unified, cleaned rating table
-- UNION of user_rating_history + user_additional_rating (both have identical schemas)

WITH source_user_rating_history AS (

    SELECT *
    FROM {{ source('bronze', 'br_user_rating_history') }}
    WHERE _rescued_data IS NULL

), 

source_user_additional_rating AS (

    SELECT *
    FROM {{ source('bronze', 'br_ratings_for_additional_users') }}
    WHERE _rescued_data IS NULL

),

rating_history AS (

    SELECT
        CAST(userId    AS BIGINT)   AS user_id,
        CAST(movieId   AS BIGINT)   AS movie_id,
        TRY_CAST(rating AS DOUBLE)  AS rating,
        -- Convert Unix epoch (seconds) to readable timestamp and date
        CAST(
            FROM_UNIXTIME(TRY_CAST(tstamp AS BIGINT))
        AS TIMESTAMP)               AS rated_at,
        'rating_history'            AS source_table
    FROM source_user_rating_history
    WHERE TRY_CAST(rating AS DOUBLE) IS NOT NULL

),

additional_rating AS (

    SELECT
        CAST(userId    AS BIGINT)   AS user_id,
        CAST(movieId   AS BIGINT)   AS movie_id,
        TRY_CAST(rating AS DOUBLE)  AS rating,
        CAST(
            FROM_UNIXTIME(TRY_CAST(tstamp AS BIGINT))
        AS TIMESTAMP)               AS rated_at,
        'additional_rating'         AS source_table
    FROM source_user_additional_rating
    WHERE TRY_CAST(rating AS DOUBLE) IS NOT NULL

),

unioned AS (

    SELECT * FROM rating_history
    UNION ALL
    SELECT * FROM additional_rating

)

SELECT
    user_id,
    movie_id,
    rating,
    rated_at,
    CAST(rated_at AS DATE)          AS rated_date,
    source_table
FROM unioned
