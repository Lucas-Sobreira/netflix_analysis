{{
    config(
        materialized = 'table',
        schema       = '02_silver'
    )
}}

-- stg_beliefs: cleaned belief_data table
-- Maps isSeen integer enum to a readable label
-- Casts all numeric and timestamp columns

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'br_belief_data') }}
    WHERE _rescued_data IS NULL

),

cleaned AS (

    SELECT
        -- Identifiers
        CAST(userId    AS BIGINT)       AS user_id,
        CAST(movieId   AS BIGINT)       AS movie_id,

        -- Seen status: map integer enum to descriptive label
        CASE CAST(isSeen AS INT)
            WHEN  1 THEN 'seen'
            WHEN  0 THEN 'not_seen'
            WHEN -1 THEN 'no_response'
            ELSE        'unknown'
        END                             AS is_seen_label,

        -- Watch date: only populated when isSeen = 1
        TRY_CAST(watchDate AS DATE)     AS watch_date,

        -- Rating columns (scale 0.5 to 5.0)
        CAST(userElicitRating   AS DOUBLE)  AS user_elicit_rating,
        CAST(userPredictRating  AS DOUBLE)  AS user_predict_rating,
        CAST(systemPredictRating AS DOUBLE) AS system_predict_rating,

        -- Confidence score 1–5
        CAST(userCertainty AS INT)      AS user_certainty,

        -- Timestamps
        CAST(
            FROM_UNIXTIME(CAST(tstamp AS BIGINT))
        AS TIMESTAMP)                   AS recorded_at,

        -- Experiment metadata
        CAST(movie_idx AS INT)          AS month_idx,
        CAST(source    AS INT)          AS source_group

    FROM source

)

SELECT *
FROM cleaned
