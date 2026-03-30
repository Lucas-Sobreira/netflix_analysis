{{
    config(
        materialized = 'table',
        schema       = '02_silver'
    )
}}

-- stg_recommendations: cleaned user_recommendation_history table

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'br_user_recommendation_history') }}
    WHERE _rescued_data IS NULL

),

cleaned AS (

    SELECT
        CAST(userId         AS BIGINT)  AS user_id,
        CAST(movieId        AS BIGINT)  AS movie_id,
        CAST(predictedRating AS DOUBLE) AS predicted_rating,

        CAST(
            FROM_UNIXTIME(CAST(tstamp AS BIGINT))
        AS TIMESTAMP)                   AS recommended_at

    FROM source

)

SELECT *
FROM cleaned
