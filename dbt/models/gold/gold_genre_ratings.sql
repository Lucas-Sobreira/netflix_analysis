{{
    config(
        materialized = 'table',
        schema       = '03_gold'
    )
}}

-- gold_genre_ratings
-- Average real user rating per genre, with engagement metrics.
-- Joins the Silver movies model (one row per movie-genre) with unified ratings.

WITH genre_ratings AS (

    SELECT
        m.genre,
        r.rating,
        r.movie_id,
        r.user_id
    FROM {{ ref('stg_movies') }}   m
    JOIN {{ ref('stg_ratings') }}  r
        ON m.movie_id = r.movie_id
    -- Exclude movies with no genre information
    WHERE m.genre IS NOT NULL

),

aggregated AS (

    SELECT
        genre,

        -- Volume metrics
        COUNT(*)                            AS total_ratings,
        COUNT(DISTINCT movie_id)            AS distinct_movies,
        COUNT(DISTINCT user_id)             AS distinct_users,

        -- Rating statistics
        ROUND(AVG(rating), 4)               AS avg_rating,
        ROUND(MIN(rating), 1)               AS min_rating,
        ROUND(MAX(rating), 1)               AS max_rating,
        ROUND(STDDEV(rating), 4)            AS stddev_rating,

        -- Distribution buckets (% of ratings >= 4.0 = "good" movies)
        ROUND(
            100.0 * SUM(CASE WHEN rating >= 4.0 THEN 1 ELSE 0 END)
            / COUNT(*),
            2
        )                                   AS pct_high_ratings

    FROM genre_ratings
    GROUP BY genre

)

SELECT *
FROM aggregated
ORDER BY avg_rating DESC
