{{
    config(
        materialized = 'table',
        schema       = '03_gold'
    )
}}

-- gold_system_vs_actual
-- Compares the recommendation system's predicted rating against the actual
-- rating the user reported after watching the movie.
-- Aggregated per movie for a performance overview.

WITH paired AS (

    SELECT
        b.movie_id,
        b.user_id,
        b.system_predict_rating,
        b.user_elicit_rating,
        -- Absolute error between system prediction and actual rating
        ABS(b.system_predict_rating - b.user_elicit_rating) AS abs_error,
        -- Signed bias: positive = system over-predicted
        b.system_predict_rating - b.user_elicit_rating       AS signed_bias
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER(PARTITION BY user_id, movie_id ORDER BY recorded_at DESC) as rn
        FROM {{ ref('stg_beliefs') }}
    ) b
    WHERE
        b.system_predict_rating IS NOT NULL
        AND b.user_elicit_rating    IS NOT NULL
        AND b.is_seen_label         = 'seen'
        AND b.rn = 1

),

per_movie AS (

    SELECT
        movie_id,

        COUNT(*)                                AS record_count,
        COUNT(DISTINCT user_id)                 AS distinct_users,

        -- System prediction quality
        ROUND(AVG(system_predict_rating), 4)    AS avg_system_pred,
        ROUND(AVG(user_elicit_rating), 4)       AS avg_actual_rating,

        -- Error metrics
        ROUND(AVG(abs_error), 4)                AS mae,
        ROUND(STDDEV(abs_error), 4)             AS stddev_error,

        -- Bias: positive = system tends to over-predict for this movie
        ROUND(AVG(signed_bias), 4)              AS mean_bias,

        -- Quality flag
        CASE
            WHEN AVG(abs_error) <= 0.5 THEN 'good'
            WHEN AVG(abs_error) <= 1.0 THEN 'fair'
            ELSE 'poor'
        END                                     AS prediction_quality

    FROM paired
    GROUP BY movie_id
    HAVING COUNT(*) >= 5   -- only movies with enough observations

)

SELECT
    p.*,
    m.title,
    m.year_movie
FROM per_movie  p
LEFT JOIN (
    SELECT DISTINCT movie_id, title, year_movie
    FROM {{ ref('stg_movies') }}
) m ON p.movie_id = m.movie_id
ORDER BY mae ASC
