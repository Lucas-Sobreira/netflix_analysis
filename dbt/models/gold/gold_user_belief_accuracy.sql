{{
    config(
        materialized = 'table',
        schema       = '03_gold'
    )
}}

-- gold_user_belief_accuracy
-- Measures how accurately users predict their own ratings.
-- Only considers records where the user has seen the movie AND provided both 
-- an elicited rating and a predicted rating.

WITH seen_with_both AS (

    SELECT
        user_id,
        movie_id,
        user_elicit_rating,
        user_predict_rating,
        user_certainty,
        -- Absolute difference between what they predicted and what they actually rated
        ABS(user_elicit_rating - user_predict_rating) AS prediction_error
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER(PARTITION BY user_id, movie_id ORDER BY recorded_at DESC) as rn
        FROM {{ ref('stg_beliefs') }}
    ) b
    WHERE
        b.is_seen_label      = 'seen'
        AND b.user_elicit_rating   IS NOT NULL
        AND b.user_predict_rating  IS NOT NULL
        AND b.rn = 1

),

aggregated AS (

    SELECT
        user_id,

        -- Volume
        COUNT(*)                                    AS rated_movies_count,

        -- Accuracy metrics
        ROUND(AVG(prediction_error), 4)             AS mean_absolute_error,
        ROUND(STDDEV(prediction_error), 4)          AS stddev_error,
        ROUND(MAX(prediction_error), 1)             AS max_error,

        -- Confidence
        ROUND(AVG(user_certainty), 4)               AS avg_certainty,

        -- Proportion of "exact" predictions (error <= 0.5 stars)
        ROUND(
            100.0 * SUM(CASE WHEN prediction_error <= 0.5 THEN 1 ELSE 0 END)
            / COUNT(*),
            2
        )                                           AS pct_accurate_predictions

    FROM seen_with_both
    GROUP BY user_id

)

SELECT *
FROM aggregated
ORDER BY mean_absolute_error ASC
