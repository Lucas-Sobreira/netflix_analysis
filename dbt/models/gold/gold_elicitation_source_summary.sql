{{
    config(
        materialized = 'table',
        schema       = '03_gold'
    )
}}

-- gold_elicitation_source_summary
-- Summarizes user belief and prediction behaviour per elicitation source category.
-- Shows whether users are more confident / accurate for popular vs serendipitous movies.

WITH elicitation_with_beliefs AS (

    SELECT
        e.source_label,
        e.movie_id,
        b.user_id,
        b.user_predict_rating,
        b.user_elicit_rating,
        b.user_certainty,
        b.is_seen_label,
        b.system_predict_rating
    FROM {{ ref('stg_elicitation_set') }}  e
    JOIN (
        SELECT *, 
               ROW_NUMBER() OVER(PARTITION BY user_id, movie_id ORDER BY recorded_at DESC) as rn
        FROM {{ ref('stg_beliefs') }}
    ) b
        ON e.movie_id = b.movie_id
        AND e.month_idx = b.month_idx
    WHERE b.rn = 1

),

aggregated AS (

    SELECT
        source_label,

        -- Volume
        COUNT(*)                                            AS total_belief_records,
        COUNT(DISTINCT movie_id)                            AS distinct_movies,
        COUNT(DISTINCT user_id)                             AS distinct_users,

        -- Seen status breakdown
        SUM(CASE WHEN is_seen_label = 'seen'        THEN 1 ELSE 0 END) AS seen_count,
        SUM(CASE WHEN is_seen_label = 'not_seen'    THEN 1 ELSE 0 END) AS not_seen_count,
        SUM(CASE WHEN is_seen_label = 'no_response' THEN 1 ELSE 0 END) AS no_response_count,

        -- Prediction behaviour
        ROUND(AVG(user_predict_rating), 4)                  AS avg_predict_rating,
        ROUND(AVG(user_certainty), 4)                       AS avg_certainty,

        -- Actual rating (only for seen movies where elicit rating exists)
        ROUND(AVG(
            CASE WHEN is_seen_label = 'seen' THEN user_elicit_rating END
        ), 4)                                               AS avg_actual_rating,

        -- System prediction
        ROUND(AVG(system_predict_rating), 4)                AS avg_system_pred,

        -- Accuracy of user predictions (for seen movies with both values)
        ROUND(AVG(
            CASE
                WHEN is_seen_label = 'seen'
                    AND user_predict_rating IS NOT NULL
                    AND user_elicit_rating  IS NOT NULL
                THEN ABS(user_predict_rating - user_elicit_rating)
            END
        ), 4)                                               AS avg_user_prediction_mae

    FROM elicitation_with_beliefs
    GROUP BY source_label

)

SELECT *
FROM aggregated
ORDER BY avg_certainty DESC
