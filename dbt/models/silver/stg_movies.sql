{{
    config(
        materialized = 'table',
        schema       = '02_silver'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'br_movies') }}
    -- Drop rows that DLT could not parse (malformed source records)
    WHERE _rescued_data IS NULL

),

processed AS (

    SELECT
        -- Rename to snake_case
        movieId                                                                     AS movie_id,

        -- Clean title: strip the trailing "(YYYY)" suffix
        TRIM(REGEXP_REPLACE(title, r'\s*\(\d{4}\)\s*$', ''))                       AS title,

        -- Extract the 4-digit year from the title into its own column
        TRY_CAST(REGEXP_EXTRACT(title, r'\((\d{4})\)') AS INT)                     AS year_movie,

        -- Detect IMAX: TRUE if the movie was tagged with IMAX in any genre slot
        ARRAY_CONTAINS(SPLIT(genres, '\\|'), 'IMAX')                               AS is_imax,

        -- Build a clean genres array:
        --   1. Split on pipe
        --   2. Remove 'IMAX' (moved to its own boolean column)
        --   3. Remove '(no genres listed)' → becomes NULL via the CASE below
        --   4. If the resulting array is empty (e.g. only had IMAX), return NULL
        CASE
            WHEN ARRAY_SIZE(
                FILTER(
                    SPLIT(genres, '\\|'),
                    g -> g NOT IN ('IMAX', '(no genres listed)')
                )
            ) = 0
                THEN NULL
            ELSE
                FILTER(
                    SPLIT(genres, '\\|'),
                    g -> g NOT IN ('IMAX', '(no genres listed)')
                )
        END                                                                         AS genres_array

    FROM source

)

-- Explode genres: one row per genre.
-- LATERAL VIEW OUTER ensures that movies with NULL genres_array
-- still produce exactly one row, with genre = NULL.
SELECT
    movie_id,
    title,
    year_movie,
    is_imax,
    genre

FROM processed
LATERAL VIEW OUTER EXPLODE(genres_array) AS genre
