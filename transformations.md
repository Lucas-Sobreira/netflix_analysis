# Data Transformations Overview

This document summarizes the core transformations implemented in the Silver and Gold logic layers of the `dbt-databricks` MovieLens project.

## Silver Layer (Staging)
The Silver layer cleans, casts, and normalizes raw data coming from the DLT Bronze pipeline. These views ensure data quality and explicitly handle broken records prior to deeper aggregations.

1. **`stg_movies`** 
   - **Data Cleaning**: Strips the release year from the movie title into its own `year_movie` numeric column. 
   - **Array Unnesting**: Converts delimited genre lists into parsed arrays and breaks out `IMAX` as a standalone boolean flag. Uses `LATERAL VIEW OUTER EXPLODE` to unroll the array so that each row represents a distinct `movie_id` and `genre` combination.
2. **`stg_ratings`**
   - **Unioning Sources**: Merges the main User Rating History with the Additional Users Rating dataset into a single cohesive stream.
   - **Quality Enforcement**: Leverages `TRY_CAST` to intercept and filter out malformed rating inputs (e.g. `'NA'`) cleanly without throwing pipeline-halting cast exceptions. Converts Unix epochs into readable Data Warehouse timestamps.
3. **`stg_beliefs`**
   - **Type Normalization & Mapping**: Casts metrics into `DOUBLE` and maps integer enums (like `isSeen`) into descriptive strings (`'seen'`, `'not_seen'`).
   - **Metadata Extraction**: Extracts crucial experiment variables such as `month_idx` and `source_group`.
4. **`stg_elicitation_set`**
   - Maps DLT experiment source groups (1-5) into human-readable categories (`popularity`, `trending`, `serendipity`).
5. **`stg_recommendations`**
   - Validates schema and converts UNIX epoch timing markers into proper timestamp data types.

## Gold Layer (Business Value)
The Gold layer joins and aggregates the clean Silver records into robust, wide analytical mart tables designed for direct BI consumption and reporting.

1. **`gold_system_vs_actual`**
   - **Objective**: Evaluates the predictive accuracy of the platform's recommendation engine against verifiable real-world user ratings.
   - **Transformations**: Deduplicates overlapping user beliefs by taking the most recent input, computes absolute errors, groups aggregations by movie, and calculates rigorous standard deviations (MAE) against expected quality thresholds (`good`/`fair`/`poor`).
   - **Use Case**: Used by data scientists to monitor algorithm drift or performance per-movie over time. 
2. **`gold_user_belief_accuracy`**
   - **Objective**: Flips the perspective to measure how effectively the *users* themselves predicted their own eventual ratings over unseen movies once they ultimately watched them.
   - **Transformations**: Enforces deduplication on user histories to combat record multiplication, and computes the Mean Absolute Error across a user's predictions. Flags the proportion of predictions that fell within 0.5 stars of matching exactly (`pct_accurate_predictions`).
   - **Use Case**: Used to identify "reliable" or "oracle" raters within the platform.
3. **`gold_genre_ratings`**
   - **Objective**: Compiles vast global insights into how differing genres perform globally.
   - **Transformations**: Joins user ratings onto globally exploded movie genres. Aggregates deep volume metrics and calculates exactly what percentage of ratings are exceptionally positive (`pct_high_ratings`).
4. **`gold_elicitation_source_summary`**
   - **Objective**: Summarizes the volume and accuracy of user ratings gathered over distinct elicitation cohorts / strategies.
   - **Transformations**: Meticulously joins the raw elicitation events directly against corresponding `stg_beliefs` instances bounded by exact temporal keys (`month_idx`). Groups by `source_label` to profile accuracy metrics cleanly without Cartesian inflation.

---

## Next Steps: Dashboards & Reporting Models

Using these Gold tables, you could build out several highly impactful visualization dashboards in BI tools like Databricks SQL, Tableau, or PowerBI. Below are some tangible examples:

### 1. "Algorithm Performance" Dashboard
- **Primary Source**: `gold_system_vs_actual`
- **Visuals**: 
  - **Scatter Plot**: `avg_system_pred` vs `avg_actual_rating` on the X/Y axes to visually reveal overarching systemic bias.
  - **Categorical Pie Chart**: The distribution of `prediction_quality` buckets (`good`, `fair`, `poor`).
  - **Data Table**: Top/Bottom performing models sorted by `mae` logically exposing algorithms to refine next sprint.

### 2. "User Engagement & Cohort Tracking" Report
- **Primary Source**: `gold_elicitation_source_summary`
- **Visuals**:
  - **Clustered Bar Chart**: Comparison of `total_belief_records` natively split by `source_label`. (e.g., Does the `trending` strategy capture more feedback than the `serendipity` strategy?)
  - **100% Stacked Bar Chart**: Displaying `seen_count` vs `not_seen_count` vs `no_response_count` broken down per cohort group to map true engagement throughput.

### 3. "Content Strategy" Heatmap
- **Primary Source**: `gold_genre_ratings`
- **Visuals**:
  - **Heatmap Grid**: Overlaying Genres (Y-axis) against their `pct_high_ratings` & `stddev_rating`.
  - **Scatter Plot**: `total_ratings` (Volume) vs `avg_rating` (Quality) by genre mapped to quadrant thresholds to identify sleeper-hit genres versus saturated market choices.
