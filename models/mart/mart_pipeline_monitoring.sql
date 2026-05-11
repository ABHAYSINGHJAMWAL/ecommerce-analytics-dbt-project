{{ config(materialized='table') }}

WITH model_row_counts AS (
    SELECT 'fct_revenue' AS model_name,
           COUNT(*) AS row_count,
           MAX(order_date) AS latest_date,
           CURRENT_TIMESTAMP() AS checked_at
    FROM {{ ref('fct_revenue') }}

    UNION ALL

    SELECT 'mart_growth_metrics',
           COUNT(*),
           MAX(event_date),
           CURRENT_TIMESTAMP()
    FROM {{ ref('mart_growth_metrics') }}

    UNION ALL

    SELECT 'mart_user_retention',
           COUNT(*),
           MAX(cohort_month),
           CURRENT_TIMESTAMP()
    FROM {{ ref('mart_user_retention') }}

    UNION ALL

    SELECT 'mart_ab_test_results',
           COUNT(*),
           MAX(experiment_start_date),
           CURRENT_TIMESTAMP()
    FROM {{ ref('mart_ab_test_results') }}
),

with_health_flags AS (
    SELECT
        model_name,
        row_count,
        latest_date,
        checked_at,
        CASE
            WHEN row_count = 0 THEN 'CRITICAL - empty table'
            WHEN row_count < 2 THEN 'WARNING - low row count'
            ELSE 'OK'
        END AS health_status,
        DATE_DIFF(CURRENT_DATE(), CAST(latest_date AS DATE), DAY) AS days_since_latest_data
    FROM model_row_counts
)

SELECT * FROM with_health_flags
ORDER BY
    CASE health_status
        WHEN 'CRITICAL - empty table' THEN 1
        WHEN 'WARNING - low row count' THEN 2
        ELSE 3
    END