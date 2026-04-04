{{ config(materialized='table') }}

WITH experiment_assignments AS (
    SELECT
        experiment_id,
        experiment_name,
        user_id,
        experiment_group,
        assigned_at,
        experiment_start_date
    FROM {{ ref('stg_experiment_assignments') }}
),

purchases AS (
    SELECT DISTINCT 
        CAST(user_id AS STRING) AS user_id
    FROM {{ ref('stg_events') }}
    WHERE event_name = 'purchase'
),

user_level AS (
    SELECT
        e.experiment_id,
        e.experiment_name,
        e.user_id,
        e.experiment_group,
        e.experiment_start_date,
        CASE WHEN p.user_id IS NOT NULL THEN 1 ELSE 0 END AS converted
    FROM experiment_assignments e
    LEFT JOIN purchases p 
        ON e.user_id = p.user_id
),

experiment_stats AS (
    SELECT
        experiment_id,
        experiment_name,
        experiment_start_date,
        experiment_group,
        COUNT(DISTINCT user_id) AS users,
        SUM(converted) AS conversions,
        {{ safe_divide('SUM(converted)', 'COUNT(DISTINCT user_id)') }} AS conversion_rate
    FROM user_level
    GROUP BY 1, 2, 3, 4
),

pivoted AS (
    SELECT
        experiment_id,
        experiment_name,
        MIN(experiment_start_date) AS experiment_start_date,
        MAX(CASE WHEN experiment_group = 'control' THEN users END) AS control_users,
        MAX(CASE WHEN experiment_group = 'treatment' THEN users END) AS treatment_users,
        MAX(CASE WHEN experiment_group = 'control' THEN conversions END) AS control_conversions,
        MAX(CASE WHEN experiment_group = 'treatment' THEN conversions END) AS treatment_conversions,
        MAX(CASE WHEN experiment_group = 'control' THEN conversion_rate END) AS control_rate,
        MAX(CASE WHEN experiment_group = 'treatment' THEN conversion_rate END) AS treatment_rate
    FROM experiment_stats
    GROUP BY 1, 2
),

final AS (
    SELECT
        experiment_id,
        experiment_name,
        experiment_start_date,
        control_users,
        treatment_users,
        control_users + treatment_users AS total_users,
        control_conversions,
        treatment_conversions,
        ROUND(control_rate, 4) AS control_conversion_rate,
        ROUND(treatment_rate, 4) AS treatment_conversion_rate,
        ROUND(treatment_rate - control_rate, 4) AS absolute_lift,
        ROUND({{ safe_divide('(treatment_rate - control_rate)', 'control_rate') }}, 4) AS relative_lift,

        CASE
            WHEN ABS(control_users - treatment_users) /
                CAST(control_users + treatment_users AS FLOAT64) > 0.05
            THEN TRUE ELSE FALSE
        END AS sample_ratio_mismatch,

        CASE
            WHEN (
                ABS(treatment_rate - control_rate) /
                NULLIF(SQRT(
                    ((control_conversions + treatment_conversions) /
                     CAST(control_users + treatment_users AS FLOAT64)) *
                    (1 - (control_conversions + treatment_conversions) /
                     CAST(control_users + treatment_users AS FLOAT64)) *
                    (1.0/control_users + 1.0/treatment_users)
                ), 0)
            ) > 1.96
            THEN TRUE ELSE FALSE
        END AS is_significant,

        DATE_DIFF(CURRENT_DATE(), experiment_start_date, DAY) AS days_running,

        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), experiment_start_date, DAY) >= 7
            THEN TRUE ELSE FALSE
        END AS has_minimum_duration,

        CURRENT_TIMESTAMP() AS calculated_at

    FROM pivoted
)

SELECT * FROM final