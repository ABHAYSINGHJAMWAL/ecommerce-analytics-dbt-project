{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'user_pre_experiment_raw') }}
),

cleaned AS (
    SELECT
        CAST(user_id AS STRING) AS user_id,
        pre_experiment_purchases,
        pre_experiment_events,
        observation_period_days,
        {{ safe_divide('pre_experiment_purchases', 'observation_period_days') }}
            AS pre_experiment_conversion_rate
    FROM source
)

SELECT * FROM cleaned