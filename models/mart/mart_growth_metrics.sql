{{ config(materialized='table') }}

WITH events AS (
    SELECT
        user_id,
        event_name,
        DATE(event_timestamp) AS event_date
    FROM {{ ref('stg_events') }}
),

daily AS (
    SELECT
        event_date,
        COUNT(DISTINCT user_id) AS dau,
        COUNT(*) AS total_events,
        COUNTIF(event_name = 'purchase') AS purchases
    FROM events
    GROUP BY event_date
)

SELECT
    event_date,
    dau,
    dau AS wau,
    dau AS mau,
    purchases,
    {{ safe_divide('dau', 'dau') }} AS stickiness_ratio,
    {{ safe_divide('purchases', 'dau') }} AS purchase_conversion_rate
FROM daily
ORDER BY event_date