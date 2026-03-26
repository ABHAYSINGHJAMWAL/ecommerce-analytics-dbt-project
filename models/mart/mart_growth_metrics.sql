{{ config(
    materialized = 'table',
    partition_by = {
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

WITH events AS (
    SELECT
        user_id,
        DATE(event_timestamp) AS event_date   
    FROM {{ ref('stg_events') }}
),

dau AS (
    SELECT
        event_date,
        COUNT(DISTINCT user_id) AS dau
    FROM events
    GROUP BY event_date
),

wau AS (
    SELECT 
        DATE_TRUNC(event_date, WEEK) AS week_start,
        COUNT(DISTINCT user_id) AS wau
    FROM events
    GROUP BY week_start
),

mau AS (
    SELECT
        DATE_TRUNC(event_date, MONTH) AS month_start,
        COUNT(DISTINCT user_id) AS mau
    FROM events
    GROUP BY month_start
)

SELECT
    d.event_date,
    d.dau,
    w.wau,
    m.mau,
    SAFE_DIVIDE(CAST(d.dau AS FLOAT64), m.mau) AS stickiness_ratio
FROM dau d
LEFT JOIN wau w
    ON DATE_TRUNC(d.event_date, WEEK) = w.week_start
LEFT JOIN mau m
    ON DATE_TRUNC(d.event_date, MONTH) = m.month_start
