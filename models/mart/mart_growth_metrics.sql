{{ config(materialized='table') }}

WITH events AS (
    SELECT
        user_id,
        DATE_TRUNC(event_timestamp, DAY) AS event_day
    FROM {{ ref('stg_events') }}
),

dau AS (
    SELECT
        event_day,
        COUNT(DISTINCT user_id) AS dau
    FROM events
    GROUP BY event_day
),

wau AS (
    SELECT 
        DATE_TRUNC(event_day, WEEK) AS week_start,
        COUNT(DISTINCT user_id) AS wau
    FROM events
    GROUP BY week_start
),

mau AS (
    SELECT
        DATE_TRUNC(event_day, MONTH) AS month_start,
        COUNT(DISTINCT user_id) AS mau
    FROM events
    GROUP BY month_start
)

SELECT
    d.event_day,
    d.dau,
    w.wau,
    m.mau,
    SAFE_DIVIDE(CAST(d.dau AS FLOAT64), m.mau) AS stickiness_ratio
FROM dau d
LEFT JOIN wau w
    ON DATE_TRUNC(d.event_day, WEEK) = w.week_start
LEFT JOIN mau m
    ON DATE_TRUNC(d.event_day, MONTH) = m.month_start
ORDER BY d.event_day