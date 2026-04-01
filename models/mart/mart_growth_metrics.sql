{{ config(
    materialized = 'table',
    partition_by = {
        "field": "event_date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

WITH all_dates AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2024-12-31' as date)"
    ) }}
),

events AS (
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
    DATE(d.date_day) AS event_date,   
    COALESCE(da.dau, 0) AS dau,
    w.wau,
    m.mau,
    {{ safe_divide('dau', 'mau') }} AS stickiness_ratio
FROM all_dates d

LEFT JOIN dau da
    ON d.date_day = da.event_date

LEFT JOIN wau w
    ON DATE_TRUNC(d.date_day, WEEK) = w.week_start

LEFT JOIN mau m
    ON DATE_TRUNC(d.date_day, MONTH) = m.month_start

