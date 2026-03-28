{{ config(materialized = 'table') }}

WITH events AS (
    SELECT 
        user_id,
        event_name,
        DATE_TRUNC(event_timestamp, DAY) AS event_day
    FROM {{ ref('stg_events') }}
),

daily_activity AS (
    SELECT 
        event_day,
        COUNT(DISTINCT user_id) AS daily_active_users,
        COUNT(*) AS total_events,
        SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchases
    FROM events
    GROUP BY event_day
)

SELECT 
    event_day,
    daily_active_users,
    total_events,
    purchases,
    {{ safe_divide('purchases', 'total_events') }} AS purchase_conversion_rate
FROM daily_activity
ORDER BY event_day