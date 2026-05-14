{{ config(materialized='table') }}

WITH events AS (
    SELECT
        user_id,
        event_name,
        DATE(event_timestamp) AS event_date
    FROM {{ ref('stg_events') }}
)

SELECT
    event_date,
    COUNT(DISTINCT user_id) AS daily_active_users,
    COUNT(*) AS total_events,
    COUNTIF(event_name = 'purchase') AS purchases,
    COUNTIF(event_name = 'view_product') AS product_views,
    COUNTIF(event_name = 'add_to_cart') AS cart_adds,
    ROUND(
        COUNTIF(event_name = 'purchase') * 1.0 /
        NULLIF(COUNT(DISTINCT user_id), 0),
        4
    ) AS purchase_conversion_rate
FROM events
GROUP BY event_date