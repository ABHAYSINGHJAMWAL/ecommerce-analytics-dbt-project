{{ config(materialized = 'table') }}

WITH events AS (
    SELECT user_id, event_name
    FROM {{ ref('stg_events') }}
),

total_users AS (
    SELECT COUNT(DISTINCT user_id) AS total_users
    FROM events
),

feature_usage AS (
    SELECT 
        event_name AS feature,
        COUNT(DISTINCT user_id) AS users_using_feature
    FROM events
    GROUP BY event_name
)

SELECT 
    f.feature,
    f.users_using_feature,
    t.total_users,
    SAFE_DIVIDE(CAST(f.users_using_feature AS FLOAT64), t.total_users) AS adoption_rate
FROM feature_usage f
CROSS JOIN total_users t
ORDER BY adoption_rate DESC