{{ config(materialized ='table') }}

WITH events AS (
    SELECT user_id, event_name
    FROM {{ ref('stg_events') }}
),

experiment_users AS (
    SELECT DISTINCT 
        user_id,
        CASE 
            WHEN MOD(user_id, 2) = 0 THEN 'control'
            ELSE 'treatment'
        END AS experiment_group
    FROM events
),

purchases AS (
    SELECT DISTINCT user_id
    FROM events
    WHERE event_name = 'purchase'
),

joined AS (
    SELECT 
        e.experiment_group,
        e.user_id,
        CASE 
            WHEN p.user_id IS NOT NULL THEN 1 
            ELSE 0 
        END AS purchased
    FROM experiment_users e
    LEFT JOIN purchases p
        ON e.user_id = p.user_id
),

aggregated AS (
    SELECT 
        experiment_group,
        COUNT(DISTINCT user_id) AS users,
        SUM(purchased) AS purchases
    FROM joined
    GROUP BY experiment_group
)

SELECT 
    experiment_group,
    users,
    purchases,
    {{ safe_divide('purchases','users') }} AS purchase_conversion_rate
FROM aggregated