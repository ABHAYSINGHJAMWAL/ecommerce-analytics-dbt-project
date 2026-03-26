{{ config(materialized = 'table' ) }}

WITH users AS (
    SELECT 
        user_id,
        DATE_TRUNC(signup_timestamp, MONTH) AS cohort_month
    FROM {{ ref('stg_users') }}
),

orders AS (
    SELECT 
        user_id,
        DATE_TRUNC(order_timestamp, MONTH) AS order_month
    FROM {{ ref('fct_revenue') }}
),

joined AS (
    SELECT 
        u.cohort_month,
        o.order_month,
        o.user_id
    FROM users u
    JOIN orders o
        ON u.user_id = o.user_id
    WHERE o.order_month >= u.cohort_month
),

aggregated AS (
    SELECT 
        cohort_month,
        order_month,
        COUNT(DISTINCT user_id) AS active_users
    FROM joined
    GROUP BY cohort_month, order_month
)

SELECT *
FROM aggregated
ORDER BY cohort_month, order_month