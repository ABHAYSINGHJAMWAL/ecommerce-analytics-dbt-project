{{ config(materialized = 'table') }}
 
WITH revenue AS (
    SELECT *
    FROM {{ ref('fct_revenue') }}
),

aggregated AS (
    SELECT 
        DATE_TRUNC(order_timestamp, MONTH) AS revenue_month,
        SUM(order_amount) AS total_revenue,
        COUNT(order_id) AS total_orders
    FROM revenue
    GROUP BY revenue_month
)

SELECT *
FROM aggregated