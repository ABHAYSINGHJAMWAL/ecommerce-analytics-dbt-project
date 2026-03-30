{{ config(
    materialized = 'table'
) }}

WITH orders AS (
    SELECT *
    FROM {{ ref('int_orders_enriched') }}
),

refunds AS (
    SELECT *
    FROM {{ ref('int_order_refunds') }}
),

joined AS (
    SELECT
        o.order_id,
        o.user_id,
        o.order_timestamp,
        DATE(o.order_timestamp) AS order_date,
        o.order_amount,
        o.currency,
        o.country,
        o.acquisition_channel,
        COALESCE(r.total_refunded_amount, 0) AS total_refund_amount,
        GREATEST(
            o.order_amount - COALESCE(r.total_refunded_amount, 0),
            CAST(0 AS NUMERIC)
        ) AS net_revenue
    FROM orders o
    LEFT JOIN refunds r ON o.order_id = r.order_id
)

SELECT * FROM joined