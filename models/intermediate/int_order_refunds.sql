{{ config(materialized = 'view') }}

WITH refunds AS (
    SELECT *
    FROM {{ ref('stg_refunds') }}
),

aggregated AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        SUM(CAST(refund_amount AS NUMERIC )) AS total_refunded_amount
    FROM refunds
    GROUP BY 1
)


SELECT
    order_id,
    total_refunded_amount
FROM aggregated