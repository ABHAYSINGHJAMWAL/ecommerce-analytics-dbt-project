{{ config(materialized = 'view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw','refunds_raw') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY refund_id
            ORDER BY refund_timestamp DESC
        ) AS rn
    FROM source
),

cleaned AS (
    SELECT
        CAST(refund_id AS STRING) AS refund_id,
        CAST(order_id AS STRING) AS order_id,
        CAST(refund_amount AS NUMERIC) AS refund_amount,
        CAST(refund_timestamp AS TIMESTAMP) AS refund_timestamp
    FROM deduplicated
    WHERE rn = 1
)

SELECT
    refund_id,
    order_id,
    refund_amount,
    refund_timestamp
FROM cleaned