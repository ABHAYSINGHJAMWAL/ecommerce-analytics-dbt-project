
{{ config(materialized='view') }}
  with source  as (
    select *
    from {{ source('raw', 'orders_raw') }}
),



deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY created_at DESC
        ) AS rn
    FROM source
),

cleaned AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(user_id AS STRING) AS user_id,

        CAST(created_at AS TIMESTAMP) AS order_timestamp,
        CAST(created_at AS TIMESTAMP) AS created_at,

        CAST(COALESCE(amount, 0) AS NUMERIC ) AS order_amount,

        UPPER(currency) AS currency
    FROM deduplicated
    WHERE rn = 1
)

SELECT * 
FROM cleaned