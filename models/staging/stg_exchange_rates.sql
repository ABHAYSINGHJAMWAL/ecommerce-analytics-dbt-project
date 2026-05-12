{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'exchange_rates_raw') }}
),

cleaned AS (
    SELECT
        base_currency,
        target_currency,
        ROUND(CAST(exchange_rate AS FLOAT64), 6) AS exchange_rate,
        CAST(_ingested_at AS TIMESTAMP) AS ingested_at,
        _source AS data_source,
        DATE(CAST(_ingested_at AS TIMESTAMP)) AS rate_date
    FROM source
    WHERE exchange_rate > 0
      AND target_currency IS NOT NULL
)

SELECT * FROM cleaned