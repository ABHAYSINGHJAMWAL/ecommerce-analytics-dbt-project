{{ config(materialized='table') }}

WITH spine AS (
    SELECT
        DATE_ADD(DATE('2023-01-01'), INTERVAL spine_number DAY) AS date_day
    FROM
        UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE('2026-12-31'), DATE('2023-01-01'), DAY))) AS spine_number
)

SELECT date_day FROM spine