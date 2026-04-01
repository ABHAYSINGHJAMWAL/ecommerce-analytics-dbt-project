{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('raw', 'users_raw') }}

),

deduplicated as (

    select *,
           row_number() over ( partition by user_id order by signup_timestamp desc
           ) as rn
    from source

),

cleaned AS (
    SELECT
        CAST(user_id AS STRING) AS user_id,
        CAST(signup_timestamp AS TIMESTAMP) AS signup_timestamp,
        LOWER(TRIM(country)) AS country,
        LOWER(TRIM(acquisition_channel)) AS acquisition_channel
    FROM deduplicated
    WHERE rn = 1
)

select *
from cleaned