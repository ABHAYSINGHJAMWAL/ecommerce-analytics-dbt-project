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

cleaned as (

    select
 user_id, signup_timestamp,lower(trim(country)) as country, lower(trim(acquisition_channel)) as acquisition_channel
    from deduplicated
    where rn = 1

)

select *
from cleaned