{{ config(materialized = 'view' ) }}

with source as (
    select *
    from {{ source('raw', 'events_raw') }}

),

deduplicated as (
    select *,row_number() over(partition by user_id order by event_timestamp) as rn
    from source
),

cleaned as (
    select user_id,event_name,event_timestamp
    from deduplicated
    where rn = 1
)
select *
from cleaned
