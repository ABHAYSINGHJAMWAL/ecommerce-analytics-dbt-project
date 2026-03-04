{{ config(materialized = 'view') }}
with source as (
    select *
    from {{ source('raw','refunds_raw') }}
),

deduplicated as (
    select*,row_number() over ( partition by refund_id order by refund_timestamp desc) as rn
    from source
),

cleaned as (
    select refund_id,order_id,refund_amount,refund_timestamp
    from deduplicated
    where rn = 1
)
select *
from cleaned
