
{{ config(materialized='view') }}
  with source  as (
    select *
    from {{ source('raw', 'orders_raw') }}
),

deduplicated as (
select *,row_number() over( partition by order_id order by created_at desc ) as rn
from source

),

cleaned as (SELECT
order_id,user_id,order_timestamp,coalesce(order_amount,0) as order_amount,
upper(currency) as currency,created_at
from deduplicated
where rn = 1
)
select *
 from cleaned