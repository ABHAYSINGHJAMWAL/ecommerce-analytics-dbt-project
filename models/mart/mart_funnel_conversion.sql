{{ config( materialized = 'table') }}

with events as (
    select *
    from  {{ ref('stg_events') }}

),

step_times as (
select user_id,
min(case when event_name = 'view_product' then event_timestamp end) as view_time,
min(case when event_name = 'add_to_cart' then event_timestamp end ) as cart_time,
min(case when event_name = 'purchase' then event_timestamp end )as purchase_time
from events
group by 1


)
select *
from step_times