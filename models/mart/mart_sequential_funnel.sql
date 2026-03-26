{{ config(materialized = 'table') }}

with events as (
    select user_id,event_name,event_timestamp
    from {{ ref('stg_events') }}

),

view_product as (
    select user_id,min(event_timestamp ) as view_time
    from events
    where event_name = 'view_product'
    group by user_id
),

add_to_cart as (
    select e.user_id,min(e.event_timestamp) as cart_time
    from events e
    join view_product v
    on e.user_id = v.user_id
    and e.event_timestamp > v.view_time
    where event_name = 'add_to_cart'
    group by e.user_id
),

checkout as (
select e.user_id,min(e.event_timestamp) as checkout_time
from events e
join add_to_cart c
on e.user_id = c.user_id
and e.event_timestamp > c.cart_time
where e.event_name = 'checkout'
group by e.user_id
),

purchase as (
    select e.user_id,min(e.event_timestamp) as purchase_time
    from events e
    join checkout c
    on e.user_id = c.user_id
    and e.event_timestamp > c.checkout_time
    where e.event_name = 'purchase'
    group by e.user_id
)

select 'view_product' as step,count(*) as users from view_product
union all
select 'add_to_cart',count(*) from add_to_cart
union all
select 'checkout',count(*) from checkout
union all
select 'purchase',count(*) from purchase


