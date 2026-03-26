{{ config(materialized = 'table') }}

with events as (
    select user_id,event_name
    from {{ ref('stg_events') }}
),

view_product as (
    select distinct user_id
    from events
    where event_name = 'view_product'
),
add_to_cart as (
    select distinct user_id
    from events
    where event_name = 'add_to_cart'
),
checkout as (
    select distinct user_id
    from events
    where event_name = 'checkout'
),
purchase as (
    select distinct user_id
    from events
    where event_name = 'purchase'
)

select 'view_product' as step, count(*) as users
from view_product

union all

select 'add_to_cart' ,count(*)
from add_to_cart

union all

select 'checkout',count(*)
from checkout

union all

select 'purchase',count(*)
from purchase
