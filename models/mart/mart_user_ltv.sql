{{ config(materialized = 'table') }}

with orders as (
    select user_id,order_timestamp,net_revenue
from {{ ref('fct_revenue') }}

),
aggregated as (
select user_id,
min(order_timestamp) as first_order_timestamp,
max(order_timestamp) as last_order_timestamp,
count(*) as total_orders,
sum(net_revenue) as lifetime_revenue
    from orders
    group by 1
)
select *
from aggregated