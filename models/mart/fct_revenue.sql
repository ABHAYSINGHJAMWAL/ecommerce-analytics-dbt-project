{{ config(materialized = 'incremental', unique_key = 'order_id') }}

with orders as (
    select * from  {{ ref('int_orders_enriched') }}

),

refunds as (
    select *
    from {{ ref('int_order_refunds') }}

),

joined as (
    select
   o.order_id,o.user_id,o.order_timestamp,o.order_amount,o.currency,o.country,o.acquisition_channel,
    coalesce(r.total_refunded_amount,0) as total_refund_amount,
    greatest(o.order_amount - coalesce(r.total_refunded_amount,0),0) as net_revenue
   from orders o
   left join refunds r
   on o.order_id = r.order_id
{% if is_incremental() %}
where o.order_timestamp > (select max(order_timestamp) from {{ this }} )
{% endif %}

)
select *
from joined
