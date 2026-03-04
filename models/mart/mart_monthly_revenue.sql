{{ config(materialized = 'table') }}
 
 with revenue as (
    select *
    from {{ ref('fct_revenue') }}

 ),

 aggregated as (
     select date_trunc('month',order_timestamp) as revenue_month,
     sum(order_amount) as total_revenue,
     count(order_id) as total_orders
     from revenue
     group by 1

 )
 select *
 from aggregated