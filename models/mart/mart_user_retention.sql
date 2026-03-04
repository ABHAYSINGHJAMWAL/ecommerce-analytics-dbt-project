{{ config(materialized = 'table' ) }}

with users as (
    select user_id,date_trunc('month',signup_timestamp) as cohort_month
    from {{ ref('stg_users') }}
),

orders as (
    select user_id,date_trunc('month',order_timestamp) as order_month 
    from {{ ref('fct_revenue') }}
),

joined as (
    select u.cohort_month,o.order_month,o.user_id
    from users u
    join orders o
    on u.user_id = o.user_id
    where o.order_month >= u.cohort_month

),
aggregated as (
    select cohort_month,order_month,count(distinct user_id) as active_users
    from joined
    group by 1,2
)
select *
from aggregated
order by cohort_month,order_month