{{ config(materialized = 'view') }}

with orders as (
    select *
    from {{ ref('stg_orders') }}

),

users as (
    select *
    from {{ ref('stg_users') }}
),

joined as (
    select  o.order_id,o.user_id, o.order_timestamp, o.order_amount,   o.currency, u.signup_timestamp, u.country, u.acquisition_channel
    from orders o
    left join users u
    on o.user_id = u.user_id
)

select *
from joined