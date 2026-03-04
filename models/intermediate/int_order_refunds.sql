{{ config(materialized = 'view') }}

with refunds as (
    select *
    from {{ ref('stg_refunds') }}

),

aggregated as (
select order_id,sum(refund_amount) as total_refunded_amount
from refunds
group by 1

)
select *
from aggregated