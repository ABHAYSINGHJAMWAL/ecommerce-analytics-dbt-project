select *
from {{ ref('fct_revenue') }}
where total_refund_amount > order_amount