Scale_notes.md
## Partitioning and clustering implemented

fct_revenue
- partition_by: order_date (day granularity)
- cluster_by: user_id
- Reason: revenue queries filter by date range and group by user and status

mart_product_metrics
- partition_by: event_date (day granularity)
- Reason: DAU queries always filter by date, partition eliminates full scans

mart_growth_metrics
- partition_by: metric_date (day granularity)
- Reason: growth metric queries are date-range based by nature

## fct_revenue at 500M rows
Current issue: no partitioning, full table scan on every query
Fix: partition_by order_date day granularity, cluster_by user_id and order_status
Expected improvement: query cost drops 95 percent for date-range filtered queries
BigQuery scans only relevant partitions instead of full 500M row table

## mart_user_retention at 10M users
Current issue: cohort calculation requires joining every user to every event
This creates a large cross join at scale
Fix: pre-aggregate daily active users at int layer before mart
Reduces fan-out significantly before the cohort bucketing step

## Prefect pipeline gaps at production scale
Current pipeline: dbt run then dbt test with no error handling
Missing in production:
No alerting if dbt run fails overnight
No source freshness check before models run
No retry logic for transient BigQuery errors
No SLA monitoring
No Slack or email notification on test failures
Fix direction: Prefect alerts on flow failure, Elementary for test monitoring

## mart_user_sessions at 100M events
Current model uses window functions over full event table
At 100M events this is expensive with no partition filter
Fix: partition events by event_date, filter sessionization to rolling 90 days