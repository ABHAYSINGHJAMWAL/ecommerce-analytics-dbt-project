Decisions.md

## Why incremental for fct_revenue
fct_revenue processes one row per order. Orders are immutable once placed except for status updates and refunds. Incremental materialization means only new or updated orders are processed on each run. In production this reduces runtime from full table scan to processing only today's orders. Note: currently using table materialization on BigQuery free tier because DML operations (MERGE) require billing to be enabled. Would revert to incremental in production.

## Why SCD Type 2 for snap_users
User attributes like country, email, and plan tier change over time. Without SCD2, a cohort analysis run today would use each users current country not the country they had when they signed up. This corrupts historical analysis. SCD2 preserves the full history of each users attributes with valid_from and valid_to timestamps. Example: user signed up in India in January, moved to UAE in March. SCD2 correctly shows them as India cohort for January retention analysis.

## Why staging layer exists
Raw tables cannot be trusted. They contain duplicates from CDC replication, nulls from optional API fields, and inconsistent formats across sources. Staging cleans once so all downstream models share one source of truth. No business logic lives in staging, only cleaning and standardization.

## Why intermediate layer exists
Some transformations are too complex for staging but too reusable to repeat in every mart. int_orders_enriched joins orders with refunds and computes net_order_value. Three different marts use this. Without the intermediate layer the same join logic would be duplicated three times.

## Why row_number deduplication
Source tables contain duplicate records when the CDC pipeline fires multiple times on the same update. row_number partitioned by primary key ordered by updated_at descending keeps only the most recent record. Known limitation: if two records have identical updated_at timestamps the selection is nondeterministic. Fix: add secondary sort on _etl_loaded_at or record_id.

## Why table materialization for marts
Mart models are queried by BI tools and analysts frequently. View materialization would re-execute the full SQL on every query. Table materialization pre-computes the result so analyst queries are fast. Trade-off: data is only as fresh as the last dbt run.

## fct_revenue incremental to table change
BigQuery free tier does not support DML operations like MERGE without enabling billing. Incremental models in dbt use MERGE under the hood. Changed to table materialization to keep pipeline running end-to-end. In production would use incremental with partition_by order_date and cluster_by user_id.

# Why dbt_expectations over basic tests
Basic not_null and unique tests verify structure but not business logic.
dbt_expectations adds statistical tests - value ranges, allowed values,
row count bounds, column types. These catch business logic failures that
structural tests miss entirely. Example: conversion_rate should always
be between 0 and 1 - a basic test cannot verify this, dbt_expectations can.

## Why dbt_utils
Replaces custom macro implementations with battle-tested dbt Labs versions.
date_spine from dbt_utils handles edge cases our custom version missed.
Using maintained packages reduces maintenance burden on the data team.