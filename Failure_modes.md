Failure_modes.md

## 1. Source data stops arriving
Trigger: raw tables stop receiving new rows due to upstream app deployment or API failure
Affected models: fct_revenue, mart_growth_metrics, mart_product_metrics
Failure type: SILENT - pipeline runs successfully, metrics show zero activity
Current detection: none
Fix direction: dbt source freshness checks with warn_after 4 hours and error_after 8 hours
Interview note: most dangerous failure type, looks like a real drop in DAU

## 2. Schema change in source
Trigger: engineering team renames a column without notifying data team
Affected models: stg_orders fails first then all downstream models
Failure type: LOUD - dbt run throws column not found error
Current detection: dbt run failure
Fix direction: dbt model contracts on staging layer to enforce expected schema

## 3. Deduplication nondeterminism
Trigger: two source rows have identical updated_at timestamps
Affected models: all staging models using row_number deduplication
Failure type: SILENT - wrong record selected, no error thrown
Current detection: none
Fix direction: add secondary sort column such as record_id or _etl_loaded_at

## 4. Timezone misattribution
Trigger: events near midnight IST are bucketed to wrong date in UTC
Affected models: mart_growth_metrics DAU calculation, mart_product_metrics
Failure type: SILENT - DAU counts slightly wrong every day
Current detection: none
Fix direction: CAST(event_timestamp AT TIME ZONE Asia/Kolkata AS DATE)
Scale of error: roughly 30 to 90 minutes of events per day misattributed

## 5. Experiment assignment contamination
Trigger: a user appears in both control and treatment groups due to assignment bug
Affected models: mart_ab_test_results
Failure type: SILENT - conversion rates wrong for both groups, no error
Current detection: none
Fix direction: unique test on user_id and experiment_id combination