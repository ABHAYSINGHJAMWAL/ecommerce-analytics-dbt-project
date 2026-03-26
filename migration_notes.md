## Mart layer BigQuery migration fixes

### Error type 1 — PostgreSQL cast syntax ::
Models affected: mart_feature_adoption, mart_growth_metrics, mart_product_metrics
Error message: Expected ")" but got ":"
Cause: BigQuery does not support :: cast operator
Fix: Replace col::type with CAST(col AS TYPE)
Note: CURRENT_TIMESTAMP is a keyword in Postgres, a function in BigQuery — CURRENT_TIMESTAMP()

### Error type 2 — Modulo / string operator %
Models affected: mart_ab_test_results, mart_user_sessions
Error message: Expected keyword THEN but got "%"
Cause: BigQuery does not support % for modulo
Fix: Replace a % b with MOD(a, b)

### Error type 3 — DATE_TRUNC argument order
Models affected: mart_monthly_revenue, mart_user_retention
Error message: A valid date part name is required but found [column_name]
Cause: BigQuery reverses DATE_TRUNC argument order vs PostgreSQL
Fix: DATE_TRUNC('month', col) → DATE_TRUNC(col, MONTH)
Note: Date part also loses quotes and becomes uppercase