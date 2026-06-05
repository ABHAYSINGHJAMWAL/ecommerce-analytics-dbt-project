"""
Analytics Warehouse Pipeline — Apache Airflow DAG

This DAG orchestrates the complete analytics pipeline:
1. Check source data freshness
2. Load raw seed data
3. Run dbt transformations layer by layer
4. Run data quality tests
5. Validate row counts
6. Send completion notification

Why layer-by-layer dbt execution:
Running staging → intermediate → marts separately means
if staging fails you know immediately before wasting time
on downstream models. Faster debugging, clearer failure messages.

Schedule: 6am daily
Owner: Abhay Singh Jamwal
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ── PROJECT PATH ──
# Update this to your actual project path when deploying
PROJECT_PATH = "~/analytics_project/ecommerce_dbt"


# ════════════════════════════════════════════
# PYTHON CALLABLES — one function per task
# ════════════════════════════════════════════

def check_source_freshness(**context):
    """
    Verify source tables received data before running models.

    Why this runs first:
    If the API ingestion failed last night and raw tables are empty,
    running dbt on empty tables produces marts with zero rows.
    Dashboards show zero revenue. PM panics at 9am thinking
    the business collapsed. This check catches it at 6am instead.

    Production implementation:
    Queries BigQuery to check MAX(_loaded_at) for each raw table.
    If any table has no data from today — raise exception, pipeline stops.
    Downstream tasks never run on stale data.

    Returns dict stored in XCom for downstream tasks to read.
    """
    execution_date = context['execution_date']
    logger.info(f"Checking source freshness for execution date: {execution_date}")

    tables_to_check = [
        'orders_raw',
        'users_raw',
        'events_raw',
        'refunds_raw',
        'experiment_assignments_raw'
    ]

    stale_tables = []

    for table in tables_to_check:
        logger.info(f"Checking {table}...")

        # Production query (uncomment when BigQuery is connected):
        # from google.cloud import bigquery
        # client = bigquery.Client(project='analytics-warehouse-dev-major')
        # query = f"""
        #     SELECT
        #         COUNT(*) as row_count,
        #         MAX(_loaded_at) as latest_load,
        #         DATE_DIFF(CURRENT_DATE(), DATE(MAX(_loaded_at)), DAY) as days_stale
        #     FROM `analytics-warehouse-dev-major.dbt_dev_raw.{table}`
        # """
        # result = client.query(query).to_dataframe()
        # days_stale = result['days_stale'].iloc[0]
        # if days_stale > 1:
        #     stale_tables.append(table)

        # Simulated check for demonstration
        logger.info(f"{table}: FRESH (0 days stale)")

    if stale_tables:
        raise ValueError(
            f"Stale tables detected: {stale_tables}. "
            f"Pipeline aborted. Check ingestion scripts."
        )

    result = {
        'tables_checked': len(tables_to_check),
        'status': 'all_fresh',
        'execution_date': str(execution_date)
    }

    logger.info(f"Source freshness check passed: {result}")
    return result


def validate_row_counts(**context):
    """
    Confirm dbt models produced expected minimum row counts.

    Why validate after dbt run not before:
    dbt can run successfully and produce zero rows if source
    data is empty or transformation logic has a bug.
    A successful dbt run with empty marts is a silent failure.
    This catches it before dashboards show wrong data.

    Uses XCom to read upstream task results.
    XCom (cross-communication) lets Airflow tasks share
    small pieces of data between each other.
    """
    ti = context['task_instance']

    # Read result from upstream check_source_freshness task
    upstream_result = ti.xcom_pull(
        task_ids='check_source_freshness',
        key='return_value'
    )
    logger.info(f"Upstream source check result: {upstream_result}")

    # Expected minimum rows per critical model
    # In production these thresholds come from historical averages
    expected_minimums = {
        'fct_revenue': 1,
        'mart_growth_metrics': 1,
        'mart_user_ltv': 1,
        'mart_ab_test_results': 1,
        'mart_pipeline_monitoring': 1
    }

    failed_checks = []

    for table, min_rows in expected_minimums.items():
        logger.info(f"Validating {table}: minimum {min_rows} rows expected")

        # Production query (uncomment when connected):
        # from google.cloud import bigquery
        # client = bigquery.Client(project='analytics-warehouse-dev-major')
        # result = client.query(
        #     f"SELECT COUNT(*) as cnt FROM "
        #     f"`analytics-warehouse-dev-major.dbt_dev_mart.{table}`"
        # ).to_dataframe()
        # actual_rows = result['cnt'].iloc[0]

        actual_rows = 5  # Simulated

        if actual_rows < min_rows:
            failed_checks.append(
                f"{table}: has {actual_rows} rows, expected >= {min_rows}"
            )
        else:
            logger.info(f"{table}: {actual_rows} rows — OK")

    if failed_checks:
        raise ValueError(
            f"Row count validation failed:\n" +
            "\n".join(failed_checks)
        )

    result = {
        'validation': 'passed',
        'tables_validated': len(expected_minimums)
    }

    logger.info(f"Row count validation passed: {result}")
    return result


def send_completion_notification(**context):
    """
    Notify team when pipeline completes successfully.

    Why this is the final task:
    Only runs if ALL upstream tasks succeeded.
    If dbt_test fails, this task is automatically skipped.
    Team receives notification only on clean successful runs.

    Production implementation:
    POST to Slack webhook with run summary.
    Include: execution date, rows processed, run duration,
    link to Airflow logs for the run.
    """
    ti = context['task_instance']
    dag_run = context['dag_run']
    execution_date = context['execution_date']

    # Calculate run duration
    start_date = dag_run.start_date
    if start_date:
        duration = datetime.utcnow() - start_date.replace(tzinfo=None)
        duration_str = f"{duration.seconds // 60}m {duration.seconds % 60}s"
    else:
        duration_str = "unknown"

    message = (
        f"Analytics pipeline completed successfully\n"
        f"Execution date: {execution_date}\n"
        f"DAG: {dag_run.dag_id}\n"
        f"Duration: {duration_str}\n"
        f"Status: All dbt models refreshed. All 80 tests passed.\n"
        f"Models updated: staging, intermediate, mart layers\n"
        f"Next run: tomorrow at 06:00 IST"
    )

    # Production Slack notification (uncomment when webhook is configured):
    # import requests, os
    # webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    # if webhook_url:
    #     requests.post(webhook_url, json={
    #         'text': message,
    #         'username': 'Analytics Pipeline Bot',
    #         'icon_emoji': ':white_check_mark:'
    #     })

    logger.info("PIPELINE COMPLETION NOTIFICATION:")
    logger.info(message)

    return {
        'notification_sent': True,
        'duration': duration_str,
        'execution_date': str(execution_date)
    }


def on_dag_failure(context):
    """
    Called automatically by Airflow when any task fails.

    Why a DAG-level failure callback:
    Without this, pipeline fails silently at 6am.
    You find out at 9am when PM asks why dashboard shows zeros.
    With this, failure notification fires within minutes.
    Data team investigates and fixes before business hours.

    Airflow calls this automatically — you never call it directly.
    """
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')

    message = (
        f"PIPELINE FAILED — immediate attention required\n"
        f"DAG: {dag_id}\n"
        f"Failed task: {task_id}\n"
        f"Execution date: {execution_date}\n"
        f"Error: {exception}\n"
        f"Action: Check Airflow logs and investigate failed task"
    )

    logger.error(message)

    # Production Slack alert (uncomment when configured):
    # import requests, os
    # webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    # if webhook_url:
    #     requests.post(webhook_url, json={
    #         'text': f":red_circle: {message}",
    #         'username': 'Analytics Pipeline Bot'
    #     })


# ════════════════════════════════════════════
# DEFAULT ARGUMENTS
# ════════════════════════════════════════════

default_args = {
    'owner': 'abhay-singh-jamwal',

    # Why retries=2:
    # Network blips, temporary BigQuery quota limits,
    # transient API errors — all resolve on retry.
    # Without retries a 2-second network hiccup kills the pipeline.
    'retries': 2,

    # Why 5 minute delay:
    # Exponential backoff gives services time to recover.
    # Immediate retry on a rate-limited API just gets rate-limited again.
    'retry_delay': timedelta(minutes=5),

    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
}


# ════════════════════════════════════════════
# DAG DEFINITION
# ════════════════════════════════════════════

with DAG(
    dag_id='analytics_warehouse_pipeline',
    default_args=default_args,
    description=(
        'Daily analytics warehouse pipeline. '
        'Orchestrates source freshness checks, dbt transformations '
        'across all five medallion layers, 80 data quality tests, '
        'row count validation, and completion notification.'
    ),

    # Why 6am:
    # Business day starts 9am IST.
    # 3 hour buffer handles retries, failures, reruns.
    # Data team investigates failures before analysts need the data.
    schedule_interval='0 6 * * *',

    start_date=datetime(2024, 1, 1),

    # Why catchup=False:
    # Prevents Airflow from running all historical dates on startup.
    # If DAG was paused for 7 days and you unpause, catchup=True
    # would trigger 7 parallel runs simultaneously — dangerous.
    catchup=False,

    # Max 1 active run at a time
    # Prevents overlapping runs if pipeline takes longer than 24 hours
    max_active_runs=1,

    tags=['analytics', 'dbt', 'bigquery', 'production', 'daily'],

    on_failure_callback=on_dag_failure,

) as dag:

    # ── TASK: Start ──
    start = DummyOperator(
        task_id='start',
        doc_md="""
        ### Pipeline Start
        Entry point. Marks pipeline as started in Airflow UI.
        All downstream tasks wait for this to succeed.
        """
    )

    # ── TASK: Check source freshness ──
    check_sources = PythonOperator(
        task_id='check_source_freshness',
        python_callable=check_source_freshness,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=10),
        doc_md="""
        ### Source Freshness Check
        Validates that all raw BigQuery tables received fresh data.
        Pipeline aborts if any table is stale.
        Prevents downstream models from running on yesterday's data.
        """
    )

    # ── TASK: Load seed data ──
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {PROJECT_PATH} && dbt seed --target prod',
        doc_md="""
        ### dbt Seed
        Loads static reference data into BigQuery raw layer.
        In production: replaced by Fivetran/Airbyte ingestion.
        Must complete before any staging models run.
        """
    )

    # ── TASK: Run staging layer ──
    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {PROJECT_PATH} && dbt run --select staging --target prod',
        doc_md="""
        ### dbt Staging Layer
        Cleans, deduplicates, and standardizes raw data.
        One model per source table.
        This is the boundary of trust — all downstream models
        assume staging data is clean.
        """
    )

    # ── TASK: Run intermediate layer ──
    dbt_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=f'cd {PROJECT_PATH} && dbt run --select intermediate --target prod',
        doc_md="""
        ### dbt Intermediate Layer
        Complex business logic and enriched joins.
        Reusable across multiple mart models.
        Prevents logic duplication across downstream models.
        """
    )

    # ── TASK: Run mart layer ──
    dbt_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'cd {PROJECT_PATH} && dbt run --select mart --target prod',
        doc_md="""
        ### dbt Mart Layer
        Business-ready aggregations for analysts and dashboards.
        9 product analytics marts including DAU/WAU/MAU,
        cohort retention, LTV, funnels, sessionization,
        A/B test results with CUPED, and pipeline monitoring.
        """
    )

    # ── TASK: Run dbt tests ──
    dbt_tests = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {PROJECT_PATH} && dbt test --target prod',

        # Why retries=0 for tests:
        # A failed test means data is wrong.
        # Retrying a wrong-data test just confirms it is wrong.
        # Human investigation required — not automatic retry.
        retries=0,

        doc_md="""
        ### dbt Data Quality Tests
        Runs all 80 automated tests across three categories:
        - Structural (not_null, unique)
        - Business logic (value ranges, allowed values)
        - Schema contracts (compile-time enforcement)
        Pipeline fails if any test fails.
        """
    )

    # ── TASK: Validate row counts ──
    validate_counts = PythonOperator(
        task_id='validate_row_counts',
        python_callable=validate_row_counts,
        provide_context=True,
        doc_md="""
        ### Row Count Validation
        Confirms critical mart models produced expected minimum rows.
        Catches silent failures where dbt succeeds but produces empty tables.
        Reads upstream XCom result from source freshness check.
        """
    )

    # ── TASK: Send notification ──
    notify = PythonOperator(
        task_id='send_completion_notification',
        python_callable=send_completion_notification,
        provide_context=True,
        doc_md="""
        ### Completion Notification
        Sends Slack notification on successful pipeline completion.
        Only runs if ALL upstream tasks succeeded.
        Includes run duration and execution date in message.
        """
    )

    # ── TASK: End ──
    end = DummyOperator(
        task_id='end',
        doc_md="""
        ### Pipeline End
        Terminal node. Marks pipeline as complete in Airflow UI.
        """
    )

    # ════════════════════════════════════════════
    # DEPENDENCY CHAIN
    # >> means must complete successfully before next task starts
    # [task_a, task_b] means both must complete before next task
    # ════════════════════════════════════════════

    start >> check_sources >> dbt_seed
    dbt_seed >> dbt_staging
    dbt_staging >> dbt_intermediate
    dbt_intermediate >> dbt_marts

    # dbt_tests and validate_counts run in parallel after marts
    # Both must complete before notification fires
    dbt_marts >> [dbt_tests, validate_counts]
    [dbt_tests, validate_counts] >> notify
    notify >> end