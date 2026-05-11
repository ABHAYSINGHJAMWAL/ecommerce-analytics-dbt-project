from prefect import flow, task
import subprocess
import datetime
import sys

def log(message, level="INFO"):
    timestamp = datetime.datetime.utcnow().isoformat()
    print(f"[{timestamp}] [{level}] {message}")

@task
def run_dbt_models():
    try:
        log("Starting dbt run")
        result = subprocess.run(
            ["dbt", "run"],
            check=True,
            capture_output=True,
            text=True
        )
        log("dbt run completed successfully")
        return result.stdout

    except subprocess.CalledProcessError as e:
        log(f"dbt run failed: {e.stderr}", level="ERROR")
        raise

@task
def run_dbt_tests():
    try:
        log("Starting dbt test")
        result = subprocess.run(
            ["dbt", "test"],
            check=True,
            capture_output=True,
            text=True
        )
        log("dbt test completed successfully")
        return result.stdout

    except subprocess.CalledProcessError as e:
        log(f"dbt test failed: {e.stderr}", level="ERROR")
        raise

@task
def run_monitoring():
    try:
        log("Running monitoring mart")
        result = subprocess.run(
            ["dbt", "run", "--select", "mart_pipeline_monitoring"],
            check=True,
            capture_output=True,
            text=True
        )
        log("Monitoring mart updated successfully")
        return result.stdout

    except subprocess.CalledProcessError as e:
        log(f"Monitoring mart failed: {e.stderr}", level="ERROR")
        raise

@flow
def analytics_pipeline():
    log("Pipeline started")

    try:
        run_dbt_models()
        run_dbt_tests()
        run_monitoring()

        log("Pipeline completed successfully ✅")

    except Exception as e:
        log(f"Pipeline failed: {str(e)}", level="CRITICAL")

        # 🔥 Production hook (future)
        # send_slack_alert(str(e))

        sys.exit(1)

if __name__ == "__main__":
    analytics_pipeline()