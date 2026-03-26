from prefect import flow, task
import subprocess

@task
def run_dbt_models(): 
     
  print("Running dbt models...")

  subprocess.run(["dbt", "run"], check=True)

@task
def run_dbt_tests():
  print("Running dbt tests...")
  subprocess.run(["dbt", "test"], check=True)

@flow
def analytics_pipeline():
 run_dbt_models()
 run_dbt_tests()

if __name__ == "__main__":
  
    analytics_pipeline()