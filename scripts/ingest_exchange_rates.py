import requests
import os  # <--- Added this to handle the credentials file
from google.cloud import bigquery
from datetime import datetime, timezone

def fetch_exchange_rates():
    url = "https://open.er-api.com/v6/latest/USD"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        ingested_at = datetime.now(timezone.utc).isoformat()
        rates = []
        for currency, rate in data['rates'].items():
            rates.append({
                'base_currency': 'USD',
                'target_currency': currency,
                'exchange_rate': float(rate),
                'rate_date': data['time_last_update_utc'],
                '_ingested_at': ingested_at,
                '_source': 'open.er-api.com'
            })
        print(f"Fetched {len(rates)} currency pairs")
        return rates
    except Exception as e:
        print(f"API Error: {e}")
        raise

def load_to_bigquery(rates):
    # --- IMPORTANT: CHANGE THIS PATH TO WHERE YOUR JSON FILE ACTUALLY IS ---
    # Example: r"C:\Users\abhay\Downloads\my-key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\analytics_project\ecommerce_dbt\analytics-warehouse-dev-major-ce50bee2f83d.json"

    project_id = "analytics-warehouse-dev-major"
    dataset_id = "dbt_dev_raw"
    table_id = "exchange_rates_raw"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    client = bigquery.Client(project=project_id)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("base_currency", "STRING"),
            bigquery.SchemaField("target_currency", "STRING"),
            bigquery.SchemaField("exchange_rate", "FLOAT64"),
            bigquery.SchemaField("rate_date", "STRING"),
            bigquery.SchemaField("_ingested_at", "STRING"),
            bigquery.SchemaField("_source", "STRING"),
        ]
    )
    
    print(f"Uploading data to BigQuery...")
    job = client.load_table_from_json(rates, table_ref, job_config=job_config)
    job.result()  # Waits for the table to be created/updated
    print(f"Successfully loaded {len(rates)} rows to {table_ref}")

def main():
    print("Starting exchange rate ingestion...")
    rates = fetch_exchange_rates()
    load_to_bigquery(rates)
    print("All done!")

if __name__ == "__main__":
    main()