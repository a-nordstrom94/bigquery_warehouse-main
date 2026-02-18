from airflow import DAG
import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException
from datetime import timedelta, datetime

from utils.config import (
    GCP_PROJECT_ID, 
    BRONZE_DATASET,
    SILVER_DATASET, 
    GOLD_DATASET, 
    GCP_LOCATION, 
    GCP_CONN_ID
)
# ---------------------------
# Config - Load from Environment
# ---------------------------
DBT_PROJECT_DIR = os.environ['DBT_PROJECT_DIR']
SERVICE_ACCOUNT_KEY_PATH = os.environ['GOOGLE_APPLICATION_CREDENTIALS']


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 10), 
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(hours=2),
    'on_failure_callback': lambda context: print(f"Task {context['task_instance'].task_id} failed"),
}

# ---------------------------
# DAG
# ---------------------------
with DAG(
    dag_id="olist_data_quality",
    default_args=default_args,
    description="Comprehensive data quality checks across all layers",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['quality', 'testing', 'olist'],
) as dag:

    # ---------------------------
    # Run all dbt tests
    # ---------------------------
    dbt_test_all = BashOperator(
        task_id='dbt_test_all_models',
        bash_command=f"""
        set -euo pipefail && \
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd {DBT_PROJECT_DIR} && \
        dbt test --profiles-dir . --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
        """,
        env={
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'DBT_DATASET': SILVER_DATASET,
            'GOLD_DATASET': GOLD_DATASET,
            'GCP_LOCATION': GCP_LOCATION,
            'GOOGLE_APPLICATION_CREDENTIALS': SERVICE_ACCOUNT_KEY_PATH,
            'PATH': os.environ.get('PATH'),
        },
    )

    # 2. PYTHON: Row Count Consistency (Cross-Layer)
    def check_row_count_consistency():
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq.get_client()
        
        # Bronze vs Silver
        bronze_count = next(client.query(f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{BRONZE_DATASET}.orders`").result()).cnt
        silver_count = next(client.query(f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{SILVER_DATASET}.stg_olist__orders`").result()).cnt
        
        if bronze_count != silver_count:
            raise AirflowFailException(f"Row count mismatch: Bronze ({bronze_count}) != Silver ({silver_count})")
        
        # Gold vs Silver
        gold_count = next(client.query(f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{GOLD_DATASET}.fct_orders`").result()).cnt
        if gold_count > silver_count:
            raise AirflowFailException(f"Gold has more orders ({gold_count}) than Silver ({silver_count})!")

    check_consistency = PythonOperator(
        task_id='check_row_count_consistency',
        python_callable=check_row_count_consistency,
    )

    # 3. PYTHON: Business Logic Validation
    def check_business_logic():
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq.get_client()
        
        # Check: No negative revenue
        neg_val = next(client.query(f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{GOLD_DATASET}.fct_orders` WHERE total_payment_value < 0").result()).cnt
        if neg_val > 0:
            raise AirflowFailException(f"Found {neg_val} orders with negative values!")

    check_business = PythonOperator(
        task_id='check_business_logic',
        python_callable=check_business_logic,
    )

    # 4. PYTHON: Executive Quality Report
    def generate_quality_report():
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq.get_client()
        
        print("\n" + "="*70)
        print("DATA QUALITY REPORT")
        print(f"Generated at: {datetime.now()}")
        print("="*70)
        
        # Layer Summaries
        for dataset in [BRONZE_DATASET, SILVER_DATASET, GOLD_DATASET]:
            print(f"\n📊 {dataset.upper()} SUMMARY")
            query = f"SELECT table_id, row_count FROM `{GCP_PROJECT_ID}.{dataset}.__TABLES__` WHERE table_id NOT LIKE '_ext_%'"
            for row in client.query(query).result():
                print(f"  {row.table_id:30s} | {row.row_count:,} rows")

    quality_report = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
    )

    dbt_test_all >> [check_consistency, check_business] >> quality_report