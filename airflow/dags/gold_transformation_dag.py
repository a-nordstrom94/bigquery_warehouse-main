from airflow import DAG
import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException
from datetime import timedelta, datetime

from utils.config import (
    GCP_PROJECT_ID, 
    SILVER_DATASET, 
    GOLD_DATASET, 
    GCP_LOCATION, 
    GCP_CONN_ID, 
    GOLD_TABLES
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
    dag_id="olist_gold_transformation",
    default_args=default_args,
    description=f"Build gold layer analytics tables using dbt",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'transformation', 'dbt', 'olist'],
) as dag:

    # ---------------------------
    # Run intermediate + marts models
    # ---------------------------
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f"""
        set -euo pipefail && \
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd {DBT_PROJECT_DIR} && \
        dbt run --select intermediate.* marts.* --profiles-dir . --fail-fast --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
        """,
        env={
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'DBT_DATASET': SILVER_DATASET,
            'GOLD_DATASET': GOLD_DATASET,
            'GCP_LOCATION': GCP_LOCATION,
            'GOOGLE_APPLICATION_CREDENTIALS': SERVICE_ACCOUNT_KEY_PATH,
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        },
    )
    
    # ---------------------------
    # Verify gold tables
    # ---------------------------
    def verify_gold_tables():
        """Verify that gold tables exist and have data"""
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq.get_client()
        
        print(f"\nVerifying tables in {GCP_PROJECT_ID}.{GOLD_DATASET}\n")
        
        for table_name in GOLD_TABLES:
            query = f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{GOLD_DATASET}.{table_name}`"
            try:
                result = client.query(query).result()
                count = next(result).cnt
                print(f"✓ {table_name}: {count:,} rows")
                
                if count == 0:
                    raise AirflowFailException(f"Table {table_name} is empty!")
                    
            except Exception as e:
                raise AirflowFailException(f"Error querying {table_name}: {str(e)}")

    verify_gold = PythonOperator(
        task_id='verify_gold_tables',
        python_callable=verify_gold_tables,
    )
    
    # ---------------------------
    # Test marts models
    # ---------------------------      
    dbt_test_marts = BashOperator(
        task_id='dbt_test_marts',
        bash_command=f"""
        set -euo pipefail && \
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd {DBT_PROJECT_DIR} && \
        dbt test --select marts.* --profiles-dir . --fail-fast --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
        """,
        env={
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'DBT_DATASET': SILVER_DATASET,
            'GOLD_DATASET': GOLD_DATASET,
            'GCP_LOCATION': GCP_LOCATION,
            'GOOGLE_APPLICATION_CREDENTIALS': SERVICE_ACCOUNT_KEY_PATH,
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        },
    )
    
    # ---------------------------
    # Task dependencies
    # ---------------------------
    dbt_run_marts >> verify_gold >> dbt_test_marts