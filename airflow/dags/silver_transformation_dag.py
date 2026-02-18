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
    STAGING_VIEWS
)
# ---------------------------
# Config Setting
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
    dag_id="olist_silver_transformation",
    default_args=default_args,
    description=f"Transform bronze data into silver staging views using dbt",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['silver', 'transformation', 'dbt', 'olist'],
) as dag:

    # ---------------------------
    # Run dbt staging models
    # ---------------------------
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f"""
        set -euo pipefail && \
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd {DBT_PROJECT_DIR} && \
        dbt run --select staging.* --profiles-dir . --fail-fast --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
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
    # Verify staging views
    # ---------------------------
    def verify_staging_views():
        """Verify that staging views exist and have data"""
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq.get_client()
        print(f"\nVerifying views in {GCP_PROJECT_ID}.{SILVER_DATASET}\n")
        
        for view_name in STAGING_VIEWS:
            query = f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{SILVER_DATASET}.{view_name}`"
            try:
                result = client.query(query).result()
                count = next(result).cnt
                print(f"✓ {view_name}: {count:,} rows")
                
                if count == 0:
                    raise AirflowFailException(f"View {view_name} is empty!")
                    
            except Exception as e:
                raise AirflowFailException(f"Error querying {view_name}: {str(e)}")

    verify_staging = PythonOperator(
        task_id='verify_staging_views',
        python_callable=verify_staging_views,
    )

    # ---------------------------
    # Test staging models
    # ---------------------------      
    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command=f"""
        set -euo pipefail && \
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd {DBT_PROJECT_DIR} && \
        dbt test --select staging.* --profiles-dir . --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
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
    dbt_run_staging >> verify_staging >> dbt_test_staging