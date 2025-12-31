from airflow import DAG
import os
from airflow.utils import timezone
from airflow.operators.bash import BashOperator
from datetime import timedelta

# ---------------------------
# Config
# ---------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-default-project")
DBT_PROJECT_DIR = "/usr/app/dbt_project"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ---------------------------
# DAG
# ---------------------------
with DAG(
    dag_id="olist_gold_transformation",
    default_args=default_args,
    start_date=timezone.utcnow() - timedelta(days=1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # ---------------------------
    # Run all gold+ models in correct dependency order
    # ---------------------------
    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=f"""
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd {DBT_PROJECT_DIR} && \
        dbt run --select gold+ --profiles-dir . --project-dir . --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
        """,
        env={
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'GOOGLE_APPLICATION_CREDENTIALS': '/usr/app/keys/service_account.json',
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        },
    )
    
    # ---------------------------
    # Test gold models
    # ---------------------------      
    dbt_test_gold = BashOperator(
        task_id='dbt_test_gold',
        bash_command=f"""
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd {DBT_PROJECT_DIR} && \
        dbt test --select gold+ --profiles-dir . --project-dir . --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
        """,
        env={
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'GOOGLE_APPLICATION_CREDENTIALS': '/usr/app/keys/service_account.json',
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        },
    )
    
    # ---------------------------
    # Task dependencies
    # ---------------------------
    dbt_run_gold >> dbt_test_gold