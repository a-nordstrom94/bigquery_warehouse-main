from airflow import DAG
import os  # Standard import
from airflow.utils import timezone
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException
from datetime import timedelta

# ---------------------------
# Config
# ---------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-default-project")
# Changed this to point to silver; ensure your dbt project uses this dataset name
SILVER_DATASET = "olist_silver" 
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
    dag_id="olist_silver_transformation",
    default_args=default_args,
    start_date=timezone.utcnow() - timedelta(days=1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # ---------------------------
    # Run dbt models
    # ---------------------------
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd /usr/app/dbt_project && \
        dbt run --select silver --profiles-dir . --project-dir . --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
        """,
        env={
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'GOOGLE_APPLICATION_CREDENTIALS': '/usr/app/keys/service_account.json',
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        },
    )

    # ---------------------------
    # Verify silver tables were created
    # ---------------------------
    def verify_silver_tables():
        """Verify that silver tables exist and have data"""
        bq = BigQueryHook(gcp_conn_id='google_cloud_default')
        client = bq.get_client()
        
        expected_tables = [
            'stg_customers', 'stg_orders', 'stg_order_items',
            'stg_order_payments', 'stg_reviews', 'stg_products',
            'stg_sellers', 'stg_geolocations', 'stg_product_category_name_translation'
        ]
        
        for table_name in expected_tables:
            query = f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{SILVER_DATASET}.{table_name}`"
            result = client.query(query).result()
            count = next(result).cnt
            
            if count == 0:
                raise AirflowFailException(
                    f"Silver table {table_name} is empty!"
                )
            print(f"✓ {table_name}: {count} rows")

    verify_silver = PythonOperator(
        task_id='verify_silver_tables',
        python_callable=verify_silver_tables,
    )

    # ---------------------------
    # Test silver models
    # ---------------------------      
    dbt_test_silver = BashOperator(
        task_id='dbt_test_silver',
        bash_command="""
        export PATH=$PATH:/home/airflow/.local/bin && \
        cd /usr/app/dbt_project && \
        dbt test --select silver --profiles-dir . --project-dir . --target-path /tmp/dbt_target --log-path /tmp/dbt_logs
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
    dbt_run_silver >> dbt_test_silver >> verify_silver