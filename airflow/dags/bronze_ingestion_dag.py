from airflow import DAG
import os
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator, 
    BigQueryDeleteTableOperator, 
    BigQueryCreateEmptyDatasetOperator
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException
from airflow.decorators import task
from datetime import timedelta, datetime

# Centralized imports
from utils.config import (
    GCP_PROJECT_ID,
    GCS_BUCKET,
    BRONZE_DATASET,
    GCP_LOCATION,
    GCP_CONN_ID,
    FILE_MAPPING
)

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
    dag_id="olist_bronze_ingestion",
    default_args=default_args,
    description="Ingest raw CSV files from GCS into BigQuery bronze layer",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'ingestion', 'olist'],
) as dag:

    # Ensure dataset exists before starting
    create_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bronze_dataset',
        project_id=GCP_PROJECT_ID,
        dataset_id=BRONZE_DATASET,
        location=GCP_LOCATION,
        exists_ok=True,
        gcp_conn_id=GCP_CONN_ID
    )

    ext_tasks = {}
    bronze_tasks = {}

    # Parallel Ingestion Loop
    for file_name, table_name in FILE_MAPPING.items():
        
        # ---------------------------
        # Create _ext_ tables
        # ---------------------------
        ext_tasks[table_name] = BigQueryInsertJobOperator(
            task_id=f'create_ext_{table_name}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BRONZE_DATASET}._ext_{table_name}`
                        OPTIONS(
                            format='CSV',
                            skip_leading_rows=1,
                            field_delimiter=',',
                            quote='"',
                            allow_quoted_newlines=TRUE,
                            allow_jagged_rows=TRUE,
                            uris=['gs://{GCS_BUCKET}/raw-data/{file_name}']
                        );
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        # ---------------------------
        # Create Bronze tables
        # ---------------------------
        select_clause = "*"
        if table_name == "product_category_name_translation":
            select_clause = """
                CAST(string_field_0 AS STRING) AS product_category_name,
                CAST(string_field_1 AS STRING) AS product_category_name_english
            """

        bronze_tasks[table_name] = BigQueryInsertJobOperator(
            task_id=f'create_bronze_{table_name}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BRONZE_DATASET}.{table_name}` AS
                        SELECT {select_clause} FROM `{GCP_PROJECT_ID}.{BRONZE_DATASET}._ext_{table_name}`;
                    """,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=GCP_CONN_ID,
        )

        create_bronze_dataset >> ext_tasks[table_name] >> bronze_tasks[table_name]

    # ---------------------------
    # Verify row counts
    # ---------------------------
    @task
    def verify_row_counts():
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq.get_client()
        
        for table_name in FILE_MAPPING.values():
            ext_table = f"`{GCP_PROJECT_ID}.{BRONZE_DATASET}._ext_{table_name}`"
            bronze_table = f"`{GCP_PROJECT_ID}.{BRONZE_DATASET}.{table_name}`"
            
            ext_result = client.query(f"SELECT COUNT(*) as cnt FROM {ext_table}").result()
            bronze_result = client.query(f"SELECT COUNT(*) as cnt FROM {bronze_table}").result()
            
            ext_count = next(ext_result).cnt
            bronze_count = next(bronze_result).cnt

            print(f"Checking {table_name}: Ext({ext_count}) vs Bronze({bronze_count})")

            if ext_count != bronze_count:
                raise AirflowFailException(
                    f"Row count mismatch for {table_name}: {ext_count} (ext) != {bronze_count} (bronze)"
                )

    verify_task = verify_row_counts()
    
    # Wait for all materializations before verifying
    list(bronze_tasks.values()) >> verify_task

    # ---------------------------
    # Drop _ext_ tables
    # ---------------------------
    for table_name in FILE_MAPPING.values():
        drop_task = BigQueryDeleteTableOperator(
            task_id=f'drop_ext_{table_name}',
            deletion_dataset_table=f'{GCP_PROJECT_ID}.{BRONZE_DATASET}._ext_{table_name}',
            gcp_conn_id=GCP_CONN_ID,
            trigger_rule='all_success',
        )
        verify_task >> drop_task