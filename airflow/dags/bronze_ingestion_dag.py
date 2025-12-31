from airflow import DAG
from airflow import os
from airflow.utils import timezone
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryDeleteTableOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException
from airflow.decorators import task
from datetime import timedelta

# ---------------------------
# Config
# ---------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-default-project")
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-default-bucket")
BRONZE_DATASET = os.getenv("BRONZE_DATASET", "olist_bronze")
GCP_CONN_ID = "google_cloud_default"

FILE_MAPPING = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocations",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_name_translation"
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# ---------------------------
# DAG
# ---------------------------
with DAG(
    dag_id="olist_bronze_ingestion",
    default_args=default_args,
    start_date=timezone.utcnow() - timedelta(days=1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    ext_tasks = {}
    bronze_tasks = {}

    # ---------------------------
    # Create _ext_ tables and Bronze tables
    # ---------------------------
    for file_name, table_name in FILE_MAPPING.items():
        # External table pointing to GCS
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

        # Bronze table (Materializing the data)
        if table_name == "product_category_name_translation":
            bronze_tasks[table_name] = BigQueryInsertJobOperator(
                task_id=f'create_bronze_{table_name}',
                configuration={
                    "query": {
                        "query": f"""
                            CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BRONZE_DATASET}.{table_name}` AS
                            SELECT 
                                CAST(string_field_0 AS STRING) AS product_category_name,
                                CAST(string_field_1 AS STRING) AS product_category_name_english
                            FROM `{GCP_PROJECT_ID}.{BRONZE_DATASET}._ext_{table_name}`;
                        """,
                        "useLegacySql": False,
                    }
                },
                gcp_conn_id=GCP_CONN_ID,
            )
        else:
            bronze_tasks[table_name] = BigQueryInsertJobOperator(
                task_id=f'create_bronze_{table_name}',
                configuration={
                    "query": {
                        "query": f"""
                            CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BRONZE_DATASET}.{table_name}` AS
                            SELECT * FROM `{GCP_PROJECT_ID}.{BRONZE_DATASET}._ext_{table_name}`;
                        """,
                        "useLegacySql": False,
                    }
                },
                gcp_conn_id=GCP_CONN_ID,
            )

        ext_tasks[table_name] >> bronze_tasks[table_name]

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
            
            ext_count = list(ext_result)[0]['cnt']
            bronze_count = list(bronze_result)[0]['cnt']

            if ext_count != bronze_count:
                raise AirflowFailException(
                    f"Row count mismatch for {table_name}: {ext_count} (ext) != {bronze_count} (bronze)"
                )

    verify_task = verify_row_counts()
    list(bronze_tasks.values()) >> verify_task

    # ---------------------------
    # Drop _ext_ tables
    # ---------------------------
    drop_tasks = {}
    for table_name in FILE_MAPPING.values():
        drop_tasks[table_name] = BigQueryDeleteTableOperator(
            task_id=f'drop_ext_{table_name}',
            deletion_dataset_table=f'{GCP_PROJECT_ID}.{BRONZE_DATASET}._ext_{table_name}',
            gcp_conn_id=GCP_CONN_ID,
            trigger_rule='all_success',
        )

    verify_task >> list(drop_tasks.values())