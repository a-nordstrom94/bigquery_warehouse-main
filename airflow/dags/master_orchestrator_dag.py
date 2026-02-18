from airflow import DAG
import os
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import timedelta, datetime

from utils.config import (
    GCP_PROJECT_ID, 
    BRONZE_DATASET,
    SILVER_DATASET, 
    GOLD_DATASET, 
    GCP_CONN_ID,
    FILE_MAPPING,
    STAGING_VIEWS,
    GOLD_TABLES
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
    dag_id="olist_master_pipeline",
    default_args=default_args,
    description="Master orchestrator for full Olist data pipeline: Bronze → Silver → Gold",
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['master', 'orchestrator', 'olist'],
) as dag:

    # ---------------------------
    # Trigger Bronze Ingestion (FIRST - no preflight)
    # ---------------------------
    trigger_bronze = TriggerDagRunOperator(
        task_id='trigger_bronze_ingestion',
        trigger_dag_id='olist_bronze_ingestion',
        wait_for_completion=True,
        poke_interval=10,              # Check every 10 seconds
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,            # Reset if already running
    )

    # ---------------------------
    # Verify Bronze Data (AFTER bronze completes)
    # ---------------------------
    def verify_bronze_data():
        """Verify bronze tables were created successfully"""
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq.get_client()
        
        required_tables = list(FILE_MAPPING.values())
        
        print(f"=== BRONZE DATA VERIFICATION ===")
        for table in required_tables:
            query = f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{BRONZE_DATASET}.{table}`"
            count = next(client.query(query).result()).cnt
            if count == 0:
                raise ValueError(f"Bronze table {table} is empty!")
            print(f"✓ {table}: {count:,} rows")

    verify_bronze = PythonOperator(
        task_id='verify_bronze_data',
        python_callable=verify_bronze_data,
    )

    # ---------------------------
    # Trigger Silver Transformation
    # ---------------------------
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_transformation',
        trigger_dag_id='olist_silver_transformation',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
    )

    # ---------------------------
    # Trigger Gold Transformation
    # ---------------------------
    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_transformation',
        trigger_dag_id='olist_gold_transformation',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
    )

    # ---------------------------
    # Post-pipeline validation
    # ---------------------------
    def validate_pipeline():
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = bq.get_client()
        
        print("\nPIPELINE VALIDATION SUMMARY")
        # Validation for Silver Views
        for view in STAGING_VIEWS:
            count = next(client.query(f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{SILVER_DATASET}.{view}`").result()).cnt
            print(f"Silver View: {view:<40} | {count:>10,} rows")
        
        # Check Gold (Tables)
        print(f"\nGold Layer ({GOLD_DATASET}):")
        print(f"{'Table Name':<45} {'Row Count':>20}")
        print("-" * 70)
        
        gold_tables = [
            'dim_customers', 'dim_products', 'dim_sellers',
            'fct_orders', 'fct_order_items',
            'customer_metrics', 'product_performance', 'seller_performance'
        ]
        
        for table in GOLD_TABLES:
            count = next(client.query(f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT_ID}.{GOLD_DATASET}.{table}`").result()).cnt
            print(f"Gold Table:  {table:<40} | {count:>10,} rows")
        
        print("\n" + "="*70)
        print("✅ Pipeline validation complete!")
        print("="*70 + "\n")

    validate = PythonOperator(
        task_id='validate_pipeline',
        python_callable=validate_pipeline,
    )

    # ---------------------------
    # Trigger Data Quality DAG
    # ---------------------------
    trigger_quality = TriggerDagRunOperator(
        task_id='trigger_data_quality_checks',
        trigger_dag_id='olist_data_quality',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
    )

    # ---------------------------
    # Pipeline success notification
    # ---------------------------
    def send_success_notification():
        """Log pipeline success (extend with Slack/email later)"""
        print("\n" + "="*70)
        print("🎉 OLIST DATA PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*70)
        print(f"Project: {GCP_PROJECT_ID}")
        print(f"Bronze Dataset: {BRONZE_DATASET}")
        print(f"Silver Dataset: {SILVER_DATASET}")
        print(f"Gold Dataset: {GOLD_DATASET}")
        print("="*70 + "\n")

    success_notification = PythonOperator(
        task_id='success_notification',
        python_callable=send_success_notification,
    )

    trigger_bronze >> verify_bronze >> trigger_silver >> trigger_gold >> validate >> trigger_quality >> success_notification