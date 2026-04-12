from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(hours=2),
    'on_failure_callback': lambda context: print(f"Task {context['task_instance'].task_id} failed"),
}

with DAG(
    dag_id="olist_master_pipeline",
    default_args=default_args,
    description="Master orchestrator for full Olist data pipeline: Bronze → dbt (Cosmos)",
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['master', 'orchestrator', 'olist'],
) as dag:

    trigger_bronze = TriggerDagRunOperator(
        task_id='trigger_bronze_ingestion',
        trigger_dag_id='olist_bronze_ingestion',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
    )

    trigger_dbt_pipeline = TriggerDagRunOperator(
        task_id='trigger_dbt_pipeline',
        trigger_dag_id='olist_dbt_pipeline',
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
    )

    trigger_bronze >> trigger_dbt_pipeline