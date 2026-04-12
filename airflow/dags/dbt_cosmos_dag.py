import os
from pathlib import Path
from datetime import datetime, timedelta

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode

DBT_PROJECT_DIR = Path(os.environ["DBT_PROJECT_DIR"])

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    manifest_path=DBT_PROJECT_DIR / "target" / "manifest.json",
)

profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=Path("/home/airflow/.local/bin/dbt"),
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,
    select=["path:models", "path:snapshots"],
)

dag = DbtDag(
    dag_id="olist_dbt_pipeline",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    schedule_interval=None,
    start_date=datetime(2025, 10, 10),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": lambda context: print(
            f"Task {context['task_instance'].task_id} failed"
        ),
    },
    tags=["dbt", "cosmos", "olist"],
)
