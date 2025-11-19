import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ------------------------------
# Cosmos imports
# ------------------------------
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

# ------------------------------
# DBT CONFIG
# ------------------------------
DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="citibike_dbt_gcp",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/.dbt/profiles.yml"
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path="/opt/airflow/dbt"
)

DBT_RENDER_CONFIG = RenderConfig(
    load_method=LoadMode.DBT_LS,
    select=["path:models"],  # Select all DBT models in the project
    dbt_executable_path="/usr/local/bin/dbt"
)

# ------------------------------
# DAG DEFINITION
# ------------------------------
default_args = {
    "owner": "andy",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="citibike_dbt_workflow",
    default_args=default_args,
    description="DBT transformations for the Citibike project",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "citibike"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ------------------------------
    # DBT TaskGroup
    # ------------------------------
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        render_config=DBT_RENDER_CONFIG,
    )

    # ------------------------------
    # DAG FLOW
    # ------------------------------
    start >> dbt_transform >> end

