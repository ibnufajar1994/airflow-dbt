from datetime import datetime
from airflow.datasets import Dataset
from helper.callbacks.slack_notifier import slack_notifier

from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
from cosmos import DbtDag

import os

# Define path to the dbt project directory
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/flights_warehouse_pipeline/pacflight_dbt"

# Configure dbt project settings
project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="flights_warehouse_pipeline"
)

# Configure dbt profile with Postgres credentials and target schema
profile_config = ProfileConfig(
    profile_name="warehouse",
    target_name="warehouse",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id='warehouse_pacflight',  # Airflow connection for Postgres
        profile_args={"schema": "final"}  # load into final schema
    )
)

# Instantiate the dbt DAG with rendering and failure callback settings
dag = DbtDag(
    dag_id="flights_warehouse_pipeline",
    schedule=None,                # no schedule; triggered manually or by upstream DAG
    catchup=False,                # skip past runs
    start_date=datetime(2024, 1, 1),  # DAG start date
    project_config=project_config,
    profile_config=profile_config,
    render_config=RenderConfig(
        dbt_executable_path="/opt/airflow/dbt_venv/bin",  # path to dbt CLI
        emit_datasets=True            # enable Airflow Datasets feature
    ),
    default_args={
        'on_failure_callback': slack_notifier  # notify via Slack on failure
    }
)
