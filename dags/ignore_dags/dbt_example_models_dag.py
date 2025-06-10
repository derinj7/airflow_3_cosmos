from datetime import datetime, timedelta
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# DAG for example models only
profile_config = ProfileConfig(
    profile_name="snowflake_demo",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "schema": "ANALYTICS",
            "threads": 1,
        },
    )
)

dbt_example_models_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/snowflake_demo",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
    ),
    render_config=RenderConfig(
        select=["tag:example_models"]  # Select only models with example_models tag
    ),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    dag_id="dbt_example_models_dag",
    description="Run only example dbt models",
    tags=["dbt", "example"],
)