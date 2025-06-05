from datetime import datetime, timedelta
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Using Airflow connection with username/password authentication
profile_config = ProfileConfig(
    profile_name="snowflake_demo",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",  # Your Airflow connection ID
        profile_args={
            # Only specify fields that are NOT in your Airflow connection
            # or fields you want to override
            "schema": "ANALYTICS",
            "threads": 1,
        },
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dbt/snowflake_demo",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
    ),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    dag_id="dbt_snowflake_cosmos_dag",
)