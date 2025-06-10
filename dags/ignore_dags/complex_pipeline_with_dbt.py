from datetime import datetime
from airflow import DAG  
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

def send_notification(**context):
    print("DBT models completed successfully!")

with DAG(
    dag_id="complex_pipeline_with_dbt",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    
    # Pre-dbt task
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="echo 'Extracting data from source systems...'"
    )
    
    # DBT Task Group for example models
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
    
    dbt_example_models = DbtTaskGroup(
        group_id="transform_example_data",
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dbt/snowflake_demo",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["tag:example_models"]
        ),
    )
    
    # DBT Task Group for staging models
    dbt_staging_models = DbtTaskGroup(
        group_id="transform_staging_data",
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dbt/snowflake_demo",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["tag:staging_models"]
        ),
    )
    
    # Post-dbt tasks
    quality_check = BashOperator(
        task_id="run_quality_checks",
        bash_command="echo 'Running additional quality checks...'"
    )
    
    notify = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification
    )
    
    # Define dependencies
    extract_data >> dbt_example_models >> dbt_staging_models >> quality_check >> notify