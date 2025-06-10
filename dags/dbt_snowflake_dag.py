from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig, LoadMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Using Airflow connection with username/password authentication
profile_config = ProfileConfig(
   profile_name="snowflake_demo",
   target_name="dev",
   profile_mapping=SnowflakeUserPasswordProfileMapping(
       conn_id="snowflake_default",
       profile_args={
           "schema": "ANALYTICS",
           "threads": 4,
       },
   )
)

# Use the manifest.json generated in CI/CD
render_config = RenderConfig(
   load_method=LoadMode.DBT_MANIFEST,
   manifest_path="/usr/local/airflow/dbt/snowflake_demo/target/manifest.json",
)

dbt_snowflake_dag = DbtDag(
   project_config=ProjectConfig(
       dbt_project_path="/usr/local/airflow/dbt/snowflake_demo",
   ),
   profile_config=profile_config,
   render_config=render_config,
   schedule="@daily",
   start_date=datetime(2024, 1, 1),
   dag_id="dbt_snowflake_cosmos_dag",
)