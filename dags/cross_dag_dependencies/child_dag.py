"""
Child DAG - Triggered by assets from both parent DAGs
This DAG is scheduled to run when both asset_1 and asset_2 are updated
It has one simple task that executes when triggered
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.standard.operators.python import PythonOperator
import logging

# Define the assets that this DAG depends on
# These must match the assets defined in the parent DAGs
asset_1 = Asset(
    name="processed_data_parent_1",
    uri="file:///tmp/airflow/parent1/processed_data.csv"
)

asset_2 = Asset(
    name="processed_data_parent_2",
    uri="file:///tmp/airflow/parent2/processed_data.csv"
)

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG with asset-based scheduling
dag = DAG(
    dag_id='child_dag',
    default_args=default_args,
    description='Child DAG triggered when both parent assets are updated',
    schedule=[asset_1, asset_2],  # Triggered when BOTH assets are updated
    catchup=False,
    tags=['child', 'consumer', 'assets'],
)

# Simple task that executes when triggered by assets
def simple_execution():
    """Simple task that runs when triggered by asset updates"""
    logging.info("Child DAG - Executing simple task")
    logging.info("This DAG was triggered by updates to both asset_1 and asset_2")
    logging.info("Processing combined data from both parent DAGs...")
    logging.info("Child task execution completed successfully")
    return "child_task_complete"

simple_task = PythonOperator(
    task_id='simple_task',
    python_callable=simple_execution,
    dag=dag,
)