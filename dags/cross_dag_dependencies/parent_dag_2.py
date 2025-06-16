"""
Parent DAG 2 - Produces asset_2
This DAG has two tasks:
1. Simple task that executes without emitting assets
2. Task that emits asset_2 using outlets
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.standard.operators.python import PythonOperator
import logging

# Define the asset that will be produced by this DAG
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

# Define the DAG
dag = DAG(
    dag_id='parent_dag_2',
    default_args=default_args,
    description='Second parent DAG that produces asset_2',
    schedule=None,
    catchup=False,
    tags=['parent', 'producer', 'assets'],
)

# First task - simple execution without asset emission
def simple_task_2():
    """First task that performs simple execution"""
    logging.info("Parent DAG 2 - Task 1: Executing simple task")
    logging.info("Performing data validation...")
    logging.info("Task 1 completed successfully")
    return "task_1_complete"

task_1 = PythonOperator(
    task_id='simple_task_2',
    python_callable=simple_task_2,
    dag=dag,
)

# Second task - executes and emits asset
def task_with_asset_emission():
    """Second task that executes and emits asset_2"""
    logging.info("Parent DAG 2 - Task 2: Executing with asset emission")
    logging.info("Transforming data...")
    logging.info(f"Emitting asset: {asset_2.name}")
    logging.info(f"Asset URI: {asset_2.uri}")
    logging.info("Task 2 completed - asset_2 will be emitted")
    return "asset_2_emitted"

task_2 = PythonOperator(
    task_id='emit_asset_2',
    python_callable=task_with_asset_emission,
    outlets=[asset_2],  # This task emits asset_2
    dag=dag,
)

# Set task dependencies
task_1 >> task_2