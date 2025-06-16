"""
Parent DAG 1 - Produces asset_1
This DAG has two tasks:
1. Simple task that executes without emitting assets
2. Task that emits asset_1 using outlets
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.standard.operators.python import PythonOperator
import logging


# Define the asset that will be produced by this DAG
asset_1 = Asset(
    name="processed_data_parent_1",
    uri="file:///tmp/airflow/parent1/processed_data.csv"
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
    dag_id='parent_dag_1',
    default_args=default_args,
    description='First parent DAG that produces asset_1',
    schedule=None,
    catchup=False,
    tags=['parent', 'producer', 'assets'],
)

# First task - simple execution without asset emission
def simple_task_1():
    """First task that performs simple execution"""
    logging.info("Parent DAG 1 - Task 1: Executing simple task")
    logging.info("Performing some data extraction...")
    logging.info("Task 1 completed successfully")
    return "task_1_complete"

task_1 = PythonOperator(
    task_id='simple_task_1',
    python_callable=simple_task_1,
    dag=dag,
)

# Second task - executes and emits asset
def task_with_asset_emission():
    """Second task that executes and emits asset_1"""
    logging.info("Parent DAG 1 - Task 2: Executing with asset emission")
    logging.info("Processing data...")
    logging.info(f"Emitting asset: {asset_1.name}")
    logging.info(f"Asset URI: {asset_1.uri}")
    logging.info("Task 2 completed - asset_1 will be emitted")
    return "asset_1_emitted"

task_2 = PythonOperator(
    task_id='emit_asset_1',
    python_callable=task_with_asset_emission,
    outlets=[asset_1],  # This task emits asset_1
    dag=dag,
)

# Set task dependencies
task_1 >> task_2