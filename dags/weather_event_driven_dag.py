from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task
import os
from pendulum import datetime

# Your SQS queue URL
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/833664315823/weather-data-queue"

# Define the trigger with correct parameters
trigger = MessageQueueTrigger(
    aws_conn_id="aws_default",
    queue=SQS_QUEUE_URL,
    waiter_delay=10  # Check every 10 seconds
)

# Define the asset with watcher
sqs_asset = Asset(
    "weather_sqs_asset",
    watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)]
)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=[sqs_asset],
    tags=["event-driven", "sqs", "weather"],
    catchup=False,
    max_active_runs=1
)
def weather_event_driven_dag():
    @task
    def process_sqs_message(**context):
        """Process the SQS message that triggered this DAG"""
        print("🎉 DAG was triggered by SQS message!")
        print("=" * 50)

        # Extract the triggering asset events from the context
        triggering_asset_events = context["triggering_asset_events"]

        for event in triggering_asset_events[sqs_asset]:
            # Get the message from the TriggerEvent payload
            message_body = event.payload.get("body")
            message_id = event.payload.get("message_id")

            print(f"📨 Message ID: {message_id}")
            print(f"📄 Message Body: {message_body}")

            # Process your message here
            # Add your business logic

            return {
                "processed_at": datetime.now().isoformat(),
                "message_id": message_id,
                "message_body": message_body,
                "status": "processed"
            }

    @task
    def transform_data(message_data):
        """Transform the processed message data"""
        if message_data:
            print("🔄 Transforming weather data...")
            transformed = {
                **message_data,
                "transformed": True,
                "transformation_time": datetime.now().isoformat()
            }
            print(f"✅ Transformation complete")
            return transformed
        return None

    @task
    def load_data(transformed_data):
        """Load the transformed data"""
        if transformed_data:
            print("💾 Loading data to destination...")
            print(f"Final data: {transformed_data}")
            print("✅ Data loaded successfully")
        return "Pipeline complete"

    # Define the task flow
    message_data = process_sqs_message()
    transformed = transform_data(message_data)
    load_data(transformed)


# Instantiate the DAG
weather_event_driven_dag()