"""
Simple Event-Driven DAG for Testing SQS Integration
This DAG just logs everything to verify the event-driven mechanism works
"""
from datetime import datetime
from airflow.sdk import DAG, Asset, AssetWatcher, task
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
import json

# SQS Queue URL
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/833664315823/weather-data-queue"

# Create the SQS trigger
sqs_trigger = MessageQueueTrigger(
    queue=SQS_QUEUE_URL,
    aws_conn_id="aws_default",
    waiter_delay=10
)

# Create the asset with watcher
sqs_asset = Asset(
    uri="sqs://weather-data-queue",
    watchers=[
        AssetWatcher(
            name="sqs_watcher",
            trigger=sqs_trigger
        )
    ]
)

# Create the DAG
with DAG(
        dag_id="test_sqs_event_driven",
        start_date=datetime(2024, 1, 1),
        schedule=[sqs_asset],  # Triggered by SQS messages
        catchup=False,
        tags=["test", "sqs", "event-driven"],
        description="Simple test DAG for SQS event-driven scheduling"
) as dag:
    @task
    def log_sqs_event(**context):
        """Just log everything we receive"""
        print("=" * 50)
        print("🎉 DAG TRIGGERED BY SQS MESSAGE!")
        print("=" * 50)

        # Log the entire context
        print("\n📋 Full Context:")
        print(json.dumps(str(context), indent=2))

        # Get triggering events
        triggering_asset_events = context.get("triggering_asset_events", {})

        if triggering_asset_events:
            print("\n📨 Triggering Asset Events:")
            for asset_uri, events in triggering_asset_events.items():
                print(f"\n  Asset: {asset_uri}")
                print(f"  Number of events: {len(events)}")

                for i, event in enumerate(events):
                    print(f"\n  Event {i + 1}:")
                    print(f"    Type: {type(event)}")
                    print(f"    Event: {event}")

                    # Try to extract message content
                    if hasattr(event, 'extra'):
                        print(f"    Extra: {event.extra}")
                        try:
                            if isinstance(event.extra, str):
                                parsed = json.loads(event.extra)
                                print(f"    Parsed Extra: {json.dumps(parsed, indent=4)}")
                        except:
                            pass
        else:
            print("\n❌ No triggering asset events found!")

        print("\n" + "=" * 50)
        return "Event logged successfully!"


    # Run the task
    log_sqs_event()