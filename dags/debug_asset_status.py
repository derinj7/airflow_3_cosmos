"""
Debug DAG to check if assets are registered and triggerer is running
"""
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import AssetModel
from airflow.utils.db import provide_session

with DAG(
        dag_id="debug_asset_status",
        start_date=datetime(2024, 1, 1),
        schedule=None,  # Manual trigger
        catchup=False,
        tags=["debug", "asset"],
        description="Check asset registration and status"
) as dag:
    @task
    @provide_session
    def check_assets(session=None):
        """Check if assets are registered in the database"""
        print("=" * 50)
        print("🔍 Checking Asset Registration")
        print("=" * 50)

        # Query all assets
        assets = session.query(AssetModel).all()

        if assets:
            print(f"\n✅ Found {len(assets)} assets:")
            for asset in assets:
                print(f"\n  Asset URI: {asset.uri}")
                print(f"  Asset ID: {asset.id}")
                print(f"  Created: {asset.created_at}")
                print(f"  Updated: {asset.updated_at}")

                # Check if it's our SQS asset
                if "sqs" in asset.uri.lower():
                    print("  ⭐ This is an SQS asset!")
        else:
            print("\n❌ No assets found in the database!")

        return f"Found {len(assets)} assets"


    @task
    def check_sqs_messages():
        """Check if there are messages in the queue"""
        from airflow.providers.amazon.aws.hooks.sqs import SqsHook

        print("\n" + "=" * 50)
        print("📬 Checking SQS Queue Status")
        print("=" * 50)

        try:
            hook = SqsHook(aws_conn_id="aws_default")
            queue_url = "https://sqs.us-east-1.amazonaws.com/833664315823/weather-data-queue"

            # Get queue attributes
            response = hook.get_conn().get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )

            attrs = response['Attributes']
            print(f"\n📊 Queue Statistics:")
            print(f"  Messages Available: {attrs.get('ApproximateNumberOfMessages', 0)}")
            print(f"  Messages In Flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', 0)}")
            print(f"  Messages Delayed: {attrs.get('ApproximateNumberOfMessagesDelayed', 0)}")

            # Try to peek at a message without deleting
            messages = hook.get_conn().receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                VisibilityTimeout=0,  # Don't hide the message
                WaitTimeSeconds=1
            )

            if 'Messages' in messages:
                print(f"\n📨 Sample Message:")
                msg = messages['Messages'][0]
                print(f"  Message ID: {msg['MessageId']}")
                print(f"  Body preview: {msg['Body'][:200]}...")
            else:
                print("\n📭 No messages currently in queue")

        except Exception as e:
            print(f"\n❌ Error checking queue: {str(e)}")

        return "Queue check complete"


    @task
    def check_triggerer_status():
        """Check if triggerer is running"""
        print("\n" + "=" * 50)
        print("⚙️ Triggerer Status Check")
        print("=" * 50)

        print("\n📌 To check triggerer status, run:")
        print("  astro deployment logs --deployment-id YOUR_ID --log-source triggerer --follow")

        print("\n📌 Look for these in triggerer logs:")
        print("  - 'Starting triggerer' or similar startup messages")
        print("  - Any errors related to SQS or AssetWatcher")
        print("  - Messages about polling the queue")

        print("\n📌 Also check if your DAG appears in:")
        print("  - Airflow UI → Browse → Assets")
        print("  - Your SQS asset should be listed there")

        return "Check instructions provided"


    # Run all checks
    check_assets() >> check_sqs_messages() >> check_triggerer_status()