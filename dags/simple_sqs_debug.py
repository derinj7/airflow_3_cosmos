"""
Simple debug DAG to check SQS and Asset status
"""
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

with DAG(
        dag_id="simple_sqs_debug",
        start_date=datetime(2024, 1, 1),
        schedule=None,  # Manual trigger
        catchup=False,
        tags=["debug", "sqs"],
        description="Debug SQS and check for messages"
) as dag:
    @task
    def check_sqs_queue():
        """Check SQS queue for messages"""
        from airflow.providers.amazon.aws.hooks.sqs import SqsHook
        import json

        print("=" * 50)
        print("🔍 Checking SQS Queue")
        print("=" * 50)

        try:
            hook = SqsHook(aws_conn_id="aws_default")
            queue_url = "https://sqs.us-east-1.amazonaws.com/833664315823/weather-data-queue"

            # Get queue attributes
            attrs = hook.get_conn().get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['All']
            )['Attributes']

            print(f"\n📊 Queue Status:")
            print(f"  Messages Available: {attrs.get('ApproximateNumberOfMessages', 0)}")
            print(f"  Messages In Flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', 0)}")

            # Try to receive a message (without deleting)
            response = hook.get_conn().receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                VisibilityTimeout=0,  # Don't hide the message
                WaitTimeSeconds=1
            )

            if 'Messages' in response:
                print(f"\n✅ Found {len(response['Messages'])} message(s)!")
                msg = response['Messages'][0]
                print(f"\n📨 Message Details:")
                print(f"  Message ID: {msg['MessageId']}")
                print(f"  Receipt Handle: {msg['ReceiptHandle'][:50]}...")

                # Parse body
                body = json.loads(msg['Body'])
                print(f"\n📄 Message Body:")
                print(json.dumps(body, indent=2))
            else:
                print("\n❌ No messages in queue")

        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            raise

        return "SQS check complete"


    @task
    def check_triggerer_instructions():
        """Provide instructions for checking triggerer"""
        print("\n" + "=" * 50)
        print("📋 Manual Checks Needed")
        print("=" * 50)

        print("\n1️⃣ Check if Asset is registered:")
        print("   - Go to Airflow UI → Browse → Assets")
        print("   - Look for 'sqs://weather-data-queue'")
        print("   - If it's there, click on it to see linked DAGs")

        print("\n2️⃣ Check Triggerer logs:")
        print("   - Run: astro deployment logs --deployment-id YOUR_ID --log-source triggerer")
        print("   - Look for:")
        print("     • 'Starting triggerer' message")
        print("     • Any SQS-related errors")
        print("     • Messages about polling")

        print("\n3️⃣ Check Scheduler logs:")
        print("   - Run: astro deployment logs --deployment-id YOUR_ID --log-source scheduler")
        print("   - Look for:")
        print("     • Asset watcher registration")
        print("     • Any asset-related errors")

        print("\n4️⃣ Verify your event-driven DAG:")
        print("   - Is 'test_sqs_event_driven' visible in the UI?")
        print("   - Any import errors shown?")
        print("   - Is it paused/unpaused?")

        return "Instructions provided"


    # Run checks
    check_sqs_queue() >> check_triggerer_instructions()