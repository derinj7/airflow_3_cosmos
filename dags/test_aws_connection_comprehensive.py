"""
Comprehensive test of AWS connection and SQS access
"""
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import json

with DAG(
        dag_id="test_aws_connection_comprehensive",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["test", "aws", "connection"],
        description="Test AWS connection and SQS access comprehensively"
) as dag:
    @task
    def test_connection_details():
        """Test and display AWS connection details"""
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        print("=" * 60)
        print("🔍 Testing AWS Connection: aws_default")
        print("=" * 60)

        try:
            # Get the connection
            hook = AwsBaseHook(aws_conn_id="aws_default", client_type="sts")

            # Get connection details (without exposing secrets)
            conn = hook.get_conn()

            print("\n✅ AWS connection established successfully!")

            # Test STS get caller identity
            try:
                identity = conn.get_caller_identity()
                print("\n📋 AWS Identity Information:")
                print(f"  Account: {identity['Account']}")
                print(f"  Arn: {identity['Arn']}")
                print(f"  UserId: {identity['UserId']}")
            except Exception as e:
                print(f"\n❌ Error getting caller identity: {str(e)}")

        except Exception as e:
            print(f"\n❌ Error with AWS connection: {str(e)}")
            raise

        return "Connection test complete"


    @task(trigger_rule="all_done")
    def test_sqs_access():
        """Test SQS access specifically"""
        from airflow.providers.amazon.aws.hooks.sqs import SqsHook

        print("\n" + "=" * 60)
        print("🔍 Testing SQS Access")
        print("=" * 60)

        try:
            hook = SqsHook(aws_conn_id="aws_default")
            client = hook.get_conn()

            # Test listing queues
            print("\n📋 Testing SQS Permissions:")

            try:
                response = client.list_queues()
                queue_count = len(response.get('QueueUrls', []))
                print(f"  ✅ Can list queues: Found {queue_count} queues")
            except Exception as e:
                print(f"  ❌ Cannot list queues: {str(e)}")

            # Test specific queue access
            queue_url = "https://sqs.us-east-1.amazonaws.com/833664315823/weather-data-queue"

            # Get queue attributes
            try:
                attrs = client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=['All']
                )['Attributes']

                print(f"\n✅ Can access queue: {queue_url}")
                print(f"  Messages Available: {attrs.get('ApproximateNumberOfMessages', 0)}")
                print(f"  Messages In Flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', 0)}")
                print(f"  Created: {attrs.get('CreatedTimestamp', 'N/A')}")

            except Exception as e:
                print(f"\n❌ Cannot access queue attributes: {str(e)}")

            # Test receive message permission
            try:
                response = client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    VisibilityTimeout=0,  # Don't hide the message
                    WaitTimeSeconds=1
                )

                if 'Messages' in response:
                    print(f"\n✅ Can receive messages from queue")
                    print(f"  Found {len(response['Messages'])} message(s)")
                else:
                    print(f"\n⚠️  Can receive messages but queue is empty")

            except Exception as e:
                print(f"\n❌ Cannot receive messages: {str(e)}")

        except Exception as e:
            print(f"\n❌ Error testing SQS: {str(e)}")
            raise

        return "SQS test complete"


    @task
    def test_connection_configuration():
        """Check how the connection is configured"""
        from airflow.hooks.base import BaseHook
        import os

        print("\n" + "=" * 60)
        print("🔍 Connection Configuration Check")
        print("=" * 60)

        try:
            # Get connection object
            conn = BaseHook.get_connection("aws_default")

            print("\n📋 Connection Details:")
            print(f"  Connection ID: {conn.conn_id}")
            print(f"  Connection Type: {conn.conn_type}")
            print(f"  Host: {conn.host or 'Not set'}")
            print(f"  Login (Access Key): {'Set' if conn.login else 'Not set (using IAM role)'}")
            print(f"  Password (Secret): {'Set' if conn.password else 'Not set (using IAM role)'}")
            print(f"  Schema: {conn.schema or 'Not set'}")
            print(f"  Port: {conn.port or 'Not set'}")

            # Check extra field
            if conn.extra:
                try:
                    extra = json.loads(conn.extra)
                    print(f"\n📋 Extra Configuration:")
                    for key, value in extra.items():
                        if key in ['aws_access_key_id', 'aws_secret_access_key', 'aws_session_token']:
                            print(f"  {key}: {'***' if value else 'Not set'}")
                        else:
                            print(f"  {key}: {value}")
                except:
                    print(f"  Extra (raw): {conn.extra}")

            # Check environment variables
            print(f"\n📋 Relevant Environment Variables:")
            env_vars = [
                'AWS_DEFAULT_REGION',
                'AWS_REGION',
                'AWS_ROLE_ARN',
                'AWS_WEB_IDENTITY_TOKEN_FILE',
                'AWS_ACCESS_KEY_ID',
                'AWS_SECRET_ACCESS_KEY'
            ]

            for var in env_vars:
                value = os.environ.get(var)
                if value:
                    if 'SECRET' in var or 'KEY' in var:
                        print(f"  {var}: ***")
                    else:
                        print(f"  {var}: {value}")
                else:
                    print(f"  {var}: Not set")

        except Exception as e:
            print(f"\n❌ Error checking connection config: {str(e)}")

        return "Configuration check complete"


    @task
    def test_boto3_session():
        """Test boto3 session creation"""
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        print("\n" + "=" * 60)
        print("🔍 Testing Boto3 Session Creation")
        print("=" * 60)

        try:
            hook = AwsBaseHook(aws_conn_id="aws_default")
            session = hook.get_session()

            print("\n✅ Boto3 session created successfully")

            # Get credentials info (without exposing secrets)
            creds = session.get_credentials()
            if creds:
                print("\n📋 Credentials Info:")
                print(f"  Access Key: {'***' + creds.access_key[-4:] if creds.access_key else 'Not set'}")
                print(f"  Token: {'Present' if creds.token else 'Not present'}")
                print(f"  Method: {creds.method if hasattr(creds, 'method') else 'Unknown'}")

            # Check region
            print(f"\n📋 Session Configuration:")
            print(f"  Region: {session.region_name}")

        except Exception as e:
            print(f"\n❌ Error creating boto3 session: {str(e)}")
            raise

        return "Boto3 session test complete"


    # Run all tests in sequence
    conn_test = test_connection_details()
    config_test = test_connection_configuration()
    boto_test = test_boto3_session()
    sqs_test = test_sqs_access()

    conn_test >> config_test >> boto_test >> sqs_test