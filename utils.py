import boto3
import json
import snowflake.connector


def connect_to_s3():
    """Connect to AWS S3."""
    s3_client = boto3.client("s3")
    return s3_client


def get_AWS_secret(secret_name, region_name="us-east-1"):
    """Fetch credentials from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        return secret

    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None


def connect_to_snowflake(snowflake_credentials):
    "Connect to Snowflake using credentials including user, password, account, warehouse."

    try:
        conn = snowflake.connector.connect(
            user=snowflake_credentials["user"],
            password=snowflake_credentials["password"],
            account=snowflake_credentials["account"],
            warehouse=snowflake_credentials["warehouse"],
            database=snowflake_credentials["database"],
            schema=snowflake_credentials["schema"],
        )
        return conn

    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None
