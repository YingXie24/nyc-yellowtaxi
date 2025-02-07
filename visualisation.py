import boto3
import json
import snowflake.connector
import pandas as pd


def get_secret(secret_name, region_name="us-east-1"):
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
    conn = snowflake.connector.connect(
        user=snowflake_credentials["user"],
        password=snowflake_credentials["password"],
        account=snowflake_credentials["account"],
        warehouse=snowflake_credentials.get("warehouse"),
        database=credentials.get("database"),
        schema=credentials.get("schema"),
    )

    return conn


def execute_query(conn, query):
    """Execute SQL query. The results are returned as a Pandas dataframe."""
    with conn:
        with conn.cursor() as cur:
            df = cur.execute(query).fetch_pandas_all()
            
    return df



# Main execution
if __name__ == "__main__":
    
    # Fetch credentials for Snowflake from AWS Secrets Manager.
    secret_name = "snowflake/credentials"
    credentials = get_secret(secret_name)

    # Create connection to Snowflake using the credentials.
    conn = connect_to_snowflake(credentials)

    # Run query and return results as pandas dataframe.
    query = "SELECT * from nyc_yellowtaxi.raw.taxi_trips limit 5"
    df = execute_query(conn, query)
    print(df)

