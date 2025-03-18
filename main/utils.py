import boto3
import json
import snowflake.connector
import pandas as pd


def connect_to_s3():
    """Connect to AWS S3."""
    s3_client = boto3.client("s3")
    return s3_client

def upload_to_s3(bucket_name, s3_key, data):
    """Upload data to S3 bucket."""

    s3_client = connect_to_s3()

    try:
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
        print(f"Uploaded {s3_key} to S3!")

    except Exception as e:
        print(f"Error uploading file to S3: {e}")


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
    

def execute_query(conn, query):
    """Execute SQL query. The results are returned as a Pandas dataframe."""
    with conn:
        with conn.cursor() as cur:
            df = cur.execute(query).fetch_pandas_all()

    return df


def drop_rows_with_missing_values(df: pd.DataFrame, subset: list, how: str="all") -> pd.DataFrame:
    """
    Drops rows with missing values.

    Parameters:
    df (pd.DataFrame): The input dataframe.
    subset (list): Columns to consider for missing values.
    how (str): {'any', 'all'} - 'any' drops rows with any NaNs, 'all' drops rows where all subset columns are NaN.

    Returns:
    pd.DataFrame: DataFrame with dropped rows.
    """
  
    if df.empty:
        return pd.DataFrame()
    if not subset:
        return df

    return df.dropna(how=how, subset=subset)


def add_columns(df):
    """Transforms df to add the following columns:
    -'IS_AIRPORT_TRIP'
    -'TOTAL_SURCHARGE'
    - TOTAL_AMOUNT_RINGGIT
    -'PASSENGER_CATEGORY'."""

    # Custom functions
    def is_airport_trip(airport_fee):
        if airport_fee > 0:
            return True
        else:
            return False


    def calculate_total_surcharge(row):
        if row['TOTAL_AMOUNT'] > 50:
            return row['TOTAL_AMOUNT'] - row['FARE_AMOUNT'] + 2  # Extra surcharge for large amounts
        else:
            return row['TOTAL_AMOUNT'] - row['FARE_AMOUNT']

    # Apply custom functions 
    df['IS_AIRPORT_TRIP'] = df['AIRPORT_FEE'].apply(is_airport_trip)    
    df['TOTAL_SURCHARGE'] = df.apply(calculate_total_surcharge, axis=1)

    # Apply custom function via lambda
    df['TOTAL_AMOUNT_RINGGIT'] = df['TOTAL_AMOUNT'].apply(lambda x: x * 3.5)

    # Use loc for complex logic
    df.loc[(df['PASSENGER_COUNT'] ==0) & (df['TIP_AMOUNT']==0), 'PASSENGER_CATEGORY'] = "Ghost passenger"
    df.loc[(df['PASSENGER_COUNT']>4) & (df['TIP_AMOUNT']<5), 'PASSENGER_CATEGORY'] = "Deal seeker"
    df.loc[(df['PASSENGER_COUNT']==1) & (df["TIP_AMOUNT"]>5), 'PASSENGER_CATEGORY'] = "Generous solo traveller"
    df.loc[(df['PASSENGER_COUNT']==1) & (df["TIP_AMOUNT"]<=5), 'PASSENGER_CATEGORY'] = "Budget solo traveller"
    df['PASSENGER_CATEGORY'] = df['PASSENGER_CATEGORY'].fillna("Other")

    return df
    
    


