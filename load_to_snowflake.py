import boto3
from utils import list_s3_files, get_AWS_secret, connect_to_snowflake
from config import (
    S3_BUCKET,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_TABLE,
)


# Initalise S3 and Snowflake connections
s3_client = boto3.client("s3")
snowflake_credentials = get_AWS_secret("snowflake/credentials")
snowflake_conn = connect_to_snowflake(snowflake_credentials)


def get_snowflake_loaded_files(conn) -> list:
    """Query Snowflake to get the list of loaded files."""

    query = (
        f"SELECT FILE_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.LOAD_HISTORY"
    )

    with conn.cursor() as cur:
        df = cur.execute(query).fetch_pandas_all()

    files = df["FILE_NAME"].tolist()
    return files


def load_file_to_Snowflake(conn, new_files):
    """Load new files from S3 to Snowflake."""
    if not new_files:
        print("No new files to load.")

    if conn is None or conn.is_closed():
        print("Error: Snowflake connection is closed.")
        return

    with conn.cursor() as cur:
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        

        for file in new_files:
            query = f"""
                COPY INTO {SNOWFLAKE_TABLE}
                FROM @s3_nyc_yellowtaxi_stage/{file}
                FILE_FORMAT = (TYPE = 'PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"""
            
            cur.execute(query)
   

                

if __name__ == "__main__":

    # List files in S3.
    s3_files = list_s3_files()

    # List files in Snowflake.
    snowflake_files = get_snowflake_loaded_files(snowflake_conn)

    # Load only new files in S3 into Snowflake.
    new_files = []
    for s3_file in s3_files:
        s3_file_uri = f"s3://{S3_BUCKET}/{s3_file}"
        if s3_file_uri not in snowflake_files:
            new_files.append(s3_file_uri)

    load_file_to_Snowflake(snowflake_conn, new_files)

    
