import logging
from utils import get_AWS_secret, connect_to_snowflake
from config import (
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    LOG_FILE,
)

# Snowflake configurations.
SNOWFLAKE_TABLE = "taxi_trips"


# Logging configurations.
logfile_path = LOG_FILE
logging.basicConfig(
    filename=logfile_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Initalise Snowflake connections
snowflake_credentials = get_AWS_secret("snowflake/credentials")
snowflake_conn = connect_to_snowflake(snowflake_credentials)


def load_file_to_Snowflake(conn):
    """Load new files from S3 to Snowflake.
    Snowflake COPY has inbuilt feature to check whether an S3 file has already
    been loaded to the table, therefore file-by-file comparison is not needed."""

    with conn.cursor() as cur:
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

        query = f"""
            COPY INTO {SNOWFLAKE_TABLE}
            FROM @s3_nyc_yellowtaxi_stage
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"""

        cur.execute(query)

        # Fetch the status message returned by Snowflake
        status_message = cur.fetchall()

        if cur.rowcount > 1:
            for row in status_message:
                print(f"{row[0]} is now loaded {row[1]} to Snowflake.")

        else:
            print(status_message[0][0])

    return


if __name__ == "__main__":
    # Load new files in S3 to Snowflake.
    load_file_to_Snowflake(snowflake_conn)

    logging.info(
        "The uploading of S3 files to Snowflake has been completed successfully."
    )
