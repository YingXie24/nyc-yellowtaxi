import requests
from bs4 import BeautifulSoup
import logging

from utils import connect_to_s3
from config import S3_BUCKET, S3_PREFIX, LOG_FILE


# Webscraping configurations
URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Logging configurations
logfile_path = LOG_FILE
logging.basicConfig(
    filename=logfile_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_url():
    """Scrape the webpage and extract all Parquet file links."""

    response = requests.get(URL)

    if response.status_code != 200:
        print("Failed to fetch webpage")
        return

    soup = BeautifulSoup(response.content, "html.parser")

    # Find all links where the text is "Yellow Taxi Trip Records"
    urls = []
    for a_tag in soup.find_all("a", string="Yellow Taxi Trip Records"):
        file_url = a_tag["href"]

        # Ensure it is a parquet file link
        if file_url.endswith(".parquet"):
            # Download only data from June 2023 as each file is quite big
            urls.append(file_url)

    return urls


def list_s3_files():
    """List existing files in the S3 bucket to avoid uploading duplicates."""

    # Connect to S3.
    s3_client = connect_to_s3()

    # List files in s3 bucket.
    obj_list = []
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    for obj in response.get("Contents", []):
        obj_list.append(obj["Key"])

    return obj_list


def upload_file_to_s3(file_url):
    """Retrieve content from a URL and directly upload the file to S3."""

    # Parse filename from file url.
    filename = file_url.rsplit("/")[-1]

    # Retrieve content from a URL.
    file_response = requests.get(file_url, stream=True)

    # Connect to S3.
    s3_client = connect_to_s3()

    # Upload response to S3 bucket.
    if file_response.status_code == 200:
        s3_key = f"{S3_PREFIX}{filename}"
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=file_response.content)
        print(f"Uploaded {s3_key} to S3!")

    else:
        print("Uploading to S3 has failed!")


# Main execution
if __name__ == "__main__":
    # Get current S3 files.
    existing_s3_files = list_s3_files()

    # Get all scraped URLs.
    urls = get_url()

    # Compare S3 and scraped files and identify new files.
    new_urls = []
    for url in urls:
        if url[-15:] >= "2024-06.parquet":
            if url.split("/")[-1] not in existing_s3_files:
                new_urls.append(url)

    # Upload new files to S3.
    if not new_urls:
        logging.info("There are no new files to be scraped.")

    else:
        for new_url in new_urls:
            upload_file_to_s3(new_url)
            logging.info(f"New file uploaded to S3: {new_url}")
