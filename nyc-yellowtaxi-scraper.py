import requests
import boto3
from bs4 import BeautifulSoup

# Webscraping configurations
URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# AWS S3 configurations
s3_client = boto3.client("s3")
s3_bucket = "my-nyc-yellowtaxi"
s3_prefix = ""


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


def list_s3_files(s3_bucket):
    """List existing files in the S3 bucket to avoid uploading duplicates."""
    obj_list = []

    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

    for obj in response.get("Contents", []):
        obj_list.append(obj["Key"])

    return obj_list


def upload_file_to_s3(file_url, s3_bucket):
    """Retrieve content from a URL and directly upload the file to S3."""

    # Parse filename from file url.
    filename = file_url.rsplit("/")[-1]

    # Retrieve content from a URL
    file_response = requests.get(file_url, stream=True)

    # Upload response to S3 bucket.
    if file_response.status_code == 200:
        s3_key = f"{s3_prefix}{filename}"
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=file_response.content)
        print(f"Uploaded {s3_key} to S3!")

    else:
        print("Uploading to S3 has failed!")


# Main execution
if __name__ == "__main__":

    # Get current S3 files.
    existing_s3_files = list_s3_files(s3_bucket)

    # Get all URLs available on the website.
    urls = get_url()

    # Compare S3 and scraped files and identify new files.
    new_urls = []
    for url in urls:
        if url[-15:] >= "2024-06.parquet":
            if url.split("/")[-1] not in existing_s3_files:
                new_urls.append(url)

    # Download the parquet files from the links.
    for new_url in new_urls:
        upload_file_to_s3(new_url, s3_bucket)


