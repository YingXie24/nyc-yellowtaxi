import requests
import boto3
from bs4 import BeautifulSoup
from datetime import date

# Webscraping configurations
URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# AWS S3 configurations
s3_bucket = "my-nyc-yellowtaxi"
s3_folder = date.today()

# Initialise S3 client
s3_client = boto3.client("s3")


def get_parquet_links():
    """Scrape the webpage and extract all Parquet file links."""

    response = requests.get(URL)

    if response.status_code != 200:
        print("Failed to fetch webpage")
        return

    soup = BeautifulSoup(response.content, "html.parser")

    # Find all links where the text is "Yellow Taxi Trip Records"
    links = []
    for a_tag in soup.find_all("a", string="Yellow Taxi Trip Records"):
        file_url = a_tag["href"]

        # Ensure it is a parquet file link
        if file_url.endswith(".parquet"):
            # Download only data from June 2023 as each file is quite big
            if "2023-06" in file_url:
                links.append(file_url)

    return links


def upload_file_to_s3(file_url):
    """Retrieve content from a URL and directly upload the file to S3."""

    # Parse filename from file url.
    filename = file_url.rsplit("/")[-1]

    # Retrieve content from a URL
    file_response = requests.get(file_url, stream=True)

    # Upload response to S3 bucket.
    if file_response.status_code == 200:
        s3_key = f"{s3_folder}/{filename}"
        s3_client.upload_fileobj(file_response.raw, s3_bucket, s3_key)
        print(f"Uploaded {s3_key} to S3!")

    else:
        print("Uploading to S3 has failed!")


# Main execution
if __name__ == "__main__":
    # Extract all parquet file links.
    parquet_links = get_parquet_links()

    # Download the parquet files from the links.
    for parquet_link in parquet_links:
        upload_file_to_s3(parquet_link)
