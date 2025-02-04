import os
import requests
from bs4 import BeautifulSoup

URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
download_folder = "yellow_taxi_parquet_files"
os.makedirs(download_folder, exist_ok=True)


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


def download_file(file_url):
    """Download a file from webpage and save the file locally.
        The filename is parsed from the last part of the file url."""

    # Parse filename from file url.
    filename = file_url.rsplit("/")[-1]
    full_path = os.path.join(download_folder, filename)

    # Download file using wget. 
    os.system(f"wget {file_url} -O {full_path}")
    print(f"Downloaded {filename} in the {download_folder} folder.")


# Main execution
if __name__ == "__main__":

    # Extract all parquet file links. 
    parquet_links = get_parquet_links()

    # Download the parquet files from the links.
    for parquet_link in parquet_links:
        download_file(parquet_link)
