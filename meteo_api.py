import requests
from utils import upload_to_s3
from config import METEO_API_KEY
# from datetime import datetime

# Configuration for weather API
START_DATE = "2024-06-01"
END_DATE = "2024-06-30"

# Configuration for S3
S3_BUCKET_NAME = "meteo-weather"


def get_weather_data(start_date, end_date):
    """Fetch data from Meteo API"""
    url = "https://meteostat.p.rapidapi.com/point/hourly"

    headers = {
        "x-rapidapi-key": METEO_API_KEY,
        "x-rapidapi-host": "meteostat.p.rapidapi.com",
    }

    params = {
        "lat": "40.7143",
        "lon": "-74.006",
        "start": START_DATE,
        "end": END_DATE,
        "tz": "America/Toronto",
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response

    else:
        print(f"Error fetching data. The status code is {response.status_code}")
        return None


if __name__ == "__main__":
    response = get_weather_data(START_DATE, END_DATE)

    if response:
        # Generate a unique key for the S3 file based on current timestamp
        # s3_key = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        s3_key = "20240601_000000.json"

        # Upload data to S3
        upload_to_s3(S3_BUCKET_NAME, s3_key, response.content)
