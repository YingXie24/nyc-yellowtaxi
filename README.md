This project includes the end-to-end data pipeline for yellow taxi trips in New York City. Data for this project can be found [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). 

I attempt to approach this project in incremental stages, with the aim of each stage being a self-sufficient data asset:
1. Simple pipeline: This includes a web scraper (Extract), S3 to Snowflake loader (Load), Streamlit query script (Transform & Display). The scripts are then set up to run automatically using cron scheduling.
2. Intermediate pipeline: This includes transformation using DBT. -- Work in progress
3. Advanced pipeline: This includes orchestration using Airflow. -- Future work


To see the results of the simple data pipeline, head straight to Streamlit Community Cloud [here](https://yingxie24-nyc-yellow-taxi-visualisation-qwnwcf.streamlit.app/).

Here's a print screen of visualisation on Streamlit:
![visualisation](https://github.com/user-attachments/assets/0daacf80-1766-4b1d-a01b-e6416dd823a4)

