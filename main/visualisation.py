
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.io as pio
import numpy as np

from utils import connect_to_snowflake, execute_query


def get_streamlit_secret(secret_name="snowflake"):
    """Fetch Snowflake credentials from Streamlit Secrets."""

    credentials = st.secrets[secret_name]

    return {
        "user": credentials["user"],
        "password": credentials["password"],
        "account": credentials["account"],
        "warehouse": credentials["warehouse"],
        "database": credentials["database"],
        "schema": credentials["schema"],
    }


# Main execution
if __name__ == "__main__":
    # Access Snowflake credentials stored in Streamlit Secrets.
    snowflake_credentials = get_streamlit_secret()

    # Create a Snowflake connection.
    conn = connect_to_snowflake(snowflake_credentials)

    # Run query and return results as pandas dataframe.
    query = "SELECT * from nyc_yellowtaxi.dev.stg_tlc__trips where pickup_at between '2024-06-01' and '2024-07-01'"
    df = execute_query(conn, query)

    # Transformation of pandas dataframe.
    # df = df.sample(frac=0.1)

    # # Convert PICKUP_DATETIME to datetime object
    # df["TPEP_PICKUP_DATETIME"] = df["TPEP_PICKUP_DATETIME"].astype(int)
    # df["TPEP_PICKUP_DATETIME"] = pd.to_datetime(df["TPEP_PICKUP_DATETIME"], unit="us")

    # Create a new column for the date (just the date part, no time)
    df["pickup_date"] = df["PICKUP_AT"].dt.date

    df["pickup_hour"] = df["PICKUP_AT"].dt.hour

    # Create a column for the day of the week (Sun, Mon, Tue, etc.)
    df["day_of_week"] = df["PICKUP_AT"].dt.strftime("%a")

    # Classify trips into airport and non-airport based on 'airport_fee'
    df["pickup_from_airport_flag"] = np.where(df["AIRPORT_FEE"] > 0, True, False)

    # Plot visualisation using Plotly and Streamlit.
    pio.templates.default = "seaborn"
    st.title("NYC Taxi Trip Analysis 🚖")

    # %%

    # Sidebar filters
    st.sidebar.header("Filter Options")
    min_selectable_date = "2024-06-01"
    max_selectable_date = "2024-06-30"
    date_range = st.sidebar.date_input(
        label="Select Date Range",
        value=(min_selectable_date, max_selectable_date),
        min_value=min_selectable_date,
        max_value=max_selectable_date,
    )

    min_selected_date, max_selected_date = date_range

    df_filtered = df[
        (df["pickup_date"] >= min_selected_date)
        & (df["pickup_date"] <= max_selected_date)
    ]

    # 🚖 Line chart: Count number of trips each day
    st.subheader("What is the quietest day of the week? 🤫")

    # Count the number of trips each day
    daily_trip_counts = (
        df_filtered.groupby(["pickup_date", "day_of_week"])
        .size()
        .reset_index(name="trip_count")
    )

    # Sort by date (just in case it’s not sorted)
    daily_trip_counts = daily_trip_counts.sort_values(by="pickup_date")

    fig = px.line(
        daily_trip_counts,
        x="pickup_date",
        y="trip_count",
        title="Passengers don't take taxis as much at the start of the week.",
        labels={
            "pickup_date": "Date",
            "trip_count": "Number of Trips",
            "day_of_week": "Day of Week",
        },
        markers=True,
        hover_data={"pickup_date": True, "day_of_week": True},
    )

    st.plotly_chart(fig)

    # 🚖 Histogram: Hour breakdown
    st.subheader("What shift is best for taxi drivers? 🎯")

    # df_filtered["DATE"] = df_filtered["TPEP_PICKUP_DATETIME"].dt.date
   

    fig = px.histogram(
        df_filtered,
        x="pickup_hour",
        color="pickup_date",
        barmode="group",
        title="If you hate traffic, drive in the wee hours.",
        labels={"pickup_hour": "Hour of the Day", "pickup_date": "Trip Date"},
        nbins=24,
    )
    st.plotly_chart(fig)

    # 💰 Pie Chart: Payment Method Breakdown
    st.subheader("How do people pay? 💰")

    # Aggregate total fare by payment type
    payment_counts = df_filtered["PAYMENT_TYPE"].value_counts().reset_index()
    payment_counts.columns = ["PAYMENT_TYPE", "COUNT"]
    payment_labels = {
        1: "Credit Card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
        0: "Error data",
    }
    payment_counts["PAYMENT_TYPE"] = payment_counts["PAYMENT_TYPE"].replace(
        payment_labels
    )

    fig = px.pie(
        payment_counts,
        values="COUNT",
        names="PAYMENT_TYPE",
        title="Credit card is king.",
        hole=0.4,
    )

    st.plotly_chart(fig)

    # 💰 Pie Chart: Payment Method Breakdown
    st.subheader("Do people tip more for airport pickups? 🤑")

    # Calculate average tip amount for both groups (airport and non-airport)
    average_tip = (
        df_filtered.groupby("pickup_from_airport_flag")["TIP_AMOUNT"]
        .mean()
        .reset_index()
    )

    # Replace the boolean values with more descriptive labels
    average_tip["pickup_from_airport_flag"] = average_tip[
        "pickup_from_airport_flag"
    ].map({True: "Airport Pickup", False: "Non-Airport Pickup"})

    fig = px.bar(
        average_tip,
        x="pickup_from_airport_flag",
        y="TIP_AMOUNT",
        title="Passengers from the airport are more generous.",
        labels={
            "pickup_from_airport_flag": "Pickup Type",
            "TIP_AMOUNT": "Average Tip Amount",
        },
    )

    st.plotly_chart(fig)

    # Logging message.
    print("Streamlit visualisation succesfully updated.\n")
