import streamlit as st
import pandas as pd
from main import main  # Assuming main.py contains the main function

# Streamlit UI: Date input selection
today = pd.to_datetime("today")

# Calculate the 2nd upcoming Friday from today
days_until_first_friday = (4 - today.weekday()) + (7 if (4 - today.weekday()) < 0 else 0)
second_next_friday = today + pd.DateOffset(days=days_until_first_friday + 7)

# Default date range
default_start_date = today
default_end_date = second_next_friday

# Create Streamlit input widgets for date range
start_date = st.date_input('Start Date', default_start_date)
end_date = st.date_input('End Date', default_end_date)

# Convert Streamlit date inputs to pandas datetime
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# Generate the date range
date_range = pd.date_range(start=start_date, end=end_date)

# Fetch and process the data
result_df = main(date_range)

# Separate the dataframe based on put_bid value
high_put_bid = result_df[result_df['put_bid'].apply(lambda x: float(x.strip('%')) >= 3)]
low_put_bid = result_df[result_df['put_bid'].apply(lambda x: 1 <= float(x.strip('%')) < 3)]

# Display the results in Streamlit
st.title("Earnings Calendar with Options Data")

# Show dataframe with put_bid >= 3%
st.subheader("Put Bid >= 3%")
st.dataframe(high_put_bid)

# Show dataframe with put_bid < 3%
st.subheader("1% <= Put Bid < 3%")
st.dataframe(low_put_bid)
