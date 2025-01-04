import streamlit as st
import pandas as pd
from main import main  # Assuming main.py contains the main function

# Streamlit UI: Date input selection
today = pd.to_datetime("today")
next_friday = today + pd.DateOffset(days=(4 - today.weekday()) + 7)

# Default date range
default_start_date = today
default_end_date = next_friday

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

# Display the results in Streamlit
st.title("Earnings Calendar with Options Data")
st.dataframe(result_df)

