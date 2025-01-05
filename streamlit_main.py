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

# Function to convert market cap to numerical value in billions
def convert_market_cap(value):
    if "B" in value:
        return float(value.replace("B", ""))  # Convert to billions
    elif "M" in value:
        return float(value.replace("M", "")) / 1000  # Convert to billions
    return 0  # If the value doesn't contain 'B' or 'M', treat it as 0

# Apply conversion to get numerical values for sorting
result_df['Market Cap Numeric'] = result_df['Market Cap'].apply(convert_market_cap)

# Display the results in Streamlit
st.title("Earnings Calendar with Options Data")

# Show the dataframe with the user-friendly 'Market Cap' and hidden 'Market Cap Numeric' for sorting
st.dataframe(result_df.drop(columns=["Market Cap Numeric"]))  # Hide the numeric column
