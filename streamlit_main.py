import pandas as pd
import streamlit as st
from main import main  # Assuming main.py contains the main function

# Function to convert formatted market cap strings to numeric values for sorting
def market_cap_to_numeric(market_cap):
    if isinstance(market_cap, str):
        if 'B' in market_cap:
            return float(market_cap.replace('B', '')) * 1e9
        elif 'M' in market_cap:
            return float(market_cap.replace('M', '')) * 1e6
        elif 'K' in market_cap:
            return float(market_cap.replace('K', '')) * 1e3
    return market_cap

# Streamlit app definition
def streamlit_main():
    st.title("Earnings Calendar with Options Data")

    # Default date range: today to next week's Friday
    today = pd.to_datetime("today")
    next_friday = today + pd.DateOffset(days=(4 - today.weekday()) + 7)

    # User input for date range
    start_date = st.date_input("Start Date", value=today)
    end_date = st.date_input("End Date", value=next_friday)

    # Ensure valid date range
    if start_date > end_date:
        st.error("Start date must be before or equal to end date.")
        return

    # Generate the date range
    date_range = pd.date_range(start=start_date, end=end_date)

    # Fetch and process the data
    st.write("Fetching data...")
    try:
        result_df = main(date_range)

        # Enable sorting on market cap
        result_df['market_cap'] = result_df['market_cap'].apply(market_cap_to_numeric)

        # Display results
        st.dataframe(
            result_df[['date', 'ticker', 'put_bid', 'put_ask', 'market_cap', 'full_name', 'sector', 'next_friday']],
            use_container_width=True
        )
    except Exception as e:
        st.error(f"An error occurred: {e}")

# Run the Streamlit app
if __name__ == "__main__":
    streamlit_main()
