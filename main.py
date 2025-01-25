import pandas as pd
import finnhub
import yfinance as yf
import warnings
import logging
from earnings import Calendar
import time

# Suppress the warning messages from yfinance (404 errors)
warnings.filterwarnings("ignore", category=UserWarning, module='yfinance')


# Suppress HTTP error messages from yfinance requests
class NullHandler(logging.Handler):
    def emit(self, record):
        pass


logging.getLogger('yfinance').addHandler(NullHandler())

# Initialize the Calendar instance and Finnhub client
calendar = Calendar()
finnhub_client = finnhub.Client(api_key='ctssq59r01qin3c0pde0ctssq59r01qin3c0pdeg')


# Function to fetch earnings data from Calendar API
def fetch_calendar_earnings(calendar, date_range):
    earnings_data = []
    for date in date_range:
        if date.weekday() < 5:  # Only fetch data for weekdays
            try:
                data = calendar.getEarningsByDay(date.strftime('%Y-%m-%d'))
                if data:
                    for entry in data:
                        entry['date'] = date.strftime('%Y-%m-%d')
                    earnings_data.extend(data)
            except Exception:
                continue
    return pd.DataFrame(earnings_data)


# Function to fetch earnings calendar data from Finnhub
def fetch_finnhub_earnings(client, start_date, end_date):
    data = client.earnings_calendar(_from=start_date, to=end_date, symbol="", international=False)
    return pd.DataFrame(data['earningsCalendar'])


# Function to fetch additional data from yfinance
def fetch_additional_data(row):
    try:
        ticker = yf.Ticker(row['ticker'])
        info = ticker.info
        print(ticker)
        # Market cap, full name, and sector
        market_cap = info.get('marketCap', None)
        full_name = info.get('longName', None)
        sector = info.get('sector', None)

        # ATM put option data
        earnings_date = pd.to_datetime(row['date'])
        next_friday = earnings_date + pd.DateOffset(days=(4 - earnings_date.weekday()) % 7)

        try:
            options = ticker.option_chain(next_friday.strftime('%Y-%m-%d'))
            current_price = info.get('currentPrice', None)
            puts = options.puts
            puts = puts[puts['strike'] <= current_price]
            closest_strike = puts['strike'].max()
            atm_put = puts[puts['strike'] == closest_strike]

            put_bid = atm_put['bid'].iloc[0] if not atm_put.empty else None
            put_ask = atm_put['ask'].iloc[0] if not atm_put.empty else None

            # Calculate bid/ask as percentage of strike price
            if put_bid and closest_strike:
                put_bid = (put_bid / closest_strike) * 100
            if put_ask and closest_strike:
                put_ask = (put_ask / closest_strike) * 100
        except Exception:
            put_bid, put_ask = None, None

        return pd.Series([put_bid, put_ask, market_cap, full_name, sector, next_friday],
                         index=['put_bid', 'put_ask', 'market_cap', 'full_name', 'sector', 'next_friday'])
    except Exception as e:
        print(f"Error fetching data for {row['ticker']}: {e}")
        return pd.Series([None, None, None, None, None, None],
                         index=['put_bid', 'put_ask', 'market_cap', 'full_name', 'sector', 'next_friday'])


# Format Market Cap as readable string
def format_market_cap(value):
    if value is None or pd.isna(value):  # Check if the value is None or NaN
        return None
    return round(value / 1e9, 2)


# Function to format put bid and put ask as percentages
def format_put_value(value):
    # Check if the value is None, NaT, or a non-numeric type
    if value is None or pd.isna(value):
        return None
    return f"{value:.2f}%" if value is not None else None


# Batch processing with delays
# Batch processing with delays and timing
def batch_apply_with_timing(df, func, batch_size=50, delay=5):
    """
    Apply a function to a DataFrame in batches with delays and log the time for each batch.
    """
    all_results = []
    for i in range(0, len(df), batch_size):
        batch_start_time = time.time()  # Start timing the batch
        batch = df.iloc[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1}/{(len(df) + batch_size - 1) // batch_size}...")

        # Apply the function to the current batch
        results = batch.apply(func, axis=1)
        all_results.append(results)

        # Measure and print batch time
        batch_end_time = time.time()
        batch_time = batch_end_time - batch_start_time
        print(f"Batch {i // batch_size + 1} completed in {batch_time:.2f} seconds.")

        # Delay before processing the next batch
        if i + batch_size < len(df):  # Avoid unnecessary delay after the last batch
            print(f"Waiting {delay} seconds before the next batch...")
            time.sleep(delay)

    return pd.concat(all_results)


# Main function to fetch and process data
def main(date_range):
    # Fetch data from both sources
    calendar_df = fetch_calendar_earnings(calendar, date_range)
    # Convert date range to strings for start and end
    start_date = date_range.min().strftime('%Y-%m-%d')
    end_date = date_range.max().strftime('%Y-%m-%d')
    finnhub_df = fetch_finnhub_earnings(finnhub_client, start_date, end_date)

    # Clean and prepare the Calendar DataFrame
    calendar_df = calendar_df[['date', 'ticker']] if 'ticker' in calendar_df.columns else calendar_df

    # Clean and prepare the Finnhub DataFrame
    finnhub_df = finnhub_df[['date', 'symbol']]
    finnhub_df.rename(columns={'symbol': 'ticker'}, inplace=True)

    # Merge the two datasets
    merged_df = pd.concat([finnhub_df, calendar_df], ignore_index=True)

    # Sort by 'ticker' and 'date' to prioritize earliest dates
    merged_df = merged_df.sort_values(by=['ticker', 'date'])

    # Remove duplicates, keeping the earliest date for each ticker
    unique_ticker_df = merged_df.drop_duplicates(subset=['ticker'], keep='first')

    # Apply the additional data fetching function
    #print(len(unique_ticker_df))
    additional_data = batch_apply_with_timing(unique_ticker_df, fetch_additional_data, batch_size=50, delay=5)
    #additional_data = unique_ticker_df.apply(fetch_additional_data, axis=1)

    # Assign the additional data to the DataFrame's columns
    unique_ticker_df.loc[:,
    ['put_bid', 'put_ask', 'market_cap', 'full_name', 'sector', 'next_friday']] = additional_data

    # Format Market Cap as readable string
    unique_ticker_df.loc[:, 'market_cap'] = unique_ticker_df['market_cap'].apply(format_market_cap)

    # Apply formatting functions for put_bid and put_ask
    unique_ticker_df.loc[:, 'put_bid'] = unique_ticker_df['put_bid'].apply(format_put_value)
    unique_ticker_df.loc[:, 'put_ask'] = unique_ticker_df['put_ask'].apply(format_put_value)

    # Calculate next Friday for each date in the 'date' column
    unique_ticker_df.loc[:, 'next_friday'] = pd.to_datetime(unique_ticker_df['date']).apply(
        lambda x: x + pd.DateOffset(days=(4 - x.weekday()) % 7)
    )

    # Remove rows where put_bid or put_ask are NaN
    unique_ticker_df = unique_ticker_df.dropna(subset=['put_bid', 'put_ask'])

    # Reorder columns as required
    unique_ticker_df = unique_ticker_df[
        ['date', 'ticker', 'put_bid', 'put_ask', 'market_cap', 'full_name', 'sector', 'next_friday']]

    # Sort by 'date'
    unique_ticker_df = unique_ticker_df.sort_values(by='date')

    return unique_ticker_df


# Main testing area
if __name__ == "__main__":
    # Set date range for testing
    date_range = pd.date_range(start="2025-01-25", end="2025-02-07")

    # Call main function to process data
    result_df = main(date_range)

    # Print the final DataFrame for testing
    print("Final DataFrame:")
    print(result_df)

    # Save the cleaned DataFrame to CSV (optional)
    result_df.to_csv("earnings_with_options.csv", index=False)
