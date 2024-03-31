import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
import sqlite3

# Function to ingest OHLC data from Yahoo Finance
def fetch_stock_data(ticker, start_date, end_date):
    df = yf.download(ticker, start=start_date, end=end_date)
    return df

# Function to handle missing values
def handle_missing_values(df):
    # Impute missing values using mean or any other strategy
    imputer = SimpleImputer(strategy='mean')
    df_filled = pd.DataFrame(imputer.fit_transform(df), columns=df.columns, index=df.index)
    return df_filled

# Function to detect and correct outliers
def handle_outliers(df):
    # Implement outlier detection and correction techniques
    # For example, remove rows where prices deviate significantly from the mean
    return df

# Function to standardize date format
def standardize_date_format(df):
    # Convert index to datetime format if not already
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    return df

# Function to calculate technical indicators
def calculate_technical_indicators(df):
    # Implement calculation of technical indicators such as moving averages, RSI, etc.
    # Example:
    df['SMA_20'] = df['Close'].rolling(window=20).mean()
    return df

# Function to resample data
def resample_data(df, frequency='D'):
    # Resample the data based on desired frequency (default: daily)
    df_resampled = df.resample(frequency).agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', 'Volume':'sum'})
    return df_resampled

# Function to store data in SQLite database
def store_data_in_db(df, db_name='stock_data.db', table_name='stock_data'):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace')
    conn.close()

# Example usage
if __name__ == "__main__":
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    start_date = '2023-01-01'
    end_date = '2024-01-01'
    
    for ticker in tickers:
        # Ingest data
        df = fetch_stock_data(ticker, start_date, end_date)
        
        # Data cleaning
        df = handle_missing_values(df)
        df = handle_outliers(df)
        df = standardize_date_format(df)
        
        # Data transformation
        df = calculate_technical_indicators(df)
        df = resample_data(df)
        
        # Data storage
        store_data_in_db(df)
