#Step 1: Downlaod historical stock information

import yfinance as yf
import pandas as pd

#List of stock tickers
tickers = [
    "AAPL",
    "TSLA",
    "NVDA",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "JPM",
    "DIS",
    "NFLX",
    "PYPL",
]

#Download historical stock data and save as CSV
for ticker in tickers:
    stock_data = yf.download(ticker, period="5y", interval="1d")
    stock_data.to_csv(f"../data/{ticker}_historical_data.csv")
    print(f"Downloaded data for {ticker}")

print("All stock data downloaded and saved.")
