# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 21:26:31 2025

@author: Pavilion
"""
import os
import pandas as pd
import requests
import time
from datetime import datetime
import yfinance as yf
from bs4 import BeautifulSoup


DATA_DIR = r"D:/GitHub/sp500"
run_stamp = datetime.now().strftime("%d%m%Y")  # {timestamp}
final_csv = os.path.join(DATA_DIR, f"sp500_prices_{run_stamp}.csv")
names_csv = os.path.join(DATA_DIR, "sp500_names_03012026.csv")
sp500_names = pd.read_csv(names_csv)
tickers = [t.replace(".", "-").upper() for t in sp500_names["Symbol"].astype(str)]


# Stock data
price_df = pd.DataFrame()
count = 0

for ticker_sym in tickers:
    ticker = yf.Ticker(ticker_sym)
    ticker_df = ticker.history(
        period='max', interval='1d', auto_adjust=True).sort_index()
    if ticker_df.empty:
        print(f"No data for ticker {ticker_sym}, skipping.")
        continue

    # Create a full date range from the first to the last trading day.
    full_index = pd.date_range(
        start=ticker_df.index.min(), end=ticker_df.index.max(), freq="D")

    # Reindex to this full range and forward-fill missing values
    ticker_df = ticker_df.reindex(full_index).ffill()

    ticker_df['Company'] = ticker_sym
    ticker_df.reset_index(inplace=True)
    price_df = pd.concat([price_df, ticker_df], ignore_index=True)
    count += 1
    print(count, f"{ticker_sym} processed. Total records: {len(price_df)}")

price_df.to_csv(final_csv)
print("Not scrapped tickers:", set(tickers) - set(price_df['Company'].unique()))