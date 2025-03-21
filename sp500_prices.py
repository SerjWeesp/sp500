# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 21:26:31 2025

@author: Pavilion
"""
import pandas as pd 
import requests 
import time
import yfinance as yf
from yahoofinancials import YahooFinancials
from bs4 import BeautifulSoup


wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

wiki_response = requests.get(wiki_url)
soup_wiki = BeautifulSoup(wiki_response.text, 'html.parser')
table = soup_wiki.find('table', {'class': "wikitable"})
sp500 = pd.read_html(str(table))[0]

tickers = sp500['Symbol'].str.replace('.', '-', regex=False)

#Stock data
price_df = pd.DataFrame()
count = 0

for ticker_sym in tickers:
    ticker = yf.Ticker(ticker_sym)
    ticker_df = ticker.history(period='max', interval='1d', auto_adjust=True).sort_index()
    if ticker_df.empty:
        print(f"No data for ticker {ticker_sym}, skipping.")
        continue
    
    # Create a full date range from the first to the last trading day.
    full_index = pd.date_range(start=ticker_df.index.min(), end=ticker_df.index.max(), freq="D")
    
    # Reindex to this full range and forward-fill missing values
    ticker_df = ticker_df.reindex(full_index).ffill()
    
    ticker_df['Company'] = ticker_sym
    ticker_df.reset_index(inplace=True)
    price_df = pd.concat([price_df, ticker_df], ignore_index=True)
    count += 1
    print(count, f"{ticker_sym} processed. Total records: {len(price_df)}")
    
price_df.to_csv('sp500_prices.csv')

