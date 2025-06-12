# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 20:50:34 2025

@author: Pavilion
"""
import pandas as pd
import re
import time
import requests
import warnings
import pickle
import os
from datetime import datetime
from bs4 import BeautifulSoup

# -------------------------------
# Get S&P 500 Tickers from Wikipedia
# -------------------------------
wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
wiki_response = requests.get(wiki_url)
soup_wiki = BeautifulSoup(wiki_response.text, 'html.parser')
table = soup_wiki.find('table', {'class': "wikitable"})
sp500 = pd.read_html(str(table))[0]
tickers = sp500['Symbol'].str.replace('.', '-', regex=False)


def load_cik_mapping(file_path):
    """
    Reads the SEC Ticker-to-CIK mapping file from a local file.
    """
    cik_df = pd.read_csv(file_path, sep="\t", names=["Ticker", "CIK"])
    cik_df["CIK"] = cik_df["CIK"].astype(
        str).str.zfill(10)  # Ensure 10-digit CIK
    cik_dict = dict(zip(cik_df['Ticker'], cik_df['CIK']))
    return cik_dict


def get_sec_filing_dates(tickers, cik_dict, form_type="10-Q", count=10):

    if type(tickers) != list:
        tickers = [tickers]

    merged_df = pd.DataFrame()
    for ticker in tickers:
        cik = cik_dict.get(ticker.lower())
        if not cik:
            print(f"CIK not found for {ticker}.")
            return None

        # SEC API to fetch filings
        sec_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        headers = {"User-Agent": "Mozilla/5.0"}

        response = requests.get(sec_url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to retrieve SEC filings for {ticker}.")
            return None

        filings_data = response.json()

        # Extract 10-Q filings
        filings = filings_data.get("filings", {}).get("recent", {})

        # Extract relevant data
        form_list = list(zip(filings["form"], filings["filingDate"]))
        ten_q_filings = [f for f in form_list if f[0] == form_type]

        df = pd.DataFrame(ten_q_filings[:count], columns=[
                          "Form Type", "Filing Date"])
        df['Ticker'] = ticker
        merged_df = pd.concat([merged_df, df], axis=0)
    return merged_df


# Load and display the CIK mapping
# https://www.sec.gov/include/ticker.txt
cik_dict = load_cik_mapping(file_path="ticker.txt")
count = 1

dates_df = pd.DataFrame()
for ticker in tickers:
    ticker_df = get_sec_filing_dates(
        ticker, cik_dict=cik_dict, form_type="10-Q", count=None)
    dates_df = pd.concat([dates_df, ticker_df], axis=0)
    print(f"{count}: {ticker} processed. Total records so far: {len(dates_df)}")
    count += 1

dates_df.to_csv('report_dates.csv')
