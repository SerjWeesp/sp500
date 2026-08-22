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
import io
import os
from datetime import datetime
from bs4 import BeautifulSoup

RUN_STUMP = datetime.now().strftime("%d%m%Y")  # {timestamp}

def load_cik_mapping_from_web(url="https://www.sec.gov/include/ticker.txt"):
    """
    Fetches the SEC Ticker-to-CIK mapping file directly from the web.
    """
    # The SEC strictly requires a declared User-Agent.
    headers = {
        "User-Agent": "Sergey (your_actual_email@domain.com)" 
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Fails fast if the request is blocked or fails
    
    # io.StringIO wraps the text response so pd.read_csv can process it like a local file
    cik_df = pd.read_csv(io.StringIO(response.text), sep="\t", names=["Ticker", "CIK"])
    
    # Ensure 10-digit CIK
    cik_df["CIK"] = cik_df["CIK"].astype(str).str.zfill(10)  
    
    cik_dict = dict(zip(cik_df['Ticker'], cik_df['CIK']))
    
    return cik_dict


def get_sec_filing_dates(tickers, cik_dict):
    """
    Fetches the complete history of strictly 10-Q and 10-K filings for the given tickers.
    """
    if type(tickers) != list:
        tickers = [tickers]

    merged_df = pd.DataFrame()
    headers = {"User-Agent": "Sergey (your_actual_email@domain.com)"}
    
    # We use a Set for O(1) ultra-fast lookups
    target_forms = {"10-Q", "10-K"} 

    for ticker in tickers:
        cik = cik_dict.get(ticker.lower())
        if not cik:
            print(f"CIK not found for {ticker}.")
            continue

        sec_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(sec_url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to retrieve SEC filings for {ticker}.")
            continue

        filings_data = response.json()
        filings = filings_data.get("filings", {})
        
        # Array to hold only the forms we care about
        clean_filings = []

        # 1. Process the recent 1,000 filings
        recent = filings.get("recent", {})
        recent_forms = zip(recent.get("form", []), recent.get("filingDate", []), recent.get("reportDate", []))
        
        # Filter instantly to save RAM
        clean_filings.extend([f for f in recent_forms if f[0] in target_forms])

        # 2. Process older archives to get the complete history
        older_files = filings.get("files", [])
        for archive in older_files:
            file_name = archive.get("name")
            if not file_name:
                continue
            
            archive_url = f"https://data.sec.gov/submissions/{file_name}"
            archive_resp = requests.get(archive_url, headers=headers)
            
            if archive_resp.status_code == 200:
                archive_data = archive_resp.json()
                archive_forms = zip(archive_data.get("form", []), archive_data.get("filingDate", []), archive_data.get("reportDate", []))
                
                # Filter instantly to save RAM
                clean_filings.extend([f for f in archive_forms if f[0] in target_forms])
            
            time.sleep(0.15) # SEC rate limit protection

        # 3. Build the final dataframe for this ticker
        df = pd.DataFrame(clean_filings, columns=["Form Type", "Filing Date", "Period Ending"])
        df['Ticker'] = ticker
        
        if not df.empty:
            merged_df = pd.concat([merged_df, df], axis=0)
            
        time.sleep(0.15)

    return merged_df

cik_dict = load_cik_mapping_from_web()
sp500 = pd.read_csv('data/sp500_names_14052026.csv')
tickers = sp500['Symbol'].str.replace('.', '-', regex=False)

count = 1

dates_df = pd.DataFrame()
for ticker in tickers:
    ticker_df = get_sec_filing_dates(
        ticker, cik_dict=cik_dict)
    dates_df = pd.concat([dates_df, ticker_df], axis=0)
    print(f"{count}: {ticker} processed. Total records so far: {len(dates_df)}")
    count += 1

dates_df.to_csv(f'data/sp500_report_dates_{RUN_STUMP}.csv', index=False)
