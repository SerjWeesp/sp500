# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 21:38:54 2025

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

RUN_STUMP = datetime.now().strftime("%d%m%Y")  # {timestamp}

# -------------------------------
# Get S&P 500 Tickers from Wikipedia
# -------------------------------
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

wiki_response = requests.get(wiki_url, headers=headers)
soup_wiki = BeautifulSoup(wiki_response.text, 'html.parser')
table = soup_wiki.find('table', {'class': "wikitable"})
sp500 = pd.read_html(str(table))[0]

# tickers = sp500['Symbol'].str.replace('.', '-', regex=False)
sp500.to_csv(f'sp500_names_{RUN_STUMP}.csv')
print("Total number of tickers: ", len(sp500))

