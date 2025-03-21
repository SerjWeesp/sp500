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

# -------------------------------
# Get S&P 500 Tickers from Wikipedia
# -------------------------------
wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

wiki_response = requests.get(wiki_url)
soup_wiki = BeautifulSoup(wiki_response.text, 'html.parser')
table = soup_wiki.find('table', {'class': "wikitable"})
sp500 = pd.read_html(str(table))[0]

tickers = sp500['Symbol'].str.replace('.', '-', regex=False)
tickers.to_csv('tickers.csv')
print("Total number of tickers: ", len(tickers))