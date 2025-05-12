# -*- coding: utf-8 -*-
"""
Created on Sun May 11 10:54:35 2025

@author: Pavilion
"""

import pandas as pd
import re
import time
import requests
import warnings
import pickle
import os
import time
from datetime import datetime
from io import StringIO
from bs4 import BeautifulSoup

# Selenium imports for Edge
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException



warnings.filterwarnings("ignore", category=DeprecationWarning)

#financial report type
report = 'cash-flow-statement'

# -------------------------------
# Get S&P 500 Tickers from Wikipedia
# -------------------------------
wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
wiki_response = requests.get(wiki_url)
soup_wiki = BeautifulSoup(wiki_response.text, 'html.parser')
table = soup_wiki.find('table', {'class': "wikitable"})
sp500 = pd.read_html(str(table))[0]
tickers = sp500['Symbol'].str.replace('.', '-', regex=False)

# -------------------------------
# Selenium Setup
# -------------------------------
edge_driver_path = r'D:\Extra study\edgedriver_win64\msedgedriver.exe'
edge_options = EdgeOptions()
edge_options.page_load_strategy = "eager"
service = EdgeService(executable_path=edge_driver_path)
driver = webdriver.Edge(service=service, options=edge_options)
wait = WebDriverWait(driver, 180)  # wait up to 10s for each attempt

df = pd.DataFrame()
counter = 0
max_retries = 3

for ticker in tickers[counter:]:
    counter +=1
    for report in ['', 'balance-sheet', 'cash-flow-statement', 'ratios']:
        url = f'https://stockanalysis.com/stocks/{ticker}/financials/{report}'
        driver.get(url)

        # Retry clicking the “Quarters” button up to max_retries times
        for attempt in range(max_retries):
            try:
                button = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "/html/body/div/div[1]/div[2]/main/div[2]/nav[2]/ul/li[2]/button")
                ))
                button.click()
                
                # Locate the table by XPath and get its outer HTML
                table_elem = driver.find_element(By.XPATH, '//*[@id="main-table"]')
                table_html = table_elem.get_attribute("outerHTML")
                
                # Parse with pandas (via a StringIO buffer to avoid FutureWarning)
                buffer = StringIO(table_html)
                ticker_df = pd.read_html(buffer)[0]
                ticker_df.set_index(ticker_df.columns[0], inplace=True)  
                
                # Melt dataframe
                long = (ticker_df.stack(level=[0,1], future_stack=True).reset_index(name='Value'))
            
                # Rename the newly created columns:
                long.columns = ['Metric','Fiscal Quarter','Period Ending','Value']
                long = long[long['Value']!='Upgrade']
                long['Ticker'] = ticker
                df = pd.concat([df, long], ignore_index=True)
                
                print(f"{counter} {ticker} {report} is scrapped.Total number of rows is {len(df)}.")
                break
            
            except TimeoutException:
                if attempt == max_retries - 1:
                    print(f"Button never appeared for {url}. Skippoing ticker")

df.to_csv('C:/Users/Pavilion/GitHub/sp500/sp500_financials_new.csv')
            
        
    
    