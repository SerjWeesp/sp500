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
from selenium.common.exceptions import WebDriverException, NoSuchElementException, TimeoutException, StaleElementReferenceException


warnings.filterwarnings("ignore", category=DeprecationWarning)

# -------------------------------
# Get S&P 500 Tickers from Wikipedia
# -------------------------------

sp500_names = pd.read_csv('D:/GitHub/sp500/sp500_names.csv')
tickers = sp500_names['Symbol']

# -------------------------------
# Selenium Setup
# -------------------------------
edge_driver_path = r'C:\Users\Dell\Downloads\edgedriver_win64\msedgedriver.exe'
edge_options = EdgeOptions()
edge_options.page_load_strategy = "eager"
service = EdgeService(executable_path=edge_driver_path)
driver = webdriver.Edge(service=service, options=edge_options)
wait = WebDriverWait(driver, 180)  

# -------------------------------
# Load Progress from Pickle (if exists)
# -------------------------------
pickle_file = 'D:/GitHub/sp500/sp500_financials_temp.pkl'

if os.path.exists(pickle_file):
    with open(pickle_file, 'rb') as f:
        data = pickle.load(f)
        df = data['df']
        counter = data['counter']
        tickers = tickers[counter:]  # Restart from the last processed ticker
        if not tickers.empty:
            print(
                f"Resuming from ticker {tickers.iloc[0]} at counter {counter}...")
        else:
            print("Tickers list is empty. Starting from the beginning.")
            tickers = sp500_names['Symbol'] # Reset tickers
else:
    df = pd.DataFrame()
    counter = 0

max_retries = 3
reports = ['', 'balance-sheet', 'cash-flow-statement', 'ratios']

for ticker in tickers:
    skip_ticker = False
    counter += 1
    for report in reports:
        if skip_ticker:
            print(f"[{ticker}] 404 encountered → skipping remaining reports")
            continue
        else:
            url = f'https://stockanalysis.com/stocks/{ticker}/financials/{report}'
            driver.get(url)
            time.sleep(1)
            # 1) bail out on 404
            try:
                banner = driver.find_element(
                    By.XPATH,
                    "//div[@class='mb-4 text-2xl font-bold sm:text-3xl']"
                ).text
                if banner == "Page Not Found - 404":
                    skip_ticker = True
                    break
            except NoSuchElementException:
                # banner not found → assume page is good
                pass
            
            #bail out on cookies
            try:
                btn = driver.find_element(
                    By.XPATH,
                    "/html/body/div[2]/div[2]/div[2]/div[2]/div[2]/button[1]/p"
                )
                btn.click()
            except NoSuchElementException:
                pass
            # 2) click the “+20 Quarters” button with retries
            for attempt in range(1, max_retries + 1):
                try:
                    btn = wait.until(EC.element_to_be_clickable((
                        By.XPATH,
                        "/html/body/div/div[1]/div[2]/main/div[2]/nav[2]/ul/li[2]/button"
                    )))
                    btn.click()
                    time.sleep(1)
                    break  # success → exit retry loop
                except TimeoutException:
                    if attempt == max_retries:
                        print(f"[{ticker} – {report}] button never appeared after {max_retries} tries; skipping report")
                        break
                    else:
                        print(f"[{ticker} – {report}] retry {attempt}/{max_retries}…")
            else:
                # only runs if the loop never hit `break`
                print(f"[{ticker} – {report}] failed to click, moving on")
                continue
    
            # 3) extract the table
            try:
                table_html = driver.find_element(
                    By.XPATH, '//*[@id="main-table"]'
                ).get_attribute("outerHTML")
                ticker_df = pd.read_html(StringIO(table_html))[0]
                ticker_df.set_index(ticker_df.columns[0], inplace=True)
            except NoSuchElementException:
                print(f"[{ticker} – {report}] no main-table found")
         
            # Melt dataframe
            long = (ticker_df.stack(
                level=[0, 1], future_stack=True).reset_index(name='Value'))
    
            # Rename the newly created columns:
            long.columns = ['Metric', 'Fiscal Quarter',
                            'Period Ending', 'Value']
            long = long[long['Value'] != 'Upgrade']
            long['Ticker'] = ticker
            df = pd.concat([df, long], ignore_index=True)
            print(
                f"{counter} {ticker} {report} is scrapped.Total number of rows is {len(df)}.")  
            data = {'df': df, 'counter': counter}
            with open(pickle_file, 'wb') as f:
                pickle.dump(data, f)
     

df.drop_duplicates(inplace=True)
df.to_csv('sp500_financials_new.csv')
driver.quit()  # Ensure the driver is closed even if errors occur
