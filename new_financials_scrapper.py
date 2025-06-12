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
from selenium.common.exceptions import WebDriverException


warnings.filterwarnings("ignore", category=DeprecationWarning)

# -------------------------------
# Get S&P 500 Tickers from Wikipedia
# -------------------------------

sp500_names = pd.read_csv('D:/GitHub/sp500/sp500_names.csv')
tickers = sp500_names['Symbol']


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
            tickers = sp500_names['Symbol']  # Reset tickers
else:
    df = pd.DataFrame()
    counter = 0


max_retries = 2

# -------------------------------
# Selenium Setup
# -------------------------------
edge_driver_path = r'C:\Users\Dell\Downloads\edgedriver_win64\msedgedriver.exe'
edge_options = EdgeOptions()
edge_options.page_load_strategy = "eager"
service = EdgeService(executable_path=edge_driver_path)
driver = webdriver.Edge(service=service, options=edge_options)
wait = WebDriverWait(driver, 180)  # wait up to 10s for each attempt

try:
    for ticker in tickers:
        counter += 1
        skip_ticker = False  # Flag to skip the ticker if TimeoutException occurs

        for report in ['', 'balance-sheet', 'cash-flow-statement', 'ratios']:
            if skip_ticker:
                print(
                    f"Skipping {ticker} {report} due to previous TimeoutException")
                continue

            url = f'https://stockanalysis.com/stocks/{ticker}/financials/{report}'

            # Retry clicking the “Quarters” button up to max_retries times
            for attempt in range(max_retries):
                try:
                    driver.get(url)
                    button = wait.until(EC.element_to_be_clickable(
                        (By.XPATH,
                         "/html/body/div/div[1]/div[2]/main/div[2]/nav[2]/ul/li[2]/button")
                    ))
                    button.click()
                    time.sleep(2)
                    # Locate the table by XPath and get its outer HTML
                    table_elem = driver.find_element(
                        By.XPATH, '//*[@id="main-table"]')
                    table_html = table_elem.get_attribute("outerHTML")

                    # Parse with pandas (via a StringIO buffer to avoid FutureWarning)
                    buffer = StringIO(table_html)
                    ticker_df = pd.read_html(buffer)[0]
                    ticker_df.set_index(ticker_df.columns[0], inplace=True)

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
                    break  # Exit the attempt loop

                except TimeoutException:
                    print(f"TimeoutException for {ticker} {report}")
                    if attempt == max_retries - 1:
                        print(
                            f"Button never appeared for {url}. Skipping all reports for {ticker}")
                        skip_ticker = True  # Set the flag to skip the ticker
                        break  # Exit the attempt loop
                    else:
                        print(
                            f"Retrying {ticker} {report} (attempt {attempt + 2})")
                except Exception as e:
                    print(f"An error occurred: {e}")
                    break  # Exit the attempt loop

        if skip_ticker:
            print(f"Skipping all remaining reports for {ticker}")

        # -------------------------------
        # Save Progress to Pickle
        # -------------------------------


except WebDriverException as e:
    print(f"WebDriverException: {e}")
    print("The browser session was likely closed. Exiting the script.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    df.to_csv('sp500_financials_new.csv')
    try:
        pass  # driver.quit()  # Ensure the driver is closed even if errors occur
    except:
        pass
