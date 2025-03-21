import pandas as pd 
import requests 
from bs4 import BeautifulSoup 
from selenium import webdriver
import time
import yfinance as yf
from yahoofinancials import YahooFinancials
import missingno as mno
from datetime import datetime, timedelta
import warnings
from io import StringIO
import os

# Function to handle rate-limiting by checking Retry-After header
def handle_rate_limit(response, timeout = 20):
    if 'Retry-After' in response.headers:
        retry_after = response.headers['Retry-After']
        try:
            # Retry-After can be in seconds or as a date
            wait_time = int(retry_after)  # If it's in seconds
            print(f"Rate limit hit. Waiting for {wait_time} seconds...")
            time.sleep(wait_time)
        except ValueError:
            # If Retry-After is a date, calculate the time difference
            retry_time = datetime.strptime(retry_after, '%a, %d %b %Y %H:%M:%S GMT')
            wait_time = (retry_time - datetime.utcnow()).total_seconds()
            if wait_time > 0:
                print(f"Rate limit hit. Waiting until {retry_time} ({wait_time:.0f} seconds)...")
                time.sleep(wait_time)
    else:
        # Fallback wait time in case Retry-After is not present
        wait_time = timeout  # Default wait time
        print(f"Rate limit hit. Waiting for {wait_time} seconds...")
        time.sleep(wait_time)

# Function to retrieve the data with retry-after handling
def fetch_data(url, headers, timeout=20, retries=3):
    while retries > 0:
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 429:
                print(f"Rate limit encountered for {url}.")
                handle_rate_limit(response, timeout)
                retries -= 1  # Reduce the retry count
            elif response.status_code == 200:
                return response.text  # Return the response content if successful
            else:
                print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    return None  # Return None if all retries are exhausted


# Suppress all deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


#List of SP500 companies
wikiurl="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
response=requests.get(wikiurl)

soup = BeautifulSoup(response.text, 'html.parser')
sp500=soup.find('table',{'class':"wikitable"})
sp500=pd.read_html(str(sp500))
sp500=pd.DataFrame(sp500[0])
names = sp500['Symbol']


#financial data
variables = ['eps-earnings-per-share-diluted', 'market-cap', 'shares-outstanding']
    
headers = {
    'Accept-Encoding': 'gzip, deflate, br',  # Include Brotli for better compression support
    'Accept-Language': 'en-US,en;q=0.9',     # Adjusted language preference
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',  # Updated User-Agent for a recent browser version
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',  # Updated MIME types, including modern image formats
    'Cache-Control': 'no-cache',  # Use no-cache to ensure fresh data
    'Connection': 'keep-alive',
    'DNT': '1',  # Enable Do Not Track
}

financial_df = pd.DataFrame()

for var in variables:
    count = 0
    for name in names.str.replace('-', '.'):
        url = f"https://www.macrotrends.net/stocks/charts/{name}/apple/{var}"
        
        # Fetch data with retry-after handling
        response_text = fetch_data(url, headers, timeout = 30)
        if response_text:
            # Process the data if fetched successfully
            soup = BeautifulSoup(response_text, 'html.parser')
            ticker_fin = soup.find_all('table', {'class': "table"})
            if ticker_fin:
                # Convert the HTML table to a DataFrame
                html_str = str(ticker_fin)
                try:
                    ticker_fin = pd.read_html(StringIO(html_str))[1]
                except:
                    ticker_fin = pd.read_html(StringIO(html_str))[0]
                ticker_fin = pd.DataFrame(ticker_fin)
                ticker_fin.columns = ['Date', 'Amount']
                
                # Add extra columns for the variable and company name
                ticker_fin['Variable'] = var
                ticker_fin['COMPANY'] = name
                
                # Concatenate the result with the main DataFrame
                financial_df = pd.concat([financial_df, ticker_fin], ignore_index=True)
                print(f"{count}: {name} is done. Total number of records: {len(financial_df)}")
            else:
                print(f"No table found for {name} and {var}")
        else:
            print(f"Skipping {name} due to failure in retrieving data.")
        
        count += 1

    financial_df.to_csv('sp500_extra.csv')



