# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 18:29:42 2025

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

# Selenium imports for Edge
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from selenium.common.exceptions import StaleElementReferenceException

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
driver.maximize_window()

# Load an initial Macrotrends page to handle cookie/adblock banners.
test_url = f"https://www.macrotrends.net/stocks/charts/AAPL/a/{report}?freq=Q"
driver.get(test_url)
time.sleep(3)
try:
    accept_btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='Accept all']"))
    )
    accept_btn.click()
    print("Cookie banner accepted!")
except Exception:
    pass

# -------------------------------
# Helper Functions
# -------------------------------

def find_grid_container(driver, container_id="contenttablejqxgrid", timeout=15, iframe_timeout=5, retries=3, retry_interval=30):
    """
    Locate the grid container by its ID.
    Searches the main document first; if not found, it iterates through iframes.
    Retries the search a few times if necessary.
    Returns the container element or None if not found.
    """
    for attempt in range(retries):
        try:
            container = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.ID, container_id))
            )
            return container
        except Exception as e:
            print(f"Attempt {attempt+1}: Grid container not found in main document. Checking iframes...", e)
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            if not iframes:
                print("No iframes found on the page.")
            else:
                for iframe in iframes:
                    try:
                        src = iframe.get_attribute('src')
                    except Exception as e_get:
                        print("Could not get src attribute from an iframe:", e_get)
                        continue
                    try:
                        fresh_iframe = driver.find_element(By.XPATH, f"//iframe[@src='{src}']")
                    except Exception as e_find:
                        print("Could not re-find the iframe with src '{}': {}".format(src, e_find))
                        continue
                    try:
                        driver.switch_to.frame(fresh_iframe)
                        container = WebDriverWait(driver, iframe_timeout).until(
                            EC.presence_of_element_located((By.ID, container_id))
                        )
                        driver.switch_to.default_content()
                        return container
                    except Exception as e_iframe:
                        print("Error in iframe with src '{}': {}".format(src, e_iframe))
                        driver.switch_to.default_content()
                        continue
            print(f"Retrying in {retry_interval} seconds...")
            driver.refresh()
            time.sleep(retry_interval)
    return None


def vertical_scroll(grid_container, max_attempts=10, tolerance=1):
    """
    Scroll vertically until no new rows load or the bottom of the grid is reached.
    It checks if the current scroll position is near the maximum (scrollHeight - clientHeight).
    """
    for _ in range(max_attempts):
        try:
            prev_scroll = driver.execute_script("return arguments[0].scrollTop;", grid_container)
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", grid_container)
            #time.sleep(1)
            current_scroll = driver.execute_script("return arguments[0].scrollTop;", grid_container)
            scroll_height = driver.execute_script("return arguments[0].scrollHeight;", grid_container)
            client_height = driver.execute_script("return arguments[0].clientHeight;", grid_container)
            
            # If current_scroll is close to the maximum scrollable position, break.
            if current_scroll >= scroll_height - client_height - tolerance:
                break
            
            # Also break if there is no significant change.
            if abs(current_scroll - prev_scroll) < tolerance:
                break
        except Exception as e:
            print("Error during vertical scrolling:", e)
            try:
                driver.refresh()
            except Exception as e2:
                print("Driver refresh failed:", e2)
                break
            time.sleep(30)
    return grid_container


def capture_grid_data(grid_container, additional_offset=0):
    """
    Capture the visible grid segment (data rows) and adjust cell keys by adding 
    the current horizontal scroll (additional_offset) so that keys are absolute.
    """
    html = grid_container.get_attribute("outerHTML")
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    for row in soup.find_all("div", {"role": "row"}):
        row_id = row.get("id")
        if not row_id:
            continue
        if row_id not in data:
            data[row_id] = {}
        for cell in row.find_all("div", {"role": "gridcell"}):
            style = cell.get("style", "")
            m = re.search(r'left:\s*(\d+)px', style)
            left_val = int(m.group(1)) if m else 0
            key = left_val + additional_offset
            text = cell.get_text(strip=True)
            data[row_id][key] = text
    return data

def capture_headers():
    """Capture the visible column headers."""
    headers_dict = {}
    for header in driver.find_elements(By.XPATH, "//div[@role='columnheader']"):
        try:
            style = header.get_attribute("style")
            m = re.search(r'left:\s*(\d+)px', style)
            left_val = int(m.group(1)) if m else 0
        except Exception:
            left_val = 0
        try:
            span = header.find_element(By.XPATH, ".//span")
            header_text = span.text.strip()
        except Exception:
            header_text = header.text.strip()
        if header_text:
            headers_dict[left_val] = header_text
    return headers_dict

def capture_all_table_data(driver, grid_container, offset_x=None, max_attempts=10):
    """
    Scroll horizontally to capture the complete table.
    Returns a tuple: (combined_headers, combined_data)
      - combined_headers: dict mapping absolute left offset to header text.
      - combined_data: dict mapping row id to dict of {absolute left offset: cell text}.
    """
    # Reset horizontal scroll.
    driver.execute_script("arguments[0].scrollLeft = 0;", grid_container)
    #time.sleep(1)
    
    # Calculate offset_x if not provided.
    total_width = driver.execute_script("return arguments[0].scrollWidth;", grid_container)
    visible_width = driver.execute_script("return arguments[0].clientWidth;", grid_container)
    if offset_x is None:
        try:
            slider_track = driver.find_element(By.ID, "jqxScrollWraphorizontalScrollBarjqxgrid")
            slider_track_width = slider_track.size["width"]
            slider_thumb = driver.find_element(By.ID, "jqxScrollThumbhorizontalScrollBarjqxgrid")
            thumb_width = slider_thumb.size["width"]
            available_slider_range = slider_track_width - thumb_width
            if total_width == 0 or available_slider_range == 0:
                offset_x = visible_width
            else:
                optimal_offset = (visible_width * available_slider_range) / total_width
                offset_x = int(optimal_offset)
        except Exception as e:
            print("Error calculating optimal offset, falling back to visible_width:", e)
            offset_x = visible_width
    #print("Calculated offset_x:", offset_x)
    
    # Capture the initial segment.
    combined_headers = capture_headers()
    current_scroll = driver.execute_script("return arguments[0].scrollLeft;", grid_container)
    combined_data = capture_grid_data(grid_container, additional_offset=current_scroll)
    actions = ActionChains(driver)
    
    def get_thumb_left():
        try:
            thumb = driver.find_element(By.ID, "jqxScrollThumbhorizontalScrollBarjqxgrid")
            style = thumb.get_attribute("style")
            m = re.search(r'left:\s*(\d+)px', style)
            return int(m.group(1)) if m else 0
        except Exception as e:
            print("Error reading thumb left position:", e)
            return 0

    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        current_left = get_thumb_left()
        try:
            slider_thumb = driver.find_element(By.ID, "jqxScrollThumbhorizontalScrollBarjqxgrid")
            # Ensure slider thumb is in view
            driver.execute_script("arguments[0].scrollIntoView(true);", slider_thumb)
        except Exception as e:
            print("Slider thumb not found:", e)
            break
        try:
            actions.move_to_element(slider_thumb).click_and_hold().move_by_offset(offset_x, 0).release().perform()
        except Exception as e:
            print("Error moving slider thumb:", e)
            break
        time.sleep(1)  # Wait for new columns to load
        new_left = get_thumb_left()
        # Merge headers.
        combined_headers.update(capture_headers())
        # Capture grid data in current segment.
        current_scroll = driver.execute_script("return arguments[0].scrollLeft;", grid_container)
        segment = capture_grid_data(grid_container, additional_offset=current_scroll)
        for row_id, cells in segment.items():
            if row_id in combined_data:
                combined_data[row_id].update(cells)
            else:
                combined_data[row_id] = cells.copy()
        if new_left == current_left:
            break
    return combined_headers, combined_data

    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        current_left = get_thumb_left()
        try:
            slider_thumb = driver.find_element(By.ID, "jqxScrollThumbhorizontalScrollBarjqxgrid")
        except Exception as e:
            print("Slider thumb not found:", e)
            break
        actions.click_and_hold(slider_thumb).move_by_offset(offset_x, 0).release().perform()
        #time.sleep(1)  # wait for new columns to load
        new_left = get_thumb_left()
        # Merge headers from the new segment.
        combined_headers.update(capture_headers())
        # Capture grid data from the new segment.
        current_scroll = driver.execute_script("return arguments[0].scrollLeft;", grid_container)
        segment = capture_grid_data(grid_container, additional_offset=current_scroll)
        for row_id, cells in segment.items():
            if row_id in combined_data:
                combined_data[row_id].update(cells)
            else:
                combined_data[row_id] = cells.copy()
        if new_left == current_left:
            break
    return combined_headers, combined_data

# -------------------------------
# Checkpointing with pickle
# -------------------------------
CHECKPOINT_FILE = f"sp500_{report}.pkl"

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "rb") as f:
        checkpoint = pickle.load(f)
    fin_df = checkpoint.get("fin_df", pd.DataFrame())
    processed_tickers = checkpoint.get("processed_tickers", set())
    print(f"Resuming from checkpoint: {len(processed_tickers)} tickers processed.")
    count = len(processed_tickers)
else:
    fin_df = pd.DataFrame()
    processed_tickers = set()
    count = 0


# -------------------------------
# Main Processing Loop
# -------------------------------
# Base URL pattern for Macrotrends quarterly income statement pages
base_url = "https://www.macrotrends.net/stocks/charts/{}/a/{}?freq=Q"

for ticker in tickers:
    if ticker in processed_tickers:
        print(f"Skipping {ticker} as it's already processed.")
        continue
    url = base_url.format(ticker, report)
    driver.get(url)

    current_url = driver.current_url + "?freq=Q"
    driver.get(current_url)
    #time.sleep(1)
    # Optionally disable adblock pop-up
    try:
        disable_btn = WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Disable my adblocker')]"))
        )
        print(f"{ticker}: Adblocker disable button found; refreshing page.")
        driver.refresh()
        time.sleep(1)
    except Exception:
        pass
    
    grid_container = find_grid_container(driver)
    if grid_container is None:
        print(f"Skipping {ticker} due to missing grid container.")
        continue
        
    vertical_scroll(grid_container)
    headers_data, full_data = capture_all_table_data(driver, grid_container, offset_x=150)
    
    # Merge row data: sort row IDs by numeric part and then order cells by absolute left offset.
    def extract_row_number(row_id):
        m = re.search(r'row(\d+)', row_id)
        return int(m.group(1)) if m else 0
    sorted_row_ids = sorted(full_data.keys(), key=extract_row_number)
    final_rows = []
    for row_id in sorted_row_ids:
        cell_dict = full_data[row_id]
        sorted_keys = sorted(cell_dict.keys())
        row = [cell_dict[k] for k in sorted_keys]
        final_rows.append(row)
    
    sorted_headers = [headers_data[k] for k in sorted(headers_data.keys())]
    #print("Scraped Column Headers for", ticker, ":", sorted_headers)
    
    # Remove empty strings from each row.
    final_rows = [[item for item in row if item != ''] for row in final_rows]
    
    # Build DataFrame: if header length matches first row, use headers; otherwise, fallback.
    if sorted_headers and final_rows and len(sorted_headers) == len(final_rows[0]):
        df_ticker = pd.DataFrame(final_rows, columns=sorted_headers)
    else:
        print("Falling back to using first data row as header for", ticker)
        df_ticker = pd.DataFrame(final_rows[1:], columns=final_rows[0])
    df_ticker['Ticker'] = ticker
    
    # Optionally melt the DataFrame into long format.
    try:
        df_ticker = df_ticker.melt(id_vars=['Ticker', sorted_headers[0]])
    except Exception:
        df_ticker = df_ticker.melt(id_vars=['Ticker'])
    
    fin_df = pd.concat([fin_df, df_ticker], ignore_index=True)
    processed_tickers.add(ticker)
    print(f"{count}: {ticker} processed. Total records so far: {len(fin_df)}")
    count += 1
    
    # Save checkpoint after each ticker.
    with open(CHECKPOINT_FILE, "wb") as f:
        pickle.dump({"fin_df": fin_df, "processed_tickers": processed_tickers}, f)

driver.quit()
fin_df.to_csv(f"sp500_{report}.csv", index=False)
