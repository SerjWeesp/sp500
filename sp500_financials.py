# -*- coding: utf-8 -*-
"""
S&P 500 financials scraper with robust resume:
- state_pkl includes {timestamp}
- success-only advancement of report_idx/counter
- auto-restart WebDriver on 'invalid session id'
"""

import os
import time
import pickle
import warnings
from io import StringIO
from datetime import datetime
import ctypes
import pandas as pd
from getpass import getpass

# Selenium imports for Edge
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.keys import Keys

warnings.filterwarnings("ignore", category=DeprecationWarning)

# -------------------------------
# Config & paths
# -------------------------------
DATA_DIR = r"D:\GitHub\sp500"
os.makedirs(DATA_DIR, exist_ok=True)
EMAIL = os.getenv("APP_EMAIL") or input("Email: ")
PASSWORD = os.getenv("APP_PASSWORD") or getpass("Password: ")

run_stamp = datetime.now().strftime("%d%m%Y")  # {timestamp}
names_csv = os.path.join(DATA_DIR, f"sp500_names_{run_stamp}.csv")
state_pkl = os.path.join(DATA_DIR, f"sp500_financials_state_{run_stamp}.pkl")   # timestamped
final_csv = os.path.join(DATA_DIR, f"sp500_financials_{run_stamp}.csv")

edge_driver_path = r"C:\Users\Dell\Downloads\msedgedriver.exe"
page_wait_seconds = 20
max_retries = 3

ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001) # tell Windows to stay awake

# Reports to scrape (order matters)
reports = ["", "balance-sheet", "cash-flow-statement", "ratios"]

# -------------------------------
# Utilities
# -------------------------------
def init_driver():
    opts = EdgeOptions()
    opts.page_load_strategy = "eager"
    # opts.add_argument("--headless=new")  # enable if you want headless
    service = EdgeService(executable_path=edge_driver_path)
    drv = webdriver.Edge(service=service, options=opts)
    w = WebDriverWait(drv, page_wait_seconds)
    return drv, w

def save_state(df, counter, report_idx, last_ticker, completed_pairs):
    tmp_path = state_pkl + ".tmp"
    state = {
        "df": df,
        "counter": int(counter),
        "report_idx": int(report_idx),
        "last_ticker": last_ticker,
        "completed_pairs": list(completed_pairs),
    }
    with open(tmp_path, "wb") as f:
        pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp_path, state_pkl)
    rpt_name = reports[report_idx] if 0 <= report_idx < len(reports) else "done"
    print(f"[STATE SAVED] counter={counter}, ticker={last_ticker}, report_idx={report_idx} ({rpt_name})")

def is_invalid_session(e: Exception) -> bool:
    return "invalid session id" in str(e).lower()

# -------------------------------
# Load tickers
# -------------------------------
sp500_names = pd.read_csv(names_csv)
tickers = sp500_names["Symbol"].astype(str).tolist()

# -------------------------------
# Load or init state
# -------------------------------
if os.path.exists(state_pkl):
    with open(state_pkl, "rb") as f:
        state = pickle.load(f)
    df = state.get("df", pd.DataFrame())
    saved_counter = int(state.get("counter", 0))
    report_idx = int(state.get("report_idx", 0))
    last_ticker = state.get("last_ticker", tickers[saved_counter] if saved_counter < len(tickers) else None)
    completed_pairs = set(state.get("completed_pairs", []))

    # Recompute counter from last_ticker if possible to avoid order mismatches
    if last_ticker in tickers:
        counter = tickers.index(last_ticker)
    else:
        counter = max(0, min(saved_counter, len(tickers) - 1))
        last_ticker = tickers[counter] if tickers else None

    print(f"Resuming from ticker index {counter} ({last_ticker}), report index {report_idx}.")
else:
    df = pd.DataFrame()
    counter = 0
    report_idx = 0
    last_ticker = tickers[0] if tickers else None
    completed_pairs = set()
    print("No existing state found. Starting fresh.")

# -------------------------------
# Selenium driver
# -------------------------------
url = f"https://stockanalysis.com/stocks/aapl"
driver, wait = init_driver()
driver.get(url)
driver.maximize_window()

# Cookie banner 
time.sleep(1)
try:
    btn = driver.find_element(
        By.XPATH,
        "/html/body/div[2]/div[2]/div[2]/div[2]/div[2]/button[1]/p",
    )
    btn.click()
    
except NoSuchElementException:
    pass
except Exception:
    pass

login = driver.find_element(
    By.XPATH,
    "/html/body/div/header/div/div[2]/a[1]",
)
login.click()
time.sleep(1)
email = driver.find_element(
    By.XPATH,
    "/html/body/div/div[1]/div[2]/main/div/form/input[1]",
)
email.send_keys(EMAIL)
password_box = driver.find_element(By.XPATH, "/html/body/div/div[1]/div[2]/main/div/form/input[2]")
password_box.send_keys(PASSWORD)
password_box.send_keys(Keys.RETURN)

# -------------------------------
# Main scraping loop
# -------------------------------
try:
    while counter < len(tickers):
        ticker = tickers[counter]
        last_ticker = ticker
        print(f"== Ticker {counter+1}/{len(tickers)}: {ticker} ==")

        # Iterate reports starting from current report_idx
        r = report_idx
        while r < len(reports):
            report = reports[r]
            key = (ticker, report)

            # Skip if already completed in this session run (idempotence)
            if key in completed_pairs:
                print(f"Skip already completed: {ticker} {report}")
                r += 1
                continue

            url = f"https://stockanalysis.com/stocks/{ticker}/financials/{report}"

            attempt = 0
            success = False
            while attempt <= max_retries and not success:
                try:
                    driver.get(url)
                    # Cookie banner 
                    time.sleep(0.5)
                    try:
                        btn = driver.find_element(
                            By.XPATH,
                            "/html/body/div[2]/div[2]/div[2]/div[2]/div[2]/button[1]/p",
                        )
                        btn.click()
                    except NoSuchElementException:
                        pass
                    except Exception:
                        pass

                    # Switch to "Quarters"
                    quarters_btn = wait.until(
                        EC.element_to_be_clickable(
                            (By.XPATH, "/html/body/div/div[1]/div[2]/main/div[2]/nav[2]/ul/li[2]/button")
                        )
                    )
                    quarters_btn.click()
                    time.sleep(3)

                    # Read table
                    table_elem = driver.find_element(By.XPATH, '//*[@id="main-table"]')
                    table_html = table_elem.get_attribute("outerHTML")

                    buffer = StringIO(table_html)
                    ticker_df = pd.read_html(buffer)[0]
                    ticker_df.set_index(ticker_df.columns[0], inplace=True)

                    # Long format
                    long = ticker_df.stack(level=[0, 1], future_stack=True).reset_index(name="Value")
                    long.columns = ["Metric", "Fiscal Quarter", "Period Ending", "Value"]
                    long = long[long["Value"] != "Upgrade"]
                    long["Ticker"] = ticker

                    # Append to in-memory df (as in your original approach)
                    df = pd.concat([df, long], ignore_index=True)
                    completed_pairs.add(key)

                    print(f"SUCCESS: {ticker} {report} | rows_added={len(long)} | total_rows={len(df)}")

                    # SUCCESS ONLY: advance report index and save
                    r += 1
                    report_idx = r  # next report to attempt for this ticker
                    save_state(df, counter, report_idx, ticker, completed_pairs)
                    success = True

                except (TimeoutException, NoSuchElementException) as e:
                    attempt += 1
                    print(f"{type(e).__name__} on {ticker} {report} attempt {attempt}/{max_retries}")
                    time.sleep(2)
                    if attempt > max_retries:
                        print(f"Giving up this ticker due to repeated timeouts: {ticker}")
                        # Move to next ticker; do not mark current report as done
                        report_idx = 0
                        counter += 1
                        save_state(df, counter, report_idx, ticker, completed_pairs)
                        break  # break retry loop -> proceed to next ticker

                except WebDriverException as e:
                    # Handle 'invalid session id' by restarting driver and retrying same report
                    if is_invalid_session(e):
                        print(f"WebDriverException (invalid session) on {ticker} {report}. Restarting driver...")
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver, wait = init_driver()
                        attempt += 1
                        time.sleep(1)
                        # Do NOT advance r/report_idx/counter here
                        continue
                    else:
                        # Unknown WebDriver error: give up this ticker conservatively
                        print(f"WebDriverException on {ticker} {report}: {e}")
                        report_idx = 0
                        counter += 1
                        save_state(df, counter, report_idx, ticker, completed_pairs)
                        break  # proceed to next ticker

                except Exception as e:
                    attempt += 1
                    print(f"Error on {ticker} {report}: {e} (attempt {attempt}/{max_retries})")
                    time.sleep(1)
                    if attempt > max_retries:
                        # After repeated generic errors, skip this ticker
                        print(f"Skipping ticker due to repeated errors: {ticker}")
                        report_idx = 0
                        counter += 1
                        save_state(df, counter, report_idx, ticker, completed_pairs)
                        break

            # If we gave up the ticker inside retry loop, break out of report loop
            if counter < len(tickers) and tickers[counter] != ticker:
                # counter was incremented; move to next ticker
                break

        # If we completed all reports for this ticker successfully, move to next ticker
        if r >= len(reports) and tickers[counter] == ticker:
            report_idx = 0
            counter += 1
            save_state(df, counter, report_idx, ticker, completed_pairs)

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    # Final CSV export (as in your original)
    try:
        df.to_csv(final_csv, index=False)
        print(f"Final CSV saved to: {final_csv}")
    except Exception as e:
        print(f"Failed to save final CSV: {e}")

    try:
        driver.quit()
    except Exception:
        pass
    
# restore normal sleep behavior
ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)