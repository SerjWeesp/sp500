# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 19:11:53 2025

@author: Dell
"""

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import ta
import ctypes
from datetime import datetime

run_stamp = datetime.now().strftime("%d%m%Y")  # {timestamp}
prices_path = "sp500_prices_04012026.csv"
financials_path = "sp500_financials_03012026.csv"
financials_0_path = "sp500_financials_01092025.csv"
names_path = "sp500_names_03012026.csv"

# tell Windows to stay awake
ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001)

# Functions 

def restructure_df(df):
    """
    Restructures the input DataFrame to move 'Quarter' and 'Fiscal year'
    values into the 'Metric' column.

    Args:
        df (pd.DataFrame): Input DataFrame with columns 'Ticker', 'Date', 'Metric',
                           'Value', 'Quarter', and 'Fiscal year'.

    Returns:
        pd.DataFrame: Restructured DataFrame with columns 'Ticker', 'Date', 'Metric', 'Value'.
    """

    # Melt the DataFrame to combine 'Quarter' and 'Fiscal year' into 'Metric'
    df_melted = pd.melt(
        df,
        id_vars=['Ticker', 'Date', 'Metric', 'Value'],
        value_vars=['Quarter', 'Fiscal year'],
        var_name='New_Metric',
        value_name='New_Value'
    )

    # Create a new DataFrame from the original 'Metric' and 'Value' columns
    df_base = df_melted[['Ticker', 'Date', 'Metric', 'Value']].dropna()

    # Create a new DataFrame from the 'New_Metric' and 'New_Value' columns
    df_new = df_melted[['Ticker', 'Date', 'New_Metric', 'New_Value']].rename(
        columns={'New_Metric': 'Metric', 'New_Value': 'Value'}
    ).dropna()

    # Concatenate the two DataFrames and return the result
    return pd.concat([df_base, df_new], ignore_index=True)[['Ticker', 'Date', 'Metric', 'Value']]

def compute_quarter_metrics(df, dates):
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"].astype(str).str[:10], errors="coerce")
    df.sort_values(["Company", "Date"], inplace=True, kind="mergesort")

    results = []

    for ticker, sub in df.groupby("Company", sort=False):
        sub = sub.loc[sub["Date"].notna()]
        if sub.empty:
            continue

        dates_arr = sub["Date"].to_numpy(dtype="datetime64[ns]")

        # Use float64 accumulators to reduce cancellation
        close = sub["Close"].to_numpy(dtype=np.float64)
        vol   = sub["Volume"].to_numpy(dtype=np.float64)
        div   = sub["Dividends"].to_numpy(dtype=np.float64)

        macd = ta.trend.MACD(close=sub["Close"], window_slow=60, window_fast=30, window_sign=30)
        macd_line   = macd.macd().to_numpy()
        macd_signal = macd.macd_signal().to_numpy()
        macd_hist   = macd.macd_diff().to_numpy()
        rsi = ta.momentum.RSIIndicator(close=sub["Close"], window=60).rsi().to_numpy()

        # cumulative sums in float64
        csum_close  = np.cumsum(close)
        csum_close2 = np.cumsum(close * close)
        csum_vol    = np.cumsum(vol)
        csum_vol2   = np.cumsum(vol * vol)
        csum_div    = np.cumsum(div)
        csum_div_nz = np.cumsum((div != 0).astype(np.int64))

        s_dates = pd.Series(dates_arr)
        mask_targets = s_dates.dt.strftime("%m-%d").isin(dates)
        if not mask_targets.any():
            continue
        target_end_pos = s_dates[mask_targets].groupby(s_dates[mask_targets]).tail(1).index.to_numpy()

        # small helper
        def _range(csum, ei, si):
            return csum[ei] - (csum[si-1] if si > 0 else 0.0)

        for ei in target_end_pos:
            end_date = dates_arr[ei]
            if pd.isna(end_date):
                continue

            start_date = pd.Timestamp(end_date).replace(day=1) - relativedelta(months=2)
            si = dates_arr.searchsorted(np.datetime64(start_date, "ns"), side="left")
            if si > ei:
                continue

            cnt = ei - si + 1
            if cnt <= 0:
                continue

            # price stats
            sum_close  = _range(csum_close,  ei, si)
            sum_close2 = _range(csum_close2, ei, si)
            mean_close = sum_close / cnt

            if cnt > 1:
                # sample variance via cumsums
                var_num   = sum_close2 - (sum_close * sum_close) / cnt
                var_close = var_num / (cnt - 1)

                # clamp tiny negatives caused by FP error
                if var_close < 0:
                    if var_close > -1e-12:
                        var_close = 0.0
                    else:
                        var_close = np.nan
                std_close = np.sqrt(var_close) if np.isfinite(var_close) else np.nan
            else:
                std_close = np.nan

            w_close = close[si:ei+1]
            median_close = float(np.median(w_close))
            min_close    = float(np.min(w_close))
            max_close    = float(np.max(w_close))

            # volume stats
            sum_vol  = _range(csum_vol,  ei, si)
            sum_vol2 = _range(csum_vol2, ei, si)
            mean_vol = sum_vol / cnt
            if cnt > 1:
                var_num_v = sum_vol2 - (sum_vol * sum_vol) / cnt
                var_vol   = var_num_v / (cnt - 1)
                if var_vol < 0:
                    if var_vol > -1e-12:
                        var_vol = 0.0
                    else:
                        var_vol = np.nan
                std_vol = np.sqrt(var_vol) if np.isfinite(var_vol) else np.nan
            else:
                std_vol = np.nan

            w_vol = vol[si:ei+1]
            median_vol = float(np.median(w_vol))
            min_vol    = int(np.min(w_vol))
            max_vol    = int(np.max(w_vol))

            # dividends
            sum_div      = float(_range(csum_div, ei, si))
            cnt_div_nz   = int(_range(csum_div_nz, ei, si))
            mean_div_ex_zero = (sum_div / cnt_div_nz) if cnt_div_nz > 0 else 0.0

            results.append({
                "Ticker": ticker,
                "Date": pd.Timestamp(end_date),
                "ClosePrice": float(close[ei]),
                "MinPrice": min_close,
                "MaxPrice": max_close,
                "StdPrice": float(std_close),
                "MeanPrice": float(mean_close),
                "MedianPrice": median_close,
                "MinVolume": min_vol,
                "MaxVolume": max_vol,
                "StdVolume": float(std_vol),
                "MeanVolume": float(mean_vol),
                "MedianVolume": median_vol,
                "SumDividends": sum_div,
                "MeanDividends": float(mean_div_ex_zero),
                "CountDividends": cnt_div_nz,
                "RSI": float(rsi[ei]),
                "MACD": float(macd_line[ei]),
                "MACD_Signal": float(macd_signal[ei]),
                "MACD_Hist": float(macd_hist[ei]),
            })

    return pd.DataFrame(results)

def clean_numeric_column(series, percent_to_decimal=True):
    """
    Clean a Series of messy numeric strings and return numeric dtype.
    If percent_to_decimal=True, values ending with '%' are converted to decimals (e.g. '12%' -> 0.12).
    """
    s = series.astype(str).str.strip()

    # remove $ and commas
    s = s.str.replace(r'[\$,]', '', regex=True)

    # convert parentheses to negative numbers: "(1,234)" -> "-1234"
    s = s.str.replace(r'^\((.*)\)$', r'-\1', regex=True)

    # Optionally convert percent strings to decimal
    if percent_to_decimal:
        pct_mask = s.str.endswith('%')
        # strip '%' and convert the rest
        numeric = pd.to_numeric(s.str.rstrip('%'), downcast='float', errors='coerce')
        numeric.loc[pct_mask] = numeric.loc[pct_mask] / 100.0
        return numeric

def calc_quarterly_pct_diff(df, ticker_col='ticker', date_col='date', lags=[1, 4]):
    """
    Calculate the percentage change for each numeric column by ticker between periods 
    for given lags. For each numeric column, it computes the percent change for each lag 
    and appends new columns named "<col>_pct_diff_<lag>".

    The first row for each ticker is NaN for each lag since there's no previous period.

    Args:
        df (pd.DataFrame): The input DataFrame.
        ticker_col (str): Column name for ticker identifiers.
        date_col (str): Column name for date/time variable.
        lags (list): List of lag values (in periods) for which to compute the percent change.

    Returns:
        pd.DataFrame: A copy of 'df' with new percentage change columns appended.
    """
    # Sort by ticker/date to ensure correct chronological order
    df = df.sort_values(by=[ticker_col, date_col]).copy()

    # Identify numeric columns (exclude ticker/date from numeric, if present)
    numeric_cols = df.select_dtypes(
        include=[np.number]).columns.difference([ticker_col, date_col])

    # Build a dict of new columns; each key corresponds to a new Series
    new_cols = {}

    # For each numeric column, compute percent change for each specified lag
    for col in numeric_cols:
        for lag in lags:
            new_col_name = f"{col}_pct_diff_{lag}"
            new_cols[new_col_name] = df.groupby(
                ticker_col)[col].pct_change(periods=lag)

    # Convert the dict to a DataFrame, aligning on the original index, then concat once
    df_new = pd.DataFrame(new_cols, index=df.index)
    df = pd.concat([df, df_new], axis=1)

    return df

# Import

prices = pd.read_csv(prices_path, index_col=0, parse_dates=True, low_memory=False)
prices.rename(columns={"index": "Date"}, inplace=True)

names = pd.read_csv(names_path)

# Merging old data with new and rremove duplicates
financials = pd.read_csv(financials_path, low_memory=False)
financials_0 = pd.read_csv(financials_0_path, low_memory=False)
financials = pd.concat([financials, financials_0], axis=0,
                       ignore_index=True).drop_duplicates()

financials.loc[:, ['Ticker', 'Metric', 'Fiscal Quarter']
               ].drop_duplicates(inplace=True)

# Formatting string and dates columns
financials_clean = financials[~financials['Period Ending'].str.contains(
    'Quarters')]
financials_clean = financials_clean[financials_clean['Fiscal Quarter'].str.contains(
    'Q')]
financials_clean = financials_clean[financials_clean['Fiscal Quarter'] != 'Current']
financials_clean.fillna(0, inplace=True)
financials_clean['Date'] = pd.to_datetime(
    financials_clean['Period Ending'].str[8:], format='%b %d, %Y', errors='coerce')
financials_clean.drop(columns=['Period Ending'], inplace=True)
# financials_clean.drop(columns=['Unnamed: 0'], inplace=True)
financials_clean[['Quarter', 'Fiscal year']
                 ] = financials_clean['Fiscal Quarter'].str.split(' ', expand=True)
financials_clean['Quarter'] = financials_clean['Quarter'].str[1:].astype(
    int)        # Remove 'Q' and convert to int
financials_clean['Fiscal year'] = financials_clean['Fiscal year'].astype(
    int)        # Convert year to int
financials_clean.drop(columns=['Fiscal Quarter'], inplace=True)

# Move Quarter/Fiscal year into Metric/Value rows so financials are long-form.
financials_fiscal = restructure_df(financials_clean)
financials_fiscal.columns = ['Ticker', 'Date', 'Variable', 'Value']
financials_fiscal.reset_index(drop=True, inplace=True)

# Derive the list of dates
dates = financials_fiscal['Date'].astype('str').str[5:].unique()

# Aggregate price values and reshaping into long format
prices_agg = compute_quarter_metrics(prices, dates)
prices_agg_melt = prices_agg.melt(id_vars=['Ticker', 'Date'])
prices_agg_melt.columns = ['Ticker', 'Date', 'Variable', 'Value']

# Concatenate financial nad proce data
sp500_merged = pd.concat([financials_fiscal, prices_agg_melt], axis=0)
sp500_merged = (sp500_merged
                .sort_values(['Ticker','Date','Variable'])
                .drop_duplicates(['Ticker','Date','Variable'], keep='last'))
sp500_merged['Value'] = clean_numeric_column(sp500_merged['Value'])
sp500_merged['Date'] = sp500_merged['Date'].astype('str').str[:10]

# Reshaping merged df to wide format and drop duplicates
sp500_wide = sp500_merged[['Ticker', 'Date', 'Variable', 'Value']].pivot_table(
    index=["Ticker", "Date"], columns="Variable", values="Value", observed=True, aggfunc='first')
sp500_wide.reset_index(inplace=True)

sp500_wide_clean = sp500_wide.dropna(
    subset=['Shareholders\' Equity', 'ClosePrice'])
sp500_df = sp500_wide_clean.fillna(0)

# Adding meta data for a ticker
sp500_df_names = sp500_df.merge(
    names[['Symbol', 'GICS Sector', 'GICS Sub-Industry', 'Founded']], how='left', left_on='Ticker', right_on='Symbol')

# Calculate 1st and 4th differences and tidying the final df
sp500_diff = calc_quarterly_pct_diff(
    sp500_df_names, ticker_col='Ticker', date_col='Date')
sp500_diff.dropna(subset=['ClosePrice_pct_diff_1',
                  'ClosePrice_pct_diff_4'], inplace=True)
sp500_diff.fillna(0, inplace=True)
sp500_diff.drop(columns=['Symbol'], axis=1, inplace=True)
sp500_diff['Founded'] = sp500_diff['Founded'].str[0:4].astype('int')

# Save result as CSV
sp500_diff.to_csv(f'D:/GitHub/sp500/sp500_diff_{run_stamp}.csv')

# restore normal sleep behavior
ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)