"""
DataConverter_Backtest.py
=========================

Purpose:
--------
Convert financial CSV datasets into standardized OHLCV format AND produce
ready-to-use Backtrader and VectorBT objects for instant backtesting.

Features:
---------
- Generic multi-broker CSV support (Exness, OANDA, MT5, generic).
- Column normalization: ['Date','Open','High','Low','Close','Volume'].
- Minute-level resampling to any timeframe.
- Batch folder processing.
- Direct Backtrader and VectorBT objects in memory.
- Optional CSV output for archival.

Usage:
------
1. Single file to objects:
   df_bt, df_vbt = convert_file_to_backtest_objects('file.csv', timeframe=5)

2. Batch folder to objects:
   bt_objects, vbt_objects = batch_convert_folder_to_objects('input_folder', timeframe=1)

3. Optional CSV export:
   convert_file_to_backtest_objects('file.csv', 'output.csv', timeframe=5)

Dependencies:
-------------
- pandas
- numpy
- backtrader
- vectorbt
- tqdm (optional)

Author:
-------
Generated for fully generic financial data conversion with direct backtesting integration.
"""

import os
import pandas as pd
import numpy as np
from tqdm import tqdm

# Backtrader import
import backtrader as bt
# VectorBT import
import vectorbt as vbt

SUPPORTED_FORMATS = ['exness', 'oanda', 'mt5', 'generic']

def detect_format(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    if 'open' in cols and 'close' in cols and 'volume' in cols:
        if 'tickqty' in cols or 'spread' in cols:
            return 'mt5'
        elif 'bid' in cols and 'ask' in cols:
            return 'oanda'
        else:
            return 'exness'
    return 'generic'

def normalize_columns(df: pd.DataFrame, detected_format: str) -> pd.DataFrame:
    df = df.copy()
    cols = [c.lower() for c in df.columns]

    # Handle Date/Time
    if 'date' in cols and 'time' in cols:
        df['Date'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    elif 'datetime' in cols:
        df['Date'] = pd.to_datetime(df['datetime'])
    elif 'date' in cols:
        df['Date'] = pd.to_datetime(df['date'])
    else:
        raise ValueError("No recognizable Date column.")

    # Normalize OHLCV
    mapping = {}
    for col in cols:
        if 'open' in col: mapping['Open'] = col
        if 'high' in col: mapping['High'] = col
        if 'low' in col: mapping['Low'] = col
        if 'close' in col: mapping['Close'] = col
        if 'volume' in col: mapping['Volume'] = col

    df = df.rename(columns=mapping)
    df = df[['Date','Open','High','Low','Close','Volume']]
    df[['Open','High','Low','Close','Volume']] = df[['Open','High','Low','Close','Volume']].apply(pd.to_numeric, errors='coerce')
    df = df.dropna()
    df = df.sort_values('Date').reset_index(drop=True)
    return df

def resample_dataframe(df: pd.DataFrame, timeframe: int) -> pd.DataFrame:
    df = df.copy()
    df.set_index('Date', inplace=True)
    df_resampled = df.resample(f'{timeframe}T').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }).dropna().reset_index()
    return df_resampled

def convert_file_to_backtest_objects(input_file: str, output_csv: str = None, timeframe: int = None):
    """
    Converts a CSV into:
    - Backtrader PandasData object
    - VectorBT OHLCV DataFrame
    Optionally saves to CSV.
    """
    df = pd.read_csv(input_file)
    fmt = detect_format(df)
    df = normalize_columns(df, fmt)
    if timeframe:
        df = resample_dataframe(df, timeframe)
    if output_csv:
        df.to_csv(output_csv, index=False)
    
    # Backtrader
    df_bt = bt.feeds.PandasData(dataname=df.set_index('Date'))
    
    # VectorBT
    df_vbt = df.set_index('Date')
    
    return df_bt, df_vbt

def batch_convert_folder_to_objects(input_folder: str, timeframe: int = None):
    """
    Converts all CSVs in a folder to Backtrader and VectorBT objects.
    Returns dictionaries: {filename: object}
    """
    bt_objects = {}
    vbt_objects = {}
    for file in tqdm(os.listdir(input_folder)):
        if file.lower().endswith('.csv'):
            path = os.path.join(input_folder, file)
            bt_obj, vbt_obj = convert_file_to_backtest_objects(path, timeframe=timeframe)
            bt_objects[file] = bt_obj
            vbt_objects[file] = vbt_obj
    return bt_objects, vbt_objects
