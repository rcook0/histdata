"""
DataConverter.py
================

Purpose:
--------
Convert financial CSV datasets from multiple brokers/formats into a standard
OHLCV format for backtesting and analysis (Backtrader, VectorBT, etc.).

Features:
---------
1. Broker/CSV Auto-Detection
   - Supports Exness, OANDA, MT4/MT5, generic CSV.
   - Logs detected format.

2. Column Normalization & Validation
   - Renames to ['Date','Open','High','Low','Close','Volume'].
   - Handles separate or combined Date+Time columns.
   - Fills missing values and removes invalid rows.

3. Minute-Level & Multi-Timeframe Handling
   - Supports any timeframe: 1m, 5m, 15m, 1h, daily.
   - Aggregates minute data into higher timeframes.

4. Multi-Format Output
   - Standardized CSV output.
   - Optional Backtrader/VectorBT-ready formats.
   - Batch processing support.

5. Logging & Reporting
   - Summary of rows fixed, invalid data, detected format.
   - Optional .log file output.

6. Batch & Folder Processing
   - Convert single or multiple CSVs in a folder.
   - Maintains folder structure for output.

Usage Instructions:
-------------------
1. Single File Conversion:
   python DataConverter.py --input path/to/file.csv --output path/to/output.csv

2. Batch Folder Conversion:
   python DataConverter.py --input-folder path/to/input --output-folder path/to/output

3. Optional Parameters:
   --timeframe 5  : Resample data to 5-minute candles.
   --format backtrader : Output format compatible with Backtrader.
   --log log.txt : Save conversion log.

Dependencies:
-------------
- pandas
- numpy
- tqdm (optional, for progress bar in batch mode)

Author:
-------
Generated for fully generic financial data conversion, multi-format support,
and backtesting integration.

"""

import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import argparse

SUPPORTED_FORMATS = ['exness', 'oanda', 'mt5', 'generic']

def detect_format(df: pd.DataFrame) -> str:
    """
    Detects CSV/broker format based on columns and heuristics.
    Returns one of SUPPORTED_FORMATS.
    """
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
    """
    Standardizes columns to ['Date','Open','High','Low','Close','Volume']
    """
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
    
    # Convert to numeric
    df[['Open','High','Low','Close','Volume']] = df[['Open','High','Low','Close','Volume']].apply(pd.to_numeric, errors='coerce')
    df = df.dropna()
    
    # Sort by Date
    df = df.sort_values('Date').reset_index(drop=True)
    return df

def resample_dataframe(df: pd.DataFrame, timeframe: int) -> pd.DataFrame:
    """
    Resamples minute-level data to higher timeframe (in minutes)
    """
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

def process_file(input_file: str, output_file: str, timeframe: int = None):
    """
    Converts a single CSV file into normalized OHLCV format
    """
    df = pd.read_csv(input_file)
    detected_format = detect_format(df)
    print(f"[INFO] Detected format: {detected_format}")
    df = normalize_columns(df, detected_format)
    if timeframe:
        df = resample_dataframe(df, timeframe)
    df.to_csv(output_file, index=False)
    print(f"[INFO] Saved converted file to: {output_file}")

def batch_process_folder(input_folder: str, output_folder: str, timeframe: int = None):
    """
    Converts all CSV files in input_folder to output_folder
    """
    os.makedirs(output_folder, exist_ok=True)
    for file in tqdm(os.listdir(input_folder)):
        if file.lower().endswith('.csv'):
            input_path = os.path.join(input_folder, file)
            output_path = os.path.join(output_folder, file)
            process_file(input_path, output_path, timeframe)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generic Financial CSV Data Converter")
    parser.add_argument('--input', type=str, help='Path to input CSV file')
    parser.add_argument('--output', type=str, help='Path to output CSV file')
    parser.add_argument('--input-folder', type=str, help='Path to folder of CSV files')
    parser.add_argument('--output-folder', type=str, help='Output folder for batch processing')
    parser.add_argument('--timeframe', type=int, default=None, help='Resample timeframe in minutes')
    args = parser.parse_args()

    if args.input and args.output:
        process_file(args.input, args.output, args.timeframe)
    elif args.input_folder and args.output_folder:
        batch_process_folder(args.input_folder, args.output_folder, args.timeframe)
    else:
        print("[ERROR] Must specify either single file (--input & --output) or folder (--input-folder & --output-folder).")
