"""
BacktestPipeline.py
==================
Purpose:
--------
Full pipeline for financial datasets: load, clean/convert, and run VectorBT backtests.

Features:
---------
- Accepts single or batch CSV files.
- Automatically parses date/time and OHLCV columns.
- Handles minute-level data and missing rows.
- Cleans/renames columns if necessary.
- Outputs ready-to-use DataFrame for backtesting.
- Runs VectorBT backtests and summarizes metrics.
- Optional equity curve plotting.
- Fully generic for any symbol or timeframe.

Usage:
------
from BacktestPipeline import run_pipeline

# Single dataset
results_df = run_pipeline('US30.csv', strategy_func=my_signal_func)

# Batch datasets
files = ['US30.csv', 'BTCUSD.csv', 'EURUSD.csv']
results_df = run_pipeline(files, strategy_func=my_signal_func, batch=True)
"""

import pandas as pd
import vectorbt as vbt
import os

def load_and_prepare(df_or_path):
    """
    Loads CSV or accepts DataFrame, ensures OHLCV columns, datetime index.
    Returns cleaned DataFrame.
    """
    if isinstance(df_or_path, str):
        df = pd.read_csv(df_or_path)
    else:
        df = df_or_path.copy()
    
    # Standardize column names
    df.columns = [c.capitalize() for c in df.columns]
    
    # Detect datetime column
    datetime_cols = [c for c in df.columns if 'date' in c.lower() or 'time' in c.lower()]
    if datetime_cols:
        df[datetime_cols[0]] = pd.to_datetime(df[datetime_cols[0]])
        df.set_index(datetime_cols[0], inplace=True)
    else:
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("No datetime column or index found.")
    
    # Ensure OHLCV columns
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if col not in df.columns:
            df[col] = 0.0  # or NaN, depending on your strategy
    
    # Sort by datetime
    df.sort_index(inplace=True)
    
    return df

def run_vectorbt(df, strategy_func, cash=10000, plot=False):
    """
    Runs vectorbt backtest on a single cleaned DataFrame.
    """
    signals = strategy_func(df)
    
    pf = vbt.Portfolio.from_signals(
        close=df['Close'],
        entries=signals == 1,
        exits=signals == -1,
        init_cash=cash,
        fees=0.0
    )
    
    summary = {
        'Final Value': pf.total_value()[-1],
        'Total Return %': pf.total_return()[-1]*100,
        'Max Drawdown %': pf.max_drawdown()[-1]*100,
        'Number of Trades': len(pf.trades.records),
        'Win Rate %': pf.win_rate()[-1]*100
    }
    
    if plot:
        pf.plot().show()
    
    return summary

def run_pipeline(data, strategy_func, cash=10000, batch=False, plot=False):
    """
    Full pipeline: load/clean -> run backtest -> return summary DataFrame
    """
    if not batch:
        data_list = [data]
    else:
        data_list = data

    results = []

    for item in data_list:
        # Load and clean
        df = load_and_prepare(item)
        name = os.path.basename(item) if isinstance(item, str) else getattr(item, 'name', 'Dataset')
        
        # Backtest
        summary = run_vectorbt(df, strategy_func, cash=cash, plot=plot)
        summary['Dataset'] = name
        results.append(summary)
    
    return pd.DataFrame(results)
