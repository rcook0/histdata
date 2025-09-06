"""
VectorBTHelper.py
=================
Purpose:
--------
Run vectorbt backtests on any OHLCV dataset (CSV or DataFrame), single or batch.

Features:
---------
- Accepts single or multiple datasets.
- Works on any symbol and timeframe.
- Auto-computes basic metrics: PnL, total return, max drawdown, number of trades, win rate.
- Returns results as a Pandas DataFrame.
- Can save summary CSV or plot equity curves.
- Fully generic, minute-level compatible.

Usage:
------
from VectorBTHelper import run_vectorbt_backtest

# Single dataset
results_df = run_vectorbt_backtest('US30.csv', strategy_func=my_signal_func)

# Batch datasets
files = ['US30.csv', 'BTCUSD.csv', 'EURUSD.csv']
results_df = run_vectorbt_backtest(files, strategy_func=my_signal_func, batch=True)
"""

import pandas as pd
import vectorbt as vbt

def run_vectorbt_backtest(data, strategy_func, cash=10000, batch=False, plot=False):
    """
    Runs a vectorbt backtest.
    
    Parameters:
    -----------
    data : str (CSV path), DataFrame, or list of these
    strategy_func : function(data_df) -> signals (1=buy, -1=sell, 0=hold)
    cash : starting capital
    batch : if True, `data` is a list of datasets
    plot : if True, plot equity curves
    
    Returns:
    --------
    Pandas DataFrame summarizing results for each dataset
    """
    
    if not batch:
        data_list = [data]
    else:
        data_list = data

    summary = []

    for dataset in data_list:
        # Load dataset
        if isinstance(dataset, str):
            df = pd.read_csv(dataset, parse_dates=True, index_col=0)
            name = dataset.split('/')[-1]
        else:
            df = dataset.copy()
            name = getattr(df, 'name', 'Dataset')
        
        # Ensure OHLCV columns exist
        for col in ['Open','High','Low','Close','Volume']:
            if col not in df.columns:
                raise ValueError(f"Missing column {col} in {name}")
        
        # Generate signals
        signals = strategy_func(df)  # Should return Series aligned with df
        
        # Run vectorbt portfolio
        pf = vbt.Portfolio.from_signals(
            close=df['Close'],
            entries=signals == 1,
            exits=signals == -1,
            init_cash=cash,
            fees=0.0
        )
        
        # Collect summary metrics
        summary.append({
            'Dataset': name,
            'Final Value': pf.total_value()[-1],
            'Total Return %': pf.total_return()[-1]*100,
            'Max Drawdown %': pf.max_drawdown()[-1]*100,
            'Number of Trades': len(pf.trades.records),
            'Win Rate %': pf.win_rate()[-1]*100
        })
        
        # Optional plot
        if plot:
            pf.plot().show()
    
    return pd.DataFrame(summary)
