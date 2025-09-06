"""
BacktestHelper.py
=================
Purpose:
--------
Automatically run Backtrader backtests using converted OHLCV datasets.

Features:
---------
- Accepts single or batch Backtrader PandasData objects.
- Supports multiple symbols/datasets in one Cerebro run.
- Configurable initial cash and commission.
- Prints basic results: final portfolio, PnL, number of trades.
- Optional plot of results.

Usage:
------
from BacktestHelper import run_backtest

# Single strategy
run_backtest(bt_objects['US30.csv'], MyStrategy, cash=10000, plot=True)

# Batch backtest
run_backtest(bt_objects.values(), MyStrategy, cash=50000, plot=False)
"""

import backtrader as bt

def run_backtest(data_objs, strategy_class, cash=10000, commission=0.0, plot=True):
    """
    Runs a Backtrader backtest.
    
    Parameters:
    -----------
    data_objs : single bt.feeds.PandasData or iterable of them
    strategy_class : class of Backtrader strategy
    cash : initial cash
    commission : commission per trade (fraction, e.g., 0.001 = 0.1%)
    plot : whether to plot results
    """
    cerebro = bt.Cerebro(stdstats=True)
    
    # Add strategy
    cerebro.addstrategy(strategy_class)
    
    # Add data
    if not hasattr(data_objs, '__iter__'):
        data_objs = [data_objs]
    
    for data in data_objs:
        cerebro.adddata(data)
    
    # Set cash and commission
    cerebro.broker.set_cash(cash)
    cerebro.broker.setcommission(commission=commission)
    
    # Run backtest
    print(f"Starting Portfolio Value: {cerebro.broker.getvalue():.2f}")
    results = cerebro.run()
    print(f"Final Portfolio Value: {cerebro.broker.getvalue():.2f}")
    
    # Optional plot
    if plot:
        cerebro.plot(style='candlestick')
    
    return results
