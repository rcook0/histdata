import pandas as pd
from BacktestPipeline import run_pipeline

# Example simple strategy
def my_signal_func(df):
    # Buy if close > 20-period moving average
    # Sell if close < 20-period moving average
    ma = df['Close'].rolling(20).mean()
    signals = pd.Series(0, index=df.index)
    signals[df['Close'] > ma] = 1
    signals[df['Close'] < ma] = -1
    return signals

# Single file
results = run_pipeline('US30.csv', my_signal_func, cash=10000)
print(results)

# Batch files
files = ['US30.csv', 'BTCUSD.csv', 'EURUSD.csv']
results = run_pipeline(files, my_signal_func, batch=True, cash=50000)
print(results)
