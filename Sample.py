from BacktestHelper import run_backtest
from DataConverter_Backtest import convert_file_to_backtest_objects

# Convert file
bt_obj, _ = convert_file_to_backtest_objects('US30.csv', timeframe=1)

# Simple strategy
class MyStrategy(bt.Strategy):
    def next(self):
        if not self.position:
            self.buy(size=1)

# Run backtest
run_backtest(bt_obj, MyStrategy, cash=10000, plot=False)
