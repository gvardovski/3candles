import sys
from getdata.getdata import make_csv
from backtesting.candles_hour import make_backtest_hour
from backtesting.candles_minute import make_backtest_minute
from backtesting.candles_optimization import make_backtest_minute_optimization

def user_make_decision():
    decision = input("\nWhat do you want to do?\n1 : Getdata\n2 : MAke backtest hour interval\n" +
                     "3 : MAke backtest minute interval\n4 : MAke backtest minute interval with optimization\n5 : Exit\n")
    decision = decision.strip()
    if decision not in ['1', '2', '3', '4', '5']:
        print("Please make correct choise!")
    elif decision == '1':
        make_csv()
    elif decision == '2':
        make_backtest_hour()
    elif decision == '3':
        make_backtest_minute()
    elif decision == '4':
        make_backtest_minute_optimization()
    elif decision == '5':
        sys.exit("\nBye Bye!")
    user_make_decision()

if __name__ == "__main__":
    user_make_decision()
