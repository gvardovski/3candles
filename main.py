import sys
from getdata.getdata import make_csv
from backtesting.candles_hour import make_backtest_hour

def user_make_decision():
    decision = input(
        "\nWhat do you want to do?\n1 : Getdata\n2 : MAke backtest hour interval\n3 : MAke backtest minute interval\n4 : Exit\n")
    decision = decision.strip()
    if decision not in ["1", "2", "3", "4"]:
        print("Please make correct choise!")
    elif decision == "1":
        make_csv()
    elif decision == "2":
        make_backtest_hour()
    elif decision == "3":
        pass
    elif decision == "4":
        sys.exit("\nBye Bye!")
    user_make_decision()

if __name__ == "__main__":
    user_make_decision()
