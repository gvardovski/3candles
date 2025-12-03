from data import getdata
from dotenv import load_dotenv
from typing import Optional
import os
import yaml
import pandas as pd
import vectorbt as vbt
import numpy as np

def check_config(config):
    try:
        size = config['Trade']['size']
        size_type = config['Trade']['size_type']
        fees = config['Broker']['fees']
        fixed_fees = config['Broker']['fixed_fees']
        slippage = config['Slippage']
        init_cash = config['Initial_cash']
        freq = config['Frequency']
    except KeyError as e:
        exit(f"Your Configuration file is missing a key: {e}\nPlease, check your configuration file.")
    if size <= 0 or not isinstance(size, int):
        exit("Trade size must be a positive number.")
    if size_type not in ['amount', 'percent', 'value']:
        exit("Trade size_type must be either 'amount' 'percent' or 'value'.")
    if not isinstance(fees, (int, float)) or not isinstance(fixed_fees, (int, float)):
        exit("Broker fees must be a number.")
    if not (0 <= fees <= 100):
        exit("Broker fees must be between 0 and 100.")
    if fixed_fees < 0:
        exit("Broker fixed_fees must be a non-negative number.")
    if slippage < 0 or not isinstance(slippage, (int, float)):
        exit("Slippage must be a non-negative number.")
    if init_cash < 0 or not isinstance(slippage, (int, float)):
        exit("Initial_cash must be a non-negative number.")
    if not isinstance(freq, str):
        exit("Frequency must be a string like ('1h', '15min').")

def check_if_csv_file_exist(config):
    if os.path.exists(config['Data_filename']):
        print("Found your Data file!")
    else:
        print(f"Your CSV file '{config['Data_filename']}' doesn't exist!\nCSV file '{config['Data_filename']}' will be created automatically!")
        config['Data_filename'] = getdata.make_csv()
    return config

if __name__ == "__main__":

    load_dotenv()

    config_path = os.getenv('3CANDLES_CONFIG_PATH')
    getdata.check_if_config_file_exist(config_path, 2)

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
        check_config(config)
        config = check_if_csv_file_exist(config)

    df_hour = pd.read_csv(config['Data_filename'])
    df_hour = df_hour.set_index('Time')
    df_hour.index = pd.to_datetime(df_hour.index)
    
    df_hour = df_hour.drop(columns=['Volume'])
    df_hour['Dir'] = 0
    df_hour.loc[df_hour['Close'] > df_hour['Open'], 'Dir'] = 1
    bull_entry_mask = ((df_hour['Dir'] == 1) & (df_hour['Dir'].shift(1) == 1) & (df_hour['Dir'].shift(2) == 1) &
                       (df_hour['Close'].shift(2) < df_hour['Open']))
    df_hour['Bull Entry'] = bull_entry_mask
    bear_entry_mask = ((df_hour['Dir'] == 0) & (df_hour['Dir'].shift(1) == 0) & (df_hour['Dir'].shift(2) == 0) &
                       (df_hour['Close'].shift(2) > df_hour['Open']))
    df_hour['Bear Entry'] = bear_entry_mask

    df_hour.loc[df_hour['Bull Entry'] == True, 'SL'] = df_hour['Close'].shift(2)
    df_hour.loc[df_hour['Bear Entry'] == True, 'SL'] = df_hour['Close'].shift(2)

    df_hour.loc[df_hour['Bull Entry'] == True, 'TP'] = df_hour['Close'] - ((df_hour['Close'] - df_hour['SL']) * -1)
    df_hour.loc[df_hour['Bear Entry'] == True, 'TP'] = df_hour['Close'] + (df_hour['Close'] - df_hour['SL'])

    # print(df_hour[df_hour['Bull Entry'] == True])
    # print(df_hour[df_hour['Bear Entry'] == True])
    # print(df_hour)

    index_arr_hour = df_hour.index.to_numpy()
    open_arr_hour = df_hour['Open'].to_numpy()
    close_arr_hour = df_hour['Close'].to_numpy()
    bull_entry_arr_hour = df_hour['Bull Entry'].to_numpy()
    bear_entry_arr_hour = df_hour['Bear Entry'].to_numpy()
    sl_arr_hour = df_hour['SL'].to_numpy()
    tp_arr_hour = df_hour['TP'].to_numpy()
    price_arr_hour = np.full(len(index_arr_hour), np.nan)
    bull_entrymask_arr_hour = np.full(len(index_arr_hour), False)
    bear_entrymask_arr_hour = np.full(len(index_arr_hour), False)
    bull_exit_arr_hour = np.full(len(index_arr_hour), False)
    bear_exit_arr_hour = np.full(len(index_arr_hour), False)

    trade_direct: Optional[str] = None
    cur_sl: Optional[float] = None; cur_tp: Optional[float] = None

    for i in range(len(index_arr_hour)):
        if trade_direct is None:

            if bull_entry_arr_hour[i]:
                trade_direct = 'bull'
                bull_entrymask_arr_hour[i] = True
                cur_sl, cur_tp = sl_arr_hour[i], tp_arr_hour[i]
                price_arr_hour[i] = close_arr_hour[i]
            elif bear_entry_arr_hour[i]:
                trade_direct = 'bear'
                bear_entrymask_arr_hour[i] = True
                cur_sl, cur_tp = sl_arr_hour[i], tp_arr_hour[i]
                price_arr_hour[i] = close_arr_hour[i]
            else:
                continue  

        elif trade_direct == 'bull':
            close = False
            close_price = None

            if close_arr_hour[i] < cur_sl:
                close = True
                close_price = cur_sl
            elif close_arr_hour[i] > cur_tp:
                close = True
                close_price = cur_tp

            if close:
                price_arr_hour[i] = close_price
                trade_direct, cur_sl, cur_tp = None, None, None
                bull_exit_arr_hour[i] = True

        elif trade_direct == 'bear':
            close = False
            close_price = None

            if close_arr_hour[i] > cur_sl:
                close = True
                close_price = cur_sl
            elif close_arr_hour[i] < cur_tp:
                close = True
                close_price = cur_tp

            if close:
                price_arr_hour[i] = close_price
                trade_direct, cur_sl, cur_tp = None, None, None
                bear_exit_arr_hour[i] = True

    pf = vbt.Portfolio.from_signals(
    entries = bear_entrymask_arr_hour,
    exits = bear_exit_arr_hour,
    short_entries = bull_entrymask_arr_hour,
    short_exits = bull_exit_arr_hour,
    price = price_arr_hour,
    open = df_hour["Open"],
    close = df_hour["Close"],
    high = df_hour["High"],
    low = df_hour["Low"],
    size = config['Trade']['size'],
    size_type = config['Trade']['size_type'],
    fees = config['Broker']['fees'],
    fixed_fees = config['Broker']['fixed_fees'],
    slippage = config['Slippage'],
    init_cash = config['Initial_cash'],
    freq = config['Frequency']
    )

    print(pf.stats())