from getdata.getdata import make_csv, check_if_config_file_exist, check_env_varailable
from dotenv import load_dotenv
from typing import Optional
from src.savetopdf import save_backtesting_results_to_pdf
import os
import yaml
import pandas as pd
import vectorbt as vbt
import numpy as np

def check_config(config, flag):
    try:
        CSV_FILE_NAME = config['Data_filename_hour']
        if flag == 2:
            csv_file_name_min = config['Data_filename_minute']
            if csv_file_name_min == None or csv_file_name_min.split('.')[-1] not in ['csv', 'CSV'] or not isinstance(csv_file_name_min, str):
                exit("Data_filename_minute must be a string with .CSV extension.")
        size = config['Trade']['size']
        size_type = config['Trade']['size_type']
        fees = config['Broker']['fees']
        fixed_fees = config['Broker']['fixed_fees']
        slippage = config['Slippage']
        init_cash = config['Initial_cash']
        RR = config['RR']
        SL = config['SL']
        TP = config['TP']
    except KeyError as e:
        exit(f"Your Configuration file is missing a key: {e}\nPlease, check your configuration file.")
    if CSV_FILE_NAME == None or CSV_FILE_NAME.split('.')[-1] not in ['csv', 'CSV'] or not isinstance(CSV_FILE_NAME, str):
        exit("Data_filename_hour must be a string with .CSV extension.")
    if size == None or size <= 0 or not isinstance(size, (int, float)):
        exit("Trade size must be a positive number.")
    if size_type == None or size_type not in ['amount', 'percent', 'value']:
        exit("Trade size_type must be either 'amount' 'percent' or 'value'.")
    if fees == None or not isinstance(fees, (int, float)) or not (0 <= fees <= 100):
        exit("Broker fees must be between 0 and 100.")
    if fixed_fees == None or not isinstance(fixed_fees, (int, float)) or fixed_fees < 0:
        exit("Broker fixed_fees must be a non-negative number.")
    if slippage == None or slippage < 0 or not isinstance(slippage, (int, float)):
        exit("Slippage must be a non-negative number.")
    if init_cash == None or init_cash < 0 or not isinstance(slippage, (int, float)):
        exit("Initial_cash must be a non-negative number.")
    if RR == None or RR <= 0 or not isinstance(RR, (int, float)):
        exit("RR must be a positive number.")
    if SL == None or SL <= 0 or not isinstance(SL, (int, float)):
        exit("SL must be a positive number.")
    if TP == None or TP <= 0 or not isinstance(TP, (int, float)):
        exit("TP must be a positive number.")

def check_if_csv_file_exist(config, data_file_name):
    if os.path.exists(config[data_file_name]):
        print(f"Found your Data file '{config[data_file_name]}'!")
    else:
        print(f"Your CSV file '{config[data_file_name]}' doesn't exist!\nCSV file will be created automatically!")
        config[data_file_name] = make_csv(data_file_name)
    return config

def check_if_env_file_exist():
    if not os.path.exists('.env'):
        print(f"Your '.env' file doesn't exist!\nIt will be created automatically!")
        with open('.env', 'w') as file:
            msg = ("IS_JWT_USER=\nIS_JWT=\n3CANDLES_DATA_CONFIG_PATH=configs/dataconfig.yaml\n3CANDLES_CONFIG_PATH=configs/config.yaml")
            file.write(msg)

def make_backtest_hour():

    check_if_env_file_exist()
    load_dotenv()

    config_path = check_env_varailable('3CANDLES_CONFIG_PATH')
    check_if_config_file_exist(config_path, 2)

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
        check_config(config, 1)
        config = check_if_csv_file_exist(config, 'Data_filename_hour')

    df_hour = pd.read_csv(config['Data_filename_hour'])
    df_hour = df_hour.set_index('Time')
    df_hour.index = pd.to_datetime(df_hour.index)
    
    df_hour = df_hour.drop(columns=['Volume', 'High', 'Low'])
    df_hour['Dir'] = 0

    df_hour.loc[df_hour['Close'] > df_hour['Open'], 'Dir'] = 1
    bull_entry_mask = ((df_hour['Dir'] == 1) & (df_hour['Dir'].shift(1) == 1) & (df_hour['Dir'].shift(2) == 1) & #short
                       (df_hour['Close'].shift(2) < df_hour['Open']))
    df_hour['Bull Entry'] = bull_entry_mask
    bear_entry_mask = ((df_hour['Dir'] == 0) & (df_hour['Dir'].shift(1) == 0) & (df_hour['Dir'].shift(2) == 0) & #long
                       (df_hour['Close'].shift(2) > df_hour['Open']))
    df_hour['Bear Entry'] = bear_entry_mask

    df_hour.loc[df_hour['Bull Entry'] == True, 'SL'] = df_hour['Close'] + ((df_hour['Close'] - df_hour['Close'].shift(2)) * (config['RR'] * config['SL'])) #short
    df_hour.loc[df_hour['Bear Entry'] == True, 'SL'] = df_hour['Close'] - ((df_hour['Close'].shift(2) - df_hour['Close']) * (config['RR'] * config['SL'])) #long
    
    df_hour.loc[df_hour['Bull Entry'] == True, 'TP'] = df_hour['Close'] - ((df_hour['Close'] - df_hour['Close'].shift(2)) * (config['RR'] * config['TP'])) #short
    df_hour.loc[df_hour['Bear Entry'] == True, 'TP'] = df_hour['Close'] + ((df_hour['Close'].shift(2) - df_hour['Close']) * (config['RR'] * config['TP'])) #long

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

            if close_arr_hour[i] >= cur_sl:
                price_arr_hour[i] = cur_sl
                trade_direct, cur_sl, cur_tp = None, None, None
                bull_exit_arr_hour[i] = True
            elif close_arr_hour[i] <= cur_tp:
                price_arr_hour[i] = cur_tp
                trade_direct, cur_sl, cur_tp = None, None, None
                bull_exit_arr_hour[i] = True

        elif trade_direct == 'bear':

            if close_arr_hour[i] <= cur_sl:
                price_arr_hour[i] = cur_sl
                trade_direct, cur_sl, cur_tp = None, None, None
                bear_exit_arr_hour[i] = True
            elif close_arr_hour[i] >= cur_tp:
                price_arr_hour[i] = cur_tp
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
    size = config['Trade']['size'],
    size_type = config['Trade']['size_type'],
    fees = config['Broker']['fees'],
    fixed_fees = config['Broker']['fixed_fees'],
    slippage = config['Slippage'],
    init_cash = config['Initial_cash'],
    freq = '1h'
    )

    file_path = config['Data_filename_hour'].split('.')[0]
    save_backtesting_results_to_pdf(pf, file_path)
    pf.trades.records_readable.to_csv(f"{file_path}_trades.CSV")
    print(f"CSV file with trades '{f"{file_path}_trades.CSV"}' was created!")
