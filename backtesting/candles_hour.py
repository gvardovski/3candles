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
            CSV_FILE_NAME_MIN = config['Data_filename_minute']
            if CSV_FILE_NAME_MIN == None or CSV_FILE_NAME_MIN.split('.')[-1] not in ['csv', 'CSV'] or not isinstance(CSV_FILE_NAME_MIN, str):
                exit("Data_filename_minute must be a string with .CSV extension.")
        SIZE = config['Trade']['size']
        SIZE_TYPE = config['Trade']['size_type']
        FEES = config['Broker']['fees']
        FIXED_FEES = config['Broker']['fixed_fees']
        SLIPPAGE = config['Slippage']
        INIT_CASH = config['Initial_cash']
        RR = config['RR']
        SL = config['SL']
        TP = config['TP']
        START_TR_TIME = config['Trading_time']['Start_time']
        END_TR_TIME = config['Trading_time']['End_time']
    except KeyError as e:
        exit(f"Your Configuration file is missing a key: {e}\nPlease, check your configuration file.")
    if CSV_FILE_NAME == None or CSV_FILE_NAME.split('.')[-1] not in ['csv', 'CSV'] or not isinstance(CSV_FILE_NAME, str):
        exit("Data_filename_hour must be a string with .CSV extension.")
    if SIZE == None or SIZE <= 0 or not isinstance(SIZE, (int, float)):
        exit("Trade SIZE must be a positive number.")
    if SIZE_TYPE == None or SIZE_TYPE not in ['amount', 'percent', 'value']:
        exit("Trade SIZE_TYPE must be either 'amount' 'percent' or 'value'.")
    if FEES == None or not isinstance(FEES, (int, float)) or not (0 <= FEES <= 100):
        exit("Broker FEES must be between 0 and 100.")
    if FIXED_FEES == None or not isinstance(FIXED_FEES, (int, float)) or FIXED_FEES < 0:
        exit("Broker FIXED_FEES must be a non-negative number.")
    if SLIPPAGE == None or SLIPPAGE < 0 or not isinstance(SLIPPAGE, (int, float)):
        exit("Slippage must be a non-negative number.")
    if INIT_CASH == None or INIT_CASH < 0 or not isinstance(SLIPPAGE, (int, float)):
        exit("Initial_cash must be a non-negative number.")
    if RR == None or RR <= 0 or not isinstance(RR, (int, float)):
        exit("RR must be a positive number.")
    if SL == None or SL <= 0 or not isinstance(SL, (int, float)):
        exit("SL must be a positive number.")
    if TP == None or TP <= 0 or not isinstance(TP, (int, float)):
        exit("TP must be a positive number.")
    if not isinstance(START_TR_TIME, str) or not isinstance(END_TR_TIME, str):
        exit("TRADING TIME must be a string.")
    if START_TR_TIME.count(':') != 1 or END_TR_TIME.count(':') != 1:
        exit("TRADING TIME must be in 'hour:minute' format.")

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
    SIZE = config['Trade']['SIZE'],
    SIZE_TYPE = config['Trade']['SIZE_TYPE'],
    FEES = config['Broker']['FEES'],
    FIXED_FEES = config['Broker']['FIXED_FEES'],
    SLIPPAGE = config['Slippage'],
    INIT_CASH = config['Initial_cash'],
    freq = '1h'
    )

    file_path = config['Data_filename_hour'].split('.')[0]
    save_backtesting_results_to_pdf(pf, file_path)
    pf.trades.records_readable.to_csv(f"{file_path}_trades.CSV")
    print(f"CSV file with trades '{f"{file_path}_trades.CSV"}' was created!")
