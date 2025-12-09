from getdata.getdata import check_if_config_file_exist, check_env_varailable
from backtesting.candles_hour import check_if_env_file_exist, check_if_csv_file_exist
from src.makemetricpng import create_heatmap
from dotenv import load_dotenv
from typing import Optional
from datetime import time
from tqdm import tqdm
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
        SL_START = config['SL']['start']
        SL_END = config['SL']['end']
        SL_STEP = config['SL']['step']
        TP_START = config['TP']['start']
        TP_END = config['TP']['end']
        TP_STEP = config['TP']['step']
        START_TR_TIME = config['Trading_time']['Start_time']
        END_TR_TIME = config['Trading_time']['End_time']
        BACK_TEST_START = config['Backtesting_dates']['start']
        BACK_TEST_END = config['Backtesting_dates']['end']
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
    if SL_START == None or SL_START <= 0 or not isinstance(SL_START, (int, float)):
        exit("SL_START must be a positive number.")
    if SL_END == None or SL_END <= 0 or not isinstance(SL_END, (int, float)):
        exit("SL_END must be a positive number.")
    if SL_STEP == None or SL_STEP <= 0 or not isinstance(SL_STEP, (int, float)):
        exit("SL_STEP must be a positive number.")
    if TP_START == None or TP_START <= 0 or not isinstance(TP_START, (int, float)):
        exit("TP_START must be a positive number.")
    if TP_END == None or TP_END <= 0 or not isinstance(TP_END, (int, float)):
        exit("TP_END must be a positive number.")
    if TP_STEP == None or TP_STEP <= 0 or not isinstance(TP_STEP, (int, float)):
        exit("TP_STEP must be a positive number.")
    if not isinstance(START_TR_TIME, str) or not isinstance(END_TR_TIME, str):
        exit("TRADING TIME must be a string.")
    if START_TR_TIME.count(':') != 1 or END_TR_TIME.count(':') != 1:
        exit("TRADING TIME must be in 'hour:minute' format.")
    if not isinstance(BACK_TEST_START, str) or not isinstance(BACK_TEST_END, str):
        exit("BACKTESTING TIME must be a string.")

def process_data(df_hour: pd.DataFrame, df_min: pd.DataFrame,  config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_hour = df_hour.set_index('Time')
    df_hour.index = pd.to_datetime(df_hour.index)
    df_hour = df_hour[config['Backtesting_dates']['start']:config['Backtesting_dates']['end']]

    df_min = df_min.set_index('Time')
    df_min.index = pd.to_datetime(df_min.index)
    df_min = df_min[config['Backtesting_dates']['start']:config['Backtesting_dates']['end']]

    df_hour.loc[df_hour['Close'] > df_hour['Open'], 'Dir'] = 1
    bull_entry_mask = ((df_hour['Dir'] == 1) & (df_hour['Dir'].shift(1) == 1) & (df_hour['Dir'].shift(2) == 1) & #short
                        (df_hour['Close'].shift(2) < df_hour['Open']))
    df_hour['Bull Entry'] = bull_entry_mask
    bear_entry_mask = ((df_hour['Dir'] == 0) & (df_hour['Dir'].shift(1) == 0) & (df_hour['Dir'].shift(2) == 0) & #long
                        (df_hour['Close'].shift(2) > df_hour['Open']))
    df_hour['Bear Entry'] = bear_entry_mask

    return df_hour, df_min

def backtest_strategy(df_hour: pd.DataFrame, df_min: pd.DataFrame, config: dict) -> vbt.Portfolio:

    df_hour.loc[df_hour['Bull Entry'] == True, 'SL'] = df_hour['Close'] + ((df_hour['Close'] - df_hour['Close'].shift(2)) * (config['RR'] * config['SL']['start'])) #short
    df_hour.loc[df_hour['Bear Entry'] == True, 'SL'] = df_hour['Close'] - ((df_hour['Close'].shift(2) - df_hour['Close']) * (config['RR'] * config['SL']['start'])) #long
    
    df_hour.loc[df_hour['Bull Entry'] == True, 'TP'] = df_hour['Close'] - ((df_hour['Close'] - df_hour['Close'].shift(2)) * (config['RR'] * config['TP']['start'])) #short
    df_hour.loc[df_hour['Bear Entry'] == True, 'TP'] = df_hour['Close'] + ((df_hour['Close'].shift(2) - df_hour['Close']) * (config['RR'] * config['TP']['start'])) #long
    
    df_min['Bull Entry'] = df_hour['Bull Entry'].shift(1).reindex(df_min.index, method='ffill')
    df_min['Bear Entry'] = df_hour['Bear Entry'].shift(1).reindex(df_min.index, method='ffill')
    df_min['SL'] = df_hour['SL'].shift(1).reindex(df_min.index, method='ffill')
    df_min['TP'] = df_hour['TP'].shift(1).reindex(df_min.index, method='ffill')
    df_min['Date and hour'] = df_min.index.floor('h')
    df_min.dropna(inplace=True)

    trading_start_time = time(int(config['Trading_time']['Start_time'].split(':')[0]),int(config['Trading_time']['Start_time'].split(':')[1]))
    trading_end_time = time(int(config['Trading_time']['End_time'].split(':')[0]),int(config['Trading_time']['End_time'].split(':')[1]))

    index_arr_min = df_min.index.to_numpy()
    close_arr_min = df_min['Close'].to_numpy()
    bull_entry_arr_min = df_min['Bull Entry'].to_numpy()
    bear_entry_arr_min = df_min['Bear Entry'].to_numpy()
    sl_arr_min = df_min['SL'].to_numpy()
    tp_arr_min = df_min['TP'].to_numpy()
    date_and_hour_arr_min = df_min['Date and hour'].to_numpy()
    candle_time_arr_min = df_min.index.time
    price_arr_min = np.full(len(index_arr_min), np.nan)
    bull_entrymask_arr_min = np.full(len(index_arr_min), False)
    bear_entrymask_arr_min = np.full(len(index_arr_min), False)
    bull_exit_arr_min = np.full(len(index_arr_min), False)
    bear_exit_arr_min = np.full(len(index_arr_min), False)

    trade_direct: Optional[str] = None
    cur_sl: Optional[float] = None; cur_tp: Optional[float] = None
    opened_date_and_hour = {}

    for i in range(len(index_arr_min)):
        if trade_direct is None:
            if not (trading_start_time <= candle_time_arr_min[i] <= trading_end_time):
                continue

            date_and_hour = date_and_hour_arr_min[i]

            if bull_entry_arr_min[i] and opened_date_and_hour.get(date_and_hour) == None:
                trade_direct = 'bull'
                bull_entrymask_arr_min[i] = True
                cur_sl, cur_tp = sl_arr_min[i], tp_arr_min[i]
                price_arr_min[i] = close_arr_min[i]
                opened_date_and_hour[date_and_hour] = True
            elif bear_entry_arr_min[i] and opened_date_and_hour.get(date_and_hour) == None:
                trade_direct = 'bear'
                bear_entrymask_arr_min[i] = True
                cur_sl, cur_tp = sl_arr_min[i], tp_arr_min[i]
                price_arr_min[i] = close_arr_min[i]
                opened_date_and_hour[date_and_hour] = True
            else:
                continue  

        elif trade_direct == 'bull':

            if close_arr_min[i] >= cur_sl:
                price_arr_min[i] = cur_sl
                trade_direct, cur_sl, cur_tp = None, None, None
                bull_exit_arr_min[i] = True
            elif close_arr_min[i] <= cur_tp:
                price_arr_min[i] = cur_tp
                trade_direct, cur_sl, cur_tp = None, None, None
                bull_exit_arr_min[i] = True

        elif trade_direct == 'bear':

            if close_arr_min[i] <= cur_sl:
                price_arr_min[i] = cur_sl
                trade_direct, cur_sl, cur_tp = None, None, None
                bear_exit_arr_min[i] = True
            elif close_arr_min[i] >= cur_tp:
                price_arr_min[i] = cur_tp
                trade_direct, cur_sl, cur_tp = None, None, None
                bear_exit_arr_min[i] = True

    pf = vbt.Portfolio.from_signals(
    entries = bear_entrymask_arr_min,
    exits = bear_exit_arr_min,
    short_entries = bull_entrymask_arr_min,
    short_exits = bull_exit_arr_min,
    price = price_arr_min,
    open = df_min["Open"],
    close = df_min["Close"],
    size = config['Trade']['size'],
    size_type = config['Trade']['size_type'],
    fees = config['Broker']['fees'],
    fixed_fees = config['Broker']['fixed_fees'],
    slippage = config['Slippage'],
    init_cash = config['Initial_cash'],
    freq = '1m'
    )

    return pf

def make_backtest_minute_optimization():

    check_if_env_file_exist()
    load_dotenv()

    config_path = check_env_varailable('3CANDLES_CONFIG_PATH')
    check_if_config_file_exist(config_path, 2)

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
        check_config(config, 2)
        config = check_if_csv_file_exist(config, 'Data_filename_hour')
        config = check_if_csv_file_exist(config, 'Data_filename_minute')
    
    df_hour = pd.read_csv(config['Data_filename_hour'])
    df_min = pd.read_csv(config['Data_filename_minute'])

    df_hour, df_min = process_data(df_hour, df_min, config)

    sl_range = np.arange(config['SL']['start'], config['SL']['end'], config['SL']['step'])
    tp_range = np.arange(config['TP']['start'], config['TP']['end'], config['TP']['step'])

    rezults = []
    for cur_sl in tqdm(sl_range, leave=False, desc='SL combinations'):
        config['SL']['start'] = round(cur_sl, 2)

        for cur_tp in tqdm(tp_range, leave=False, desc='TP combinations'):
            config['TP']['start'] = round(cur_tp, 2)
            pf = backtest_strategy(df_hour, df_min, config)
            stats = pf.stats().to_dict()
            stats.update({'SL': round(cur_sl, 2), 'TP': round(cur_tp, 2)})
            rezults.append(stats)

    optimization_df = pd.DataFrame(rezults)
    file_path = config['Data_filename_minute'].split('.')[0]
    optimization_df.to_csv(f"{file_path}_optimization.CSV")
    print(f"CSV file with optimization '{f"{file_path}_optimization.CSV"}' was created!")
    create_heatmap(optimization_df, 'Total Return [%]')