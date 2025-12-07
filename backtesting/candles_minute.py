from getdata.getdata import check_if_config_file_exist, check_env_varailable
from backtesting.candles_hour import check_if_env_file_exist, check_config, check_if_csv_file_exist
from dotenv import load_dotenv
from typing import Optional
import yaml
import pandas as pd
import vectorbt as vbt
import numpy as np

def make_backtest_minute():

    check_if_env_file_exist()
    load_dotenv()

    config_path = check_env_varailable('3CANDLES_CONFIG_PATH')
    check_if_config_file_exist(config_path, 2)

    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
        check_config(config)
        config = check_if_csv_file_exist(config)

    df_hour = pd.read_csv(config['Data_filename_hour'])
    df_hour = df_hour.set_index('Time')
    df_hour.index = pd.to_datetime(df_hour.index)

    df_min = pd.read_csv(config['Data_filename_minute'])
    df_min = df_min.set_index('Time')
    df_min.index = pd.to_datetime(df_min.index)

    df_hour = df_hour.drop(columns=['Volume', 'High', 'Low'])
    df_hour['Dir'] = 0

    df_hour.loc[df_hour['Close'] > df_hour['Open'], 'Dir'] = 1
    bull_entry_mask = ((df_hour['Dir'] == 1) & (df_hour['Dir'].shift(1) == 1) & (df_hour['Dir'].shift(2) == 1) & #short
                        (df_hour['Close'].shift(2) < df_hour['Open']))
    df_hour['Bull Entry'] = bull_entry_mask
    bear_entry_mask = ((df_hour['Dir'] == 0) & (df_hour['Dir'].shift(1) == 0) & (df_hour['Dir'].shift(2) == 0) & #long
                        (df_hour['Close'].shift(2) > df_hour['Open']))
    df_hour['Bear Entry'] = bear_entry_mask

    df_hour.loc[df_hour['Bull Entry'] == True, 'SL'] = df_hour['Close'] + ((df_hour['Close'] - df_hour['Close'].shift(2)) * (config['RR'] * 0.5)) #short
    df_hour.loc[df_hour['Bear Entry'] == True, 'SL'] = df_hour['Close'] - ((df_hour['Close'].shift(2) - df_hour['Close']) * (config['RR'] * 0.5))#long

    df_hour.loc[df_hour['Bull Entry'] == True, 'TP'] = df_hour['Close'] - ((df_hour['Close'] - df_hour['Close'].shift(2)) * config['RR']) #short
    df_hour.loc[df_hour['Bear Entry'] == True, 'TP'] = df_hour['Close'] + ((df_hour['Close'].shift(2) - df_hour['Close']) * config['RR']) #long

    df_min['Bull Entry'] = df_hour['Bull Entry'].shift(1).reindex(df_min.index, method='ffill')
    df_min['Bear Entry'] = df_hour['Bear Entry'].shift(1).reindex(df_min.index, method='ffill')
    df_min['SL'] = df_hour['SL'].shift(1).reindex(df_min.index, method='ffill')
    df_min['TP'] = df_hour['TP'].shift(1).reindex(df_min.index, method='ffill')
    df_min['Date and hour'] = df_min.index.floor('h')
    df_min.dropna(inplace=True)

    index_arr_min = df_min.index.to_numpy()
    open_arr_min = df_min['Open'].to_numpy()
    close_arr_min = df_min['Close'].to_numpy()
    bull_entry_arr_min = df_min['Bull Entry'].to_numpy()
    bear_entry_arr_min = df_min['Bear Entry'].to_numpy()
    sl_arr_min = df_min['SL'].to_numpy()
    tp_arr_min = df_min['TP'].to_numpy()
    date_and_hour_arr_min = df_min['Date and hour'].to_numpy()
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

    file_path = config['Data_filename_minute'].split('.')[0]
    pf.stats().to_csv(f"{file_path}_stats.CSV")
    print(f"CSV file with statistics '{f"{file_path}_stats.CSV"}' was created!")
    pf.trades.records_readable.to_csv(f"{file_path}_trades.CSV")
    print(f"CSV file with trades '{f"{file_path}_trades.CSV"}' was created!")