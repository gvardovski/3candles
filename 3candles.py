from data import getdata
from dotenv import load_dotenv
import os
import yaml
import pandas as pd
import vectorbt as vbt

def check_config(config):
    try:
        len_start = config['RSI']['length_start']
        len_end = config['RSI']['length_end']
        len_step = config['RSI']['length_step']
        overbought = config['RSI']['overbought']
        oversold = config['RSI']['oversold']
        size = config['Trade']['size']
        size_type = config['Trade']['size_type']
        fees = config['Broker']['fees']
        fixed_fees = config['Broker']['fixed_fees']
        slippage = config['Slippage']
        init_cash = config['Initial_cash']
        freq = config['Frequency']
    except KeyError as e:
        exit(f"Your Configuration file is missing a key: {e}\nPlease, check your configuration file.")
    if not isinstance(len_start, int) or not isinstance(len_end, int) or not isinstance(len_step, int) or not isinstance(overbought, int) or not isinstance(oversold, int):
        exit("Lenght must be integer.")
    if len_start <= 0 or len_end <= 0 or len_step <= 0:
        exit("RSI length must be a positive integer.")
    if len_start >= len_end:
        exit("RSI length_start must be less than length_end.")
    if not (0 <= overbought <= 100) or not (0 <= oversold <= 100):
        exit("Overbought and Oversold levels must be between 0 and 100.")
    if overbought <= oversold:
        exit("Overbought level must be greater than Oversold level.")
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
    
    df_hour['SL'] = False
    df_hour['TP'] = False
    df_hour['Dir'] = 0
    df_hour.loc[df_hour['Close'] > df_hour['Open'], 'Dir'] = 1
    bull_entry_mask = ((df_hour['Dir'] == 0) & (df_hour['Dir'].shift(1) == 0) & (df_hour['Dir'].shift(-1) == 0) &
                       (df_hour['Close'].shift(1) > df_hour['Open'].shift(-1)))
    df_hour['Bull Entry'] = bull_entry_mask
    bear_entry_mask = ((df_hour['Dir'] == 1) & (df_hour['Dir'].shift(1) == 1) & (df_hour['Dir'].shift(-1) == 1) &
                       (df_hour['Close'].shift(1) < df_hour['Open'].shift(-1)))
    df_hour['Bear Entry'] = bear_entry_mask
    print(df_hour[df_hour['Bull Entry'] == True])
    print(df_hour[df_hour['Bear Entry'] == True])
