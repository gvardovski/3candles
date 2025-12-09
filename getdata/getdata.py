from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm
import os
import requests
import yaml
import sys
import time as wait
import pandas as pd

def check_if_config_file_exist(path, flag):
    if os.path.exists(path):
        print(f"Found your Configuration file \'{path}\'!")
    else:
        print(f"\nYour Configuration file \'{path}\' doesn't exist!\nDo you want to create it?")

        def type_choise():
            choise = input("\nType\nY if YES\nQ if NO\n")
            choise = choise.upper().strip()
            if choise == "Q":
                sys.exit("\nCreate your Configuration file \'{path}\' and try once more time!")
            elif choise == "Y":
                with open(path, "w") as file:
                    if flag == 1:
                        msg = ("# Time interval of data\nTime interval:\n" +
                        "  Start_year: 2020\n  Start_month: 1\n  End_year: 2025\n  End_month: 12\n\n" +
                        "# Place from which take data\n# Name of TOKEN which data you need\nData info:\n  Exchange: OANDA\n  Token: EURUSD\n\n" +
                        "# Time frequency of the data 'minute', 'hour', 'day', 'week'\nFrequency: hour")
                    else:
                        msg = ("# Path to the data files *.CSV\nData_filename_hour: ???.csv\nData_filename_minute: ???.csv\n\n" +
                        "# size type: 'value' 'amount' 'percent'\nTrade:\n  size: 1\n  size_type: amount\n\n" +
                        "# Fees for one amount of trade in percent\n# Fixed fees for one trade in currency units\nBroker:\n  fees: 0.0003\n  fixed_fees: 0\n\n" +
                        "# Slippage in percent\nSlippage: 0.02\n\n# Start cash value\nInitial_cash: 55000\n\n" +
                        "# Risk to Reward ratio\nRR: 1\n\n# Stop Loss in percent. Depends on the RR. 1 is 100% of RR\nSL: 0.5\n\n" +
                        "# Take Profit in percent. Depends on the RR. 1 is 100% of RR\nTP: 1")
                    file.write(msg)
            else:
                type_choise()
        type_choise()

def check_dataconfig(data_config):
    try:
        START_YEAR = data_config['Time interval']['Start_year']
        START_MONTH = data_config['Time interval']['Start_month']
        END_YEAR = data_config['Time interval']['End_year']
        END_MONTH = data_config['Time interval']['End_month']
        EXCHANGE = data_config['Data info']['Exchange']
        TOKEN = data_config['Data info']['Token']
        FREQ = data_config['Frequency']
    except KeyError as e:
        exit(f"Your Configuration file is missing a key: {e}\nPlease, check your configuration file.")
    if not isinstance(START_YEAR, int) or not isinstance(START_MONTH, int) or not isinstance(END_YEAR, int) or not isinstance(END_MONTH, int):
        exit("DATA must be integer.")
    year = datetime.now().year
    if (START_YEAR > year or START_YEAR < 2000) or (END_YEAR > year or END_YEAR < 2000):
        exit(f"YEAR must be in interval 2000 - {year}.")
    if START_YEAR > END_YEAR:
        exit("START YEAR is greater then END YEAR.")
    if (START_MONTH > 12 or START_MONTH < 1) or (END_MONTH > 12 or END_MONTH < 1):
        exit("MONTH must be in interval 1 - 12.")
    if START_MONTH > END_MONTH:
        exit("START MONTH is greater then END MONTH.")    
    if not isinstance(EXCHANGE, str) or not isinstance(TOKEN, str):
        exit("DATA INFO must be a string.")
    if FREQ not in ['second', 'minute', 'hour', 'day', 'week', 'month', 'year']:
        exit("Frequency must be a string like ('second', 'minute', 'hour').")

def take_months(data_config):
    months = []
    START_YEAR = data_config['Time interval']['Start_year']
    START_MONTH = data_config['Time interval']['Start_month']
    END_YEAR = data_config['Time interval']['End_year']
    END_MONTH = data_config['Time interval']['End_month']

    while START_YEAR <= END_YEAR and START_MONTH <= END_MONTH:
        months.append(f"{START_YEAR}-{START_MONTH:02d}")
        START_MONTH += 1
        if START_MONTH > 12:
            START_MONTH = 1
            START_YEAR += 1
    return months

def get_data_from_api(data_config):
    months = take_months(data_config)
    df = None

    EXCHANGE = data_config['Data info']['Exchange']
    TOKEN = data_config['Data info']['Token']
    FREQ = data_config['Frequency']
    print("Start loading data!")
    for month in tqdm(months):
        url = f"https://api.insightsentry.com/v3/symbols/{EXCHANGE}:{TOKEN}/history?bar_interval=1&bar_type={FREQ}&extended=false&badj=false&dadj=false&start_ym={month}"

        headers = {
            "Authorization" : f"{check_env_varailable('IS_JWT_USER')} {check_env_varailable('IS_JWT')}",
            "Accept" : "application/json"
        }

        success = False
        while not success:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                df_month = pd.DataFrame(data["series"])
                if df is None:
                    df = df_month
                else:
                    df = pd.concat([df, df_month], ignore_index=True)
                success = True
                wait.sleep(0.5)
            else:
                print(f"Request failed for month {month} with status code {response.status_code}")
                try:
                    print(response.json())
                except Exception:
                    print(response.text)
                print("Retrying in 10 seconds...")
                wait.sleep(10)
    return df

def check_env_varailable(var_name):
    if os.getenv(var_name) is None or os.getenv(var_name) == "":
        exit(f"Environment variable '{var_name}' is not set. Please set it in your '.env file.")
    else:
        return os.getenv(var_name)

def make_csv(data_file_name=None):

    load_dotenv()  

    data_config_path = check_env_varailable('3CANDLES_DATA_CONFIG_PATH')

    check_if_config_file_exist(data_config_path, 1)
    with open(data_config_path, "r") as file:
        data_config = yaml.safe_load(file)
        check_dataconfig(data_config)

    if data_file_name == 'Data_filename_minute':
        data_config['Frequency'] = 'minute'
    df = get_data_from_api(data_config)

    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df.rename(columns={"time": "Time", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
    df = df.set_index("Time")

    outpudir = "data/"
    os.makedirs(outpudir, exist_ok=True)
    csv_file_path = f"{outpudir}{data_config['Data info']['Exchange']}:{data_config['Data info']['Token']}_{data_config['Frequency']}.CSV"
    df.to_csv(csv_file_path)
    print(f"CSV file '{csv_file_path}' was created!")
    return csv_file_path