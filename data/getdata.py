from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm
import os
import requests
import yaml
import sys
import time as time_
import pandas as pd

def check_if_config_file_exist(path):
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
                    file.write("# Time interval of data\nTime interval:\n" +
                    "  Start_year: 2020\n  Start_month: 1\n  End_year: 2025\n  End_month: 12\n\n" +
                    "# Place from which take data\n# Name of token which data you need\nData info:\n  Exchange: OANDA\n  Token: EURUSD\n\n" +
                    "# Time frequency of the data 'minute', 'hour', 'day', 'week'\nFrequency: hour")
            else:
                type_choise()
        type_choise()

def check_config(config):
    try:
        start_year = config['Time interval']['Start_year']
        start_month = config['Time interval']['Start_month']
        end_year = config['Time interval']['End_year']
        end_month = config['Time interval']['End_month']
        exchange = config['Data info']['Exchange']
        token = config['Data info']['Token']
        freq = config['Frequency']
    except KeyError as e:
        exit(f"Your Configuration file is missing a key: {e}\nPlease, check your configuration file.")
    if not isinstance(start_year, int) or not isinstance(start_month, int) or not isinstance(end_year, int) or not isinstance(end_month, int):
        exit("DATA must be integer.")
    year = datetime.now().year
    if (start_year > year or start_year < 2000) or (end_year > year or end_year < 2000):
        exit(f"YEAR must be in interval 2000 - {year}.")
    if start_year > end_year:
        exit("START YEAR is greater then END YEAR.")
    if (start_month > 12 or start_month < 1) or (end_month > 12 or end_month < 1):
        exit("MONTH must be in interval 1 - 12.")
    if start_month > end_month:
        exit("START MONTH is greater then END MONTH.")    
    if not isinstance(exchange, str) or not isinstance(token, str):
        exit("DATA INFO must be a string.")
    if freq not in ['second', 'minute', 'hour', 'day', 'week', 'month', 'year']:
        exit("Frequency must be a string like ('second', 'minute', 'hour').")

def take_months(config):
    months = []
    start_year = config['Time interval']['Start_year']
    start_month = config['Time interval']['Start_month']
    end_year = config['Time interval']['End_year']
    end_month = config['Time interval']['End_month']

    while start_year <= end_year and start_month <= end_month:
        months.append(f"{start_year}-{start_month:02d}")
        start_month += 1
        if start_month > 12:
            start_month = 1
            start_year += 1
    return months

def get_data_from_api(config):
    months = take_months(config)
    df = None

    exchange = config['Data info']['Exchange']
    token = config['Data info']['Token']
    freq = config['Frequency']
    print("Start loading data!")
    for month in tqdm(months):
        url = f"https://api.insightsentry.com/v3/symbols/{exchange}:{token}/history?bar_interval=1&bar_type={freq}&extended=false&badj=false&dadj=false&start_ym={month}"

        headers = {
            "Authorization" : f"Bearer {os.getenv("IS_JWT")}",
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
                time_.sleep(0.5)
            else:
                print(f"Request failed for month {month} with status code {response.status_code}")
                try:
                    print(response.json())
                except Exception:
                    print(response.text)
                print("Retrying in 30 seconds...")
                time_.sleep(30)
    return df

if __name__ == "__main__":

    load_dotenv()  
    data_config = os.getenv('3CANDLES_DATA_CONFIG_PATH')

    check_if_config_file_exist(data_config)
    with open(data_config, "r") as file:
        config = yaml.safe_load(file)
        check_config(config)

    df = get_data_from_api(config)

    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df.rename(columns={"time": "Time", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
    df = df.set_index("Time")

    df.to_csv("data/EURUSD.CSV")