
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm
import os
import requests
import time as time_
import pandas as pd

load_dotenv()

# print(os.environ.get('IS_JWT'))

months = []
start_year = 2020
start_month = 1
end_year = 2025
end_month = 12

year = start_year
month = start_month

while year <= end_year and month <= end_month:
    months.append(f"{year}-{month:02d}")
    month += 1
    if month > 12:
        month = 1
        year += 1

# print(months)
data_df = None

for month in tqdm(months):
    url = f"https://api.insightsentry.com/v3/symbols/OANDA:EURUSD/history?bar_interval=1&bar_type=hour&extended=false&badj=false&dadj=false&start_ym={month}"

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
            if data_df is None:
                data_df = df_month
            else:
                data_df = pd.concat([data_df, df_month], ignore_index=True)
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

data_df["time"] = pd.to_datetime(data_df["time"], unit="s", utc=True)
data_df = data_df.rename(columns={"time": "Time", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
data_df = data_df.set_index("Time")

data_df.to_csv("data/EURUSD.csv")