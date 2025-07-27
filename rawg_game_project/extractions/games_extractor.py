## Importing necessary packages.

import pandas as pd
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import time
import duckdb


## Loading environment variable for api key.
load_dotenv()

## Setting variables to start working with the api.

api_key = os.getenv('RAWG_API_KEY')
base_url = 'https://api.rawg.io/api'
endpoint = '/games'
page_size = 40
start_page = 1
rate_limit_delay_sec = 1
start_date = '2025-01-01'
end_date = datetime.today().strftime('%Y-%m-%d')
date_range = f"{start_date},{end_date}"

params = {
    'key': api_key,
    'page_size': page_size,
    'page': start_page,
    'dates': date_range
}

def get_games_data(max_pages=1000):
    all_games = []
    page = 1

    while page <= max_pages:
        print(f"Fetching page {page}")

        params = {
            'key': api_key,
            'page_size': page_size,
            'page': page,
            'dates': date_range
        }

        try:
            response = requests.get(f"{base_url}{endpoint}", params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Request error on page {page}: {e}")
            break

        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break

        data = response.json()

        results = data.get('results', [])
        if not results:
            print('No more results.')
            break

        all_games.extend(results)
        page += 1

        time.sleep(rate_limit_delay_sec)

    games_df = pd.DataFrame(all_games)
    return games_df

def load_to_duckdb(df, db_path= 'rawg_data.duckdb', table_name = 'games_raw'):

    con = duckdb.connect(db_path)

    con.execute(f"create or replace table {table_name} as select * from df")

    con.close()
    print(f"Data loaded into {db_path}, table: {table_name}")

if __name__ == '__main__':
    print('Starting data extraction')
    games_df = get_games_data()

    if not games_df.empty:
        print(f"Extracted {len(games_df)} games. Now loading to DuckDB.")
        load_to_duckdb(games_df)
    else:
        print("No data was extracted. Nothing to load.")



