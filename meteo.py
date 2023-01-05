import time
import requests
import schedule
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from datetime import datetime
from dotenv import load_dotenv

# Weather forecast API: meteo.lt https://api.meteo.lt/
# Note: The API limit is 20000 requests per 24h per IP address.

PLACES = ['vilnius', 'kaunas', 'klaipeda', 'silute', 'neringa-nida']
API_FORECAST_LONG = 'https://api.meteo.lt/v1/places/{}/forecasts/long-term'
TABLE_ID_DEV = "meteo-372509.temp.new4"
TABLE_ID_STAG = "meteo-372509.temp.forecast"
SCHEMA = [SchemaField('forecastTimeUtc', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('airTemperature', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('feelsLikeTemperature', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('windSpeed', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('windGust', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('windDirection', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('cloudCover', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('seaLevelPredssure', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('relativeHumidity', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('totalPrecipitation', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('conditionCode', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('forecastCreationTimeUtc', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('forecastType', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('code', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('name', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('country', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('countryCode', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('latitude', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('longitude', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('date', 'DATETIME', 'NULLABLE', None, None, (), None)]

def get_data_from_api(url: str) -> dict:
    return requests.get(url).json()

def convert_json_to_df(json: dict) -> pd.DataFrame:
    df = pd.json_normalize(json, "forecastTimestamps", ["place", "forecastCreationTimeUtc", "forecastType"])
    df_place = pd.json_normalize(df['place'])
    return pd.concat([df, df_place], axis="columns")

def append_data_to_df(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([df1, df2], axis="index")

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df['date'] = datetime.today()
    df = df.drop(columns=['place', 'administrativeDivision'])
    df.rename(columns={'coordinates.latitude': 'latitude', 'coordinates.longitude': 'longitude'}, inplace=True)
    return df

def save_data_to_bigquery(df: pd.DataFrame, table_id: str ) -> None:

    client = bigquery.Client()

    # Check if table exists, if not - create it
    try:
        bg_table = client.get_table(table_id)
        print(f"Table '{bg_table}' exsist!")

    except Exception:
        print(f"Table '{table_id}' does not exists!")
        table = bigquery.Table(table_id, SCHEMA)
        job = client.create_table(table)
        print(f"Table '{table_id}' has been created!")

    # Append data to existing bigquery table
    # to-do: check 'job' as a result of row insertion, catch an error if list is not empty 
    try:
        job = client.insert_rows_from_dataframe(table=table_id, dataframe=df, selected_fields=SCHEMA)
        print("Rows sucesfully inserted!")
        bg_table = client.get_table(table_id)
        print("Table has {} rows".format(bg_table.num_rows))

    except Exception:
        print("Rows insertion failed!")

def periodic_task(places: list)-> None:

    forecast_df = pd.DataFrame()

    for place in places:
        json = get_data_from_api(API_FORECAST_LONG.format(place))
        df = convert_json_to_df(json)
        df = transform_data(df)
        forecast_df = append_data_to_df(forecast_df, df)

    save_data_to_bigquery(forecast_df, TABLE_ID_DEV)

# Execute
if __name__ == "__main__":
    load_dotenv()
    periodic_task(PLACES)
    
    schedule.every(3).hours.do(periodic_task, PLACES)

    while True:
        schedule.run_pending()
        time.sleep(1)
