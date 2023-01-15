import sys
import time
import logging
import requests
import schedule
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from configparser import ConfigParser
from google.cloud.bigquery import SchemaField

DEFAULT_LOGGING_LEVEL = logging.INFO

logging.basicConfig(
filename="log.txt", force=True,
    format = '%(asctime)s %(levelname)-8s %(message)s',
    level = logging.INFO,
    datefmt = '%Y-%m-%d %H:%M:%S')

FO_SCHEMA = [SchemaField('forecastTimeUtc', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('airTemperature', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('feelsLikeTemperature', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('windSpeed', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('windGust', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('windDirection', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('cloudCover', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('seaLevelPressure', 'INTEGER', 'NULLABLE', None, None, (), None),
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
OB_SCHEMA = [SchemaField('observationTimeUtc', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('airTemperature', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('feelsLikeTemperature', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('windSpeed', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('windGust', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('windDirection', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('cloudCover', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('seaLevelPressure', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('relativeHumidity', 'INTEGER', 'NULLABLE', None, None, (), None),
         SchemaField('precipitation', 'FLOAT', 'NULLABLE', None, None, (), None),
         SchemaField('conditionCode', 'STRING', 'NULLABLE', None, None, (), None),
         SchemaField('date', 'DATETIME', 'NULLABLE', None, None, (), None),
         SchemaField('station', 'STRING', 'NULLABLE', None, None, (), None)]

def load_settings(file_name: str = 'config.ini') -> dict:
    '''
    Loads settings from an ini file and returns a dictionary as the result.
    '''
   
    config = ConfigParser()
    try:
        with open(file_name) as f:
            config.read_file(f)
    except IOError:
        msg = f'Error. File {file_name} was not found. Exiting..'
        print(msg)
        logging.error(msg)
        sys.exit()

    try:
        API_FORECAST_URL = config["API"]['forecast']
        API_OBSERVATION_URL = config["API"]['observation']
        STATIONS = list(config["API"]['stations'].split(','))
        PLACES = list(config["API"]['places'].split(','))
        FO_TABLE_ID = config["TABLE"]['forecast_table']
        OB_TABLE_ID = config["TABLE"]['observation_table']
        PULL_PERIOD_IN_HOURS = int(config["SETTINGS"]['interval_hours'])
        DEFAULT_LOGGING_LEVEL = config["SETTINGS"]['logging_level']

    except KeyError:
        msg = 'Error. Missing one or more parameters in config.ini file. Exiting..'
        print(msg)
        logging.error(msg)
        sys.exit()

    logging.info(f'Success. Config file {file_name} loaded')

    return {'API_FORECAST_URL':API_FORECAST_URL,
            'API_OBSERVATION_URL':API_OBSERVATION_URL,
            'STATIONS': STATIONS,
            'PLACES': PLACES,
            'FO_TABLE_ID': FO_TABLE_ID,
            'OB_TABLE_ID': OB_TABLE_ID,
            'PULL_PERIOD_IN_HOURS': PULL_PERIOD_IN_HOURS,
            'DEFAULT_LOGGING_LEVEL': DEFAULT_LOGGING_LEVEL}

def convert_json_to_df(json: dict, col: str, *args) -> pd.DataFrame:
    '''
    Converts JSON data into a flattened format and stores it in a Pandas DataFrame
    '''
    df_place = pd.DataFrame()
    
    df = pd.json_normalize(json, col, *args) 
    # dive one level deeper for forecast records extraction
    if col=='forecastTimestamps': 
        df_place = pd.json_normalize(df['place'])
    return pd.concat([df, df_place], axis="columns")
   
def get_last_date(table_id: str, date_column: str)-> datetime:
    '''
    Retrieves the most recent date record for a specific column in a specified table. If table is empty or
    does not exista a default date of 1999-01-01 00:00:00 will be returned
    '''
    
    last_date = '1999-01-01 00:00:00'
    client = bigquery.Client()
    query  = f'select max({date_column}) as last_date from {table_id}'

    try:
        rows = client.query(query)
        for row in rows:
            last_date = row[0]
    except:
        msg = f'Error. Query execution failed: {query}, job state: {rows.state}, errors reason: {rows.errors}'
        print(msg)
        logging.error(msg)
        
    return datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')

def add_data_to_bigquery(df: pd.DataFrame, table_id: str, schema: list):
    '''
    Adds data to a BigQuery database. If the specified database does not exist, a new one will be created.
    '''
    client = bigquery.Client()
    
    # Check for table in database, if table does not exist - create it
    try:
        bg_table = client.get_table(table_id)
        msg = f'Table {bg_table} already exsist!'
        logging.info(msg)
        print(msg)

    except Exception:
        msg = f'Table {table_id} does not exists!'
        logging.info(msg)
        print(msg)
        table = bigquery.Table(table_id, schema)
        job = client.create_table(table)
        msg = f'Table {table_id} has been created!'
        logging.info(msg)
        print(msg)
    
    # Add new records
    try:
        job = client.insert_rows_from_dataframe(table=table_id, dataframe=df, selected_fields=schema)
        msg = f'Rows sucesfully inserted into table {table_id}!'
        logging.info(msg)
        print(msg)
        bg_table = client.get_table(table_id)
    
    except Exception:
        msg = f'Rows insertion into table {table_id} failed!'
        logging.error(msg)
        print(msg)

def pull_data_from_api() -> pd.DataFrame:
    '''
    Weather forecast API: meteo.lt https://api.meteo.lt/
    Note: The API limit is 20000 requests per 24h per IP address.
    '''
    
    forecast_df = pd.DataFrame()
    observation_df = pd.DataFrame()
    
    # load forecast records of each place 
    logging.info(f'Starting to load forecast data for places {PLACES}')

    for place in PLACES:
        url = API_FORECAST_URL.format(place)
        logging.info(f'Forecast API url {url}')
        json = requests.get(url).json()
        fo_df = convert_json_to_df(json, "forecastTimestamps", ["place", "forecastCreationTimeUtc", "forecastType"])
        fo_df['date'] = datetime.today()
        fo_df = fo_df.drop(columns=['place', 'administrativeDivision'])
        fo_df.rename(columns={'coordinates.latitude': 'latitude', 'coordinates.longitude': 'longitude'}, inplace=True)
        forecast_df = pd.concat([forecast_df, fo_df], axis="index")
    
    logging.info(f'Sucess. Forecast data loaded. Dataframe shape {forecast_df.shape}')
                
    # load observartion records of each station   
    logging.info(f'Starting to load observation data for stations {STATIONS}')
    for station in STATIONS:
        url = API_OBSERVATION_URL.format(station)
        logging.info(f'Observation API url {url}')
        ob_df = pd.DataFrame()
        json = requests.get(url).json() 
        df = convert_json_to_df(json, "observations")
        ob_df = pd.concat([ob_df, df], axis="index")
        ob_df['date'] = datetime.today()
        ob_df['station'] = station
        observation_df = pd.concat([observation_df, ob_df],  axis="index")
    
    logging.info(f'Sucess. Observation data loaded. Dataframe shape {observation_df.shape}')
        
    return forecast_df, observation_df 

def periodic_job():
    '''
    Periodic task to load and process loaded data. On each run, script reads config.ini file, 
    so parameters can be modified on the fly, for example logging level can be changed
    '''
    logging.info('Script is starting...')
    
    global API_FORECAST_URL
    global API_OBSERVATION_URL
    global STATIONS
    global PLACES
    global FO_TABLE_ID
    global OB_TABLE_ID
    global PULL_PERIOD_IN_HOURS
    global DEFAULT_LOGGING_LEVEL

    settings = load_settings('config.ini')  

    if DEFAULT_LOGGING_LEVEL == 'DEBUG':
        logging.getLogger().setLevel(logging.DEBUG)
    elif DEFAULT_LOGGING_LEVEL == 'ERROR':
        logging.getLogger().setLevel(logging.ERROR)
    else: 
        logging.getLogger().setLevel(logging.INFO)
    
    API_FORECAST_URL = settings['API_FORECAST_URL']
    API_OBSERVATION_URL = settings['API_OBSERVATION_URL']
    STATIONS = settings['STATIONS']
    PLACES = settings['PLACES']
    FO_TABLE_ID = settings['FO_TABLE_ID']
    OB_TABLE_ID = settings['OB_TABLE_ID']
    PULL_PERIOD_IN_HOURS = settings['PULL_PERIOD_IN_HOURS']
    DEFAULT_LOGGING_LEVEL = settings['DEFAULT_LOGGING_LEVEL']

    logging.info(f'Config: Forecast api url "{API_FORECAST_URL}"')
    logging.info(f'Config: Observartion api url "{API_OBSERVATION_URL}"')
    logging.info(f'Config: Forecast table ID "{FO_TABLE_ID}"')
    logging.info(f'Config: Observation table ID "{OB_TABLE_ID}"')
    logging.info(f'Config: Script run period "{PULL_PERIOD_IN_HOURS} hours"')

    fo, ob = pull_data_from_api()

    # get datetime of last records in db
    last_ob_date = get_last_date(OB_TABLE_ID, 'observationTimeUtc')
    last_creation_date = get_last_date(FO_TABLE_ID, 'forecastCreationTimeUtc')

    logging.info(f'Last observation date "{last_ob_date}"')
    logging.info(f'Last forecast creation date "{last_creation_date}"')

    # filter observartion dataset according last update timestamp (keep only new records)
    ob_filtered = ob[ob['observationTimeUtc'].apply(lambda x : datetime.strptime(x, '%Y-%m-%d %H:%M:%S')) > last_ob_date]
    if ob_filtered.shape[0] != 0:
        job = add_data_to_bigquery(ob_filtered, OB_TABLE_ID, OB_SCHEMA)
        msg =f'Observation table {OB_TABLE_ID} updated successfully, {ob_filtered.shape[0]} rows added'
        print(msg)
        logging.info(msg)
    else:
        msg = f'There is no new data to add to the observation table {OB_TABLE_ID}'
        print(msg)
        logging.info(msg)

    # filter forecast dataset according last creation timestamp (keep only new records)
    fo_filtered = fo[fo['forecastCreationTimeUtc'].apply(lambda x : datetime.strptime(x, '%Y-%m-%d %H:%M:%S')) > last_creation_date]
    if fo_filtered.shape[0] != 0:
        job = add_data_to_bigquery(fo_filtered, FO_TABLE_ID, FO_SCHEMA)
        msg = f'Forecast table {FO_TABLE_ID} updated successfully, {fo_filtered.shape[0]} rows added'
        print(msg)
        logging.info(msg)

    else:
        msg = f'There is no new data to add to the forecast table {FO_TABLE_ID}'
        print(msg)
        logging.info(msg)

if __name__ == "__main__":
    periodic_job()
    schedule.every(PULL_PERIOD_IN_HOURS).hours.do(periodic_job)

    while True:
        schedule.run_pending()
        time.sleep(1)
