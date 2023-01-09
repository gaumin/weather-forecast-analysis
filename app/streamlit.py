import pandas_gbq
import pandas as pd
import plotly.express as px
import streamlit as st
from google.oauth2 import service_account
import matplotlib
import tqdm
import time

file = "C:\\Users\\promind\\.kodai\\meteo-bigquery-api.json"
pandas_gbq.context.credentials = service_account.Credentials.from_service_account_file(file)
pandas_gbq.context.project = "meteo-372509"

# df = pandas_gbq.read_gbq("SELECT * from temp.forecast_deviation_by_age")
# forecast = pandas_gbq.read_gbq("SELECT * from temp.forecast_with_observation")
# df.to_parquet('forecast_deviation_by_age.parquet')
# forecast.to_parquet('forecast_with_observation.parquet')

df = pd.read_parquet('forecast_deviation_by_age.parquet')
#forecast = pd.read_parquet('forecast_with_observation.parquet')

dict = {'vilnius':'vilniaus-ams', 'kaunas':'kauno-ams', 'klaipeda':'klaipedos-ams', 'silute':'silutes-ams', 'birzai':'birzu-ams', 'utena':'utenos-ams', 'varena':'varenos-ams'}
#places = ['vilnius', 'kaunas', 'klaipeda', 'silute', 'neringa-nida', 'birzai', 'utena', 'varena']

metrics = {'Temperatūra, C':'air_temp_dev', 
            'Krituliai':'precipit_dev',
            'Vėjas, ms': 'wind_speed_dev',
            'Vėjo kryptis':'wind_dir_dev',
            'Debesuotumas':'cloud_dev',
            'Slėgis':'pressure_dev',
            'Humidity':'humidity_dev' }
		
# ------GUI ---------------------
col1, col2 = st.columns(2)

# Using object notation
add_selectbox = st.sidebar.selectbox(
    "How would you like to be contacted?",
    ("Email", "Home phone", "Mobile phone")
)

# Using "with" notation
with st.sidebar:
    add_radio = st.radio(
        "Choose a shipping method",
        ("Standard (5-15 days)", "Express (2-5 days)")
    )

with col1:
    chart_type = st.radio(
        "Analizė",
        ('Vidurkis', 'Pagal miestą', 'Visi miestai'))

with col2: 
    metric_choice = st.selectbox( 'Rodiklis: ', metrics.keys())
    
    if chart_type == 'Pagal miestą':
        place = st.selectbox( 'Pasirinkite vietovę: ', dict.keys())

# chart
if chart_type == 'Pagal miestą':
    agg_df = df[df['fo_place_code'] == place].sort_values(by=['fo_age'])
    fig = px.line(agg_df, x='fo_age', y=metrics[metric_choice], title=metric_choice, markers=True)

elif chart_type == 'Visi miestai':
    agg_df = df.groupby(['fo_place_code', 'fo_age'], as_index=False).mean().round(1)
    fig = px.line(agg_df, x='fo_age', y=metrics[metric_choice], color='fo_place_code', title=metric_choice, markers=True)

else: # vidurkis
    agg_df = df.groupby(['fo_age'], as_index=False).mean().round(1).sort_values(by=['fo_age'])
    agg_df = agg_df.sort_values(by=['fo_age'])
    fig = px.line(agg_df, x='fo_age', y=metrics[metric_choice], title=metric_choice, markers=True)


#fig.update(layout_yaxis_range = [0,agg_df['air_temp_dev'].max()+2])
fig.update(layout_xaxis_range = [-0.2,7.5])

st.plotly_chart(fig, theme="streamlit", use_container_width=True)


show_data = st.checkbox('Rodyti duomenis')

if show_data:
    st.dataframe(agg_df)
