import pandas_gbq
import pandas as pd
import plotly.express as px
import streamlit as st
from google.oauth2 import service_account
from datetime import datetime, timedelta
from datetime import date


# Title page and footer
st.title = "Prognozė"

file = "C:\\Users\\promind\\.kodai\\meteo-bigquery-api.json"
pandas_gbq.context.credentials = service_account.Credentials.from_service_account_file(file)
pandas_gbq.context.project = "meteo-372509"

#forecast = pandas_gbq.read_gbq("SELECT * from temp.latest_forecast_by_place")
#forecast.to_parquet('latest_forecast_by_place.parquet')

forecast = pd.read_parquet('latest_forecast_by_place.parquet')

dict = {'vilnius':'vilniaus-ams', 'kaunas':'kauno-ams', 'klaipeda':'klaipedos-ams', 'silute':'silutes-ams', 'birzai':'birzu-ams', 'utena':'utenos-ams', 'varena':'varenos-ams'}

metrics = {'Temperatūra, C':'fo_air_temp', 
            'Krituliai':'fo_precipit',
            'Vėjas, ms': 'fo_wind_speed',
            'Vėjo kryptis':'fo_wind_dir',
            'Debesuotumas':'fo_cloud',
            'Slėgis':'fo_pressure',
            'Humidity':'fo_humidity' }


	
# ------GUI ---------------------
col1, col2 = st.columns(2)

period = st.slider('Prognozės laikotarpis (7 dienos max)', 1, 7, 4)
st.write("Prognozės laikotarpis", period, 'dienos')

limit_date = date.today() + timedelta(days=period+1) 

st.write("Limit date ", limit_date)



with col1:
    place = st.selectbox( 'Pasirinkite vietovę: ', dict.keys())

with col2: 
    metric_choice = st.selectbox( 'Rodiklis: ', metrics.keys())

agg_df = forecast.query('fo_date < @limit_date' )
agg_df = agg_df[agg_df['fo_place_code'] == place].sort_values(by=['fo_datetime'])
fig = px.line(agg_df, x='fo_datetime', y=metrics[metric_choice], title=metric_choice, markers=True)

#fig.update(layout_yaxis_range = [0,agg_df['air_temp_dev'].max()+2])
#fig.update(layout_xaxis_range = [-0.2,7.5])
st.plotly_chart(fig, theme="streamlit", use_container_width=True)


fo_creation_date = forecast['fo_creation_date'].max()
fo_creation_time = forecast['fo_creation_time'].max()
fo_loaded_date = forecast['fo_loaded_date'].max()

st.write("Prognozės sudarymo data (meteo.lt): ", fo_creation_date, ' ', fo_creation_time )
st.write("Prognozės duomenų nuskaitymo data: ", fo_loaded_date)


show_data = st.checkbox('Rodyti duomenis')



if show_data:
    st.dataframe(agg_df)

