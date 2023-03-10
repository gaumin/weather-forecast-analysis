import pandas_gbq
import pandas as pd
import plotly.express as px
import streamlit as st
from google.oauth2 import service_account
from datetime import date, datetime, timedelta

st.title = "Prognozė :barely_sunny:"

pandas_gbq.context.credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
pandas_gbq.context.project = "meteo-372509"

forecast = pandas_gbq.read_gbq("SELECT * from PROD.latest_forecast_by_place")

dict = {'vilnius':'vilniaus-ams',
         'kaunas':'kauno-ams',
         'klaipeda':'klaipedos-ams',
         'silute':'silutes-ams',
         'birzai':'birzu-ams',
         'utena':'utenos-ams',
         'varena':'varenos-ams'}

metrics = {'Temperatūra, C':'fo_air_temp', 
            'Krituliai, mm':'fo_precipit',
            'Vėjas, ms': 'fo_wind_speed',
            'Vėjo kryptis, °':'fo_wind_dir',
            'Debesuotumas, %':'fo_cloud',
            'Slėgis, hPa':'fo_pressure',
            'Santykinė oro drėgmė, %':'fo_humidity' }

labels={'fo_place_code': 'Miestas',
        'fo_air_temp': 'Temperatūra, C',
        'fo_precipit': 'Krituliai, mm',
        'fo_wind_speed': 'Vėjas, ms',
        'fo_wind_dir': 'Vėjo kryptis, °',
        'fo_cloud': 'Debesuotumas, %',
        'fo_pressure':'Slėgis, hPa',
        'fo_humidity': 'Santykinė oro drėgmė, %',
         'fo_datetime': 'Data' }    

# ------GUI ---------------------
st.header('🌥️ Orų prognozė')
col1, col2 = st.columns(2)

period = st.slider('Prognozės laikotarpis (7 dienos max)', 1, 7, 4)
st.write("Prognozės laikotarpis", period, 'dienos')

limit_date = date.today() + timedelta(days=period+1) 

with col1:
    place = st.selectbox( 'Pasirinkite vietovę: ', dict.keys())

with col2: 
    metric_choice = st.selectbox( 'Rodiklis: ', metrics.keys())


agg_df = forecast.query('fo_date < @limit_date' )
agg_df = agg_df[agg_df['fo_place_code'] == place].sort_values(by=['fo_datetime'])

fig = px.line(agg_df, x='fo_datetime', y=metrics[metric_choice], labels=labels, title=metric_choice, markers=True)

fig.update({'layout': {'xaxis': {'fixedrange':True}, 'yaxis': {'fixedrange':True}}} )
 
config={'scrollZoom': True,
        'displaylogo': False,
        'modeBarButtonsToRemove': ['lasso2d', 'select2d']}

st.plotly_chart(fig, theme="streamlit", use_container_width=True, config=config)

fo_creation_date = forecast['fo_creation_date'].max()
fo_creation_time = forecast['fo_creation_time'].max()
fo_loaded_date = forecast['fo_loaded_date'].max().replace(microsecond=0)

st.write("Prognozės sudarymo laikas (meteo.lt): ", fo_creation_date, ' ', fo_creation_time )
st.write("Prognozės duomenų nuskaitymo laikas: ", fo_loaded_date.date(), fo_loaded_date.time())
show_data = st.checkbox('Rodyti duomenis')

if show_data:
    st.dataframe(agg_df)

