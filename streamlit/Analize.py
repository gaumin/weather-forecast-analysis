import pandas_gbq
import pandas as pd
import plotly.express as px
import streamlit as st
from google.oauth2 import service_account

pandas_gbq.context.credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
pandas_gbq.context.project = "meteo-372509"

df = pandas_gbq.read_gbq("SELECT * from PROD.forecast_deviation_by_age")
# forecast = pandas_gbq.read_gbq("SELECT * from PROD.forecast_with_observation")
# df.to_parquet('forecast_deviation_by_age.parquet')
# forecast.to_parquet('forecast_with_observation.parquet')
# df = pd.read_parquet('forecast_deviation_by_age.parquet')
# forecast = pd.read_parquet('forecast_with_observation.parquet')

dict = {'vilnius':'vilniaus-ams', 
            'kaunas':'kauno-ams', 
            'klaipeda':'klaipedos-ams',
            'silute':'silutes-ams',
            'birzai':'birzu-ams',
            'utena':'utenos-ams',
            'varena':'varenos-ams'}

metrics = {'Temperatūra, C':'air_temp_dev', 
            'Krituliai, mm':'precipit_dev',
            'Vėjas, ms': 'wind_speed_dev',
            'Vėjo kryptis, °':'wind_dir_dev',
            'Debesuotumas, %':'cloud_dev',
            'Slėgis, hPa':'pressure_dev',
            'Santykinė oro drėgmė, %':'humidity_dev' }

labels={'fo_place_code': 'Miestas',
        'fo_age': 'Prognozės laikotarpis (dienos)',
        'air_temp_dev': 'Temperatūros skirtumas, C',
        'wind_speed_dev': 'Vėjo skirtumas, ms',
        'wind_dir_dev': 'Vėjo krypties skirtumas, laipsniai',
        'cloud_dev': 'Debesuotumo skirtumas, %',
        'pressure_dev':'Slėgio skirtumas, hPa',
        'humidity_dev': 'Santykinė oro drėgmės skirtumas, %',
        'precipit_dev': 'Kritulių kiekio skitumas, mm' }     
		
# GUI
st.header(':bar_chart: Orų prognozės tikslumas')
col1, col2 = st.columns(2)

with st.sidebar:
    st.markdown('Orų **prognozės tiklsumo** analizė. Duomenų šaltinis (api.meteo.lt)')
    st.markdown('Grafike pateikiamas vidutinis prognozės tikslumas priklausiai nuo prognuozojamo laikotarpio dienomis.')

with col1:
    chart_type = st.radio(
        "Analizė",
        ('Vidurkis', 'Pagal miestą', 'Visi miestai'))

with col2: 
    metric_choice = st.selectbox( 'Rodiklis: ', metrics.keys())
    
    if chart_type == 'Pagal miestą':
        place = st.selectbox( 'Miestas: ', dict.keys())

# Chart
if chart_type == 'Pagal miestą':
    agg_df = df[df['fo_place_code'] == place].sort_values(by=['fo_age'])
    fig = px.line(agg_df, x='fo_age', y=metrics[metric_choice], labels=labels, title=metric_choice, markers=True)

elif chart_type == 'Visi miestai':
    agg_df = df.groupby(['fo_place_code', 'fo_age'], as_index=False).mean().round(1)
    fig = px.line(agg_df, x='fo_age', y=metrics[metric_choice], labels=labels, color='fo_place_code', title=metric_choice, markers=True)
 
else: 
    agg_df = df.groupby(['fo_age'], as_index=False).mean().round(1).sort_values(by=['fo_age'])
    agg_df = agg_df.sort_values(by=['fo_age'])
    fig = px.line(agg_df, x='fo_age', y=metrics[metric_choice], labels=labels, title=metric_choice, markers=True)


# fig.update(layout_yaxis_range = [0,agg_df['air_temp_dev'].max()+2])
fig.update(layout_xaxis_range = [-0.2,7.2])


st.plotly_chart(fig, theme="streamlit", use_container_width=True)


show_data = st.checkbox('Rodyti duomenis')

if show_data:
    st.dataframe(agg_df)
