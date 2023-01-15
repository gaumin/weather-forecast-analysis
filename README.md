# Weather Forecast Analysis

![Principle scheme of implementation infrastructure](https://github.com/gaumin/weather-forecast-analysis/blob/main/images/ELT.png)

## Project objective ##
Analyze the accuracy of a local weather forecasting service in Lithuania, specifically the website meteo.lt. 
To achieve this objective, these tasks must be completed:
1.	Collect weather forecast data for a specific time interval
2.	Collect weather observation data for the same time interval as forecast 
3.	Analyze collected data, compare forecast vs observation
4.	Visualize results of analysis 

## Data source ##
The data source for the project is a free API ([api.meteo.lt](https://api.meteo.lt/)) with a request limit of 20000 requests per 24 hours per IP address. 
## Results ##
The final result of the project is an online tool hosted on the Streamlit Cloud platform, which can be accessed at **[meteo-forecast.streamlit.app](https://meteo-forecast.streamlit.app)**.

## Tools used ## 
* **Raspberry Pi** - a server for python to run the script 24/7 for data retrieval from API and storing to the data warehouse (BigQuery)
* **BigQuery** - a data warehouse for storing the raw and transformed data. 
* **DBT** - a tool that enables data transformations in warehouses. 
* **DBT Cloud** - a hosted service to deploy Dbt transformations. 
* **Python, Pandas, Plotly** - for manipulating data and creating visualizations (charts)
* **Streamlit** - a python library for a fast and easy way to create web apps. 
* **Streamlit Cloud** - an open and free platform to deploy and share Streamlit apps"

## Approach
In this project, the approach chosen to handle the data pipeline was the ELT (Extract, Load, Transform) method. This approach involves transforming data after it has been loaded onto the data warehouse, as opposed to the traditional ETL (Extract, Transform, Load) method where data is transformed before loading. By pushing the transformation step to the target database, the ELT method can improve performance

## Extract:
In this layer raw data is pulled from an API using custom Python script. There are two main data objects we are interested:
1.	Weather forecast for the specific locations (including various metrics, such air temperature, precipitation, air pressure, etc).
2.	Weather observation data from meteorological stations. Those stations must be close to the locations we chose to track the forecast (in order we want to compare them together).

Python scripts was deployed on Raspberry PI which is connected to my wifi network and works 24/7. 
To run data extraction and data loading task periodically (each 3 hours) pythons library ‘schedule’ is used. This means python’s scrip needs to be executed only once using bash, and ‘schedule’ takes care of automatic scheduling for data imports. 
### Load
Raw data is loaded directly to BigQuery without any data transformation applied. For example, the API returns date columns as a string. This could be easily fixed in a Python extract-load script, it is not in line with the Extract, Load, Transform (ELT) methodology. Therefore, all raw data is loaded directly to BigQuery. Only completely necessary (or repeated) data is dropped at this stage.
Although, Raspberry copes with the data extract and load task without any difficulty, this is the only service in a data pipeline which works on-premises. In the long-term it would make sense to move this script to the cloud as the rest of the components already are.
### Transform
Major transformations of data are performed on this layer using the Data Build Tool (DBT) with SQL and Jinja templating being used to perform data transformations. DBT (Data Build Tool) is a command line tool that helps analysts write and manage data transformation scripts and it can be run on any cloud provider or on-premise.
It is important to note that BigQuery is responsible for performing all transformations, not DBT. DBT's role is limited to compiling and running SQL queries on the supported data platform. 

Key benefits of using DBT include the ability to:
* Apply data transformations to raw data, perform tests, and implement code versioning.
* Maintain data documentation and definitions within DBT.
* Build reusable and modular code using macros and Jinja
DBT Cloud is a cloud-based version of the DBT, which provides a platform to run and schedule data transformation jobs and also provides a free option for one project for one developer.
### Analyze
After data is cleaned and transformed final analysis is applied on this layer. Custom Python with Pandas is used here to manipulate and visualize data.
Finally, analysis result is build as an interactive web app using Streamlit. Furthermore, Streamlit Cloud allows you to host this program in the cloud for free.
After the data is cleaned and transformed, final analysis is applied on this layer. Python with Pandas is used here to manipulate and visualize the data. Finally, the analysis results are built as an interactive web app using Streamlit. Furthermore, Streamlit Cloud allows you to host this app in the cloud for free.
