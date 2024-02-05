# Modified after: https://medium.com/@thallyscostalat/easy-data-pipeline-automation-with-apache-airflow-and-python-83a13e8f67e9

### Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
#from datetime import datetime
import datetime as dt
import pandas as pd
import requests
import meteomatics.api as api

### Functions
# Configure API Connector
# https://github.com/meteomatics/python-connector-api/blob/master/examples/notebooks/00_Overview.ipynb

def fetch_weather_data():
    username = ### input meteomatics account data here ###
    password = ### input meteomatics account data here ###

    coordinates = [(52.49, 13.44)]
    parameters = ['t_2m:C', 'weather_symbol_1h:idx', 'uv:idx'] # Instantaneous temperature, weather symbol last 60m, UV index
    model = 'mix' # More info: https://www.meteomatics.com/en/api/request/optional-parameters/data-source/
    startdate = dt.datetime.now().replace(second=0, microsecond=0)
    enddate = startdate #+ dt.timedelta(days=1)
    interval = dt.timedelta(hours=1)

    data = api.query_time_series(coordinates, startdate, enddate, interval, parameters, username, password, model=model)

    temporary_df = data.copy()
    temporary_df.reset_index(inplace=True)
    temporary_df.to_csv('/home/seblap/Files/Arbeit/Data Analytics/GitHub repos/airflow_weather_data/temp/pipeline_temporary_df.csv', index=False)
    
    return data

# Define the function to save data to a text file
def save_to_txt_file(data):
    with open("/home/seblap/Files/Arbeit/Data Analytics/GitHub repos/airflow_weather_data/data_collection/weather_data.txt", "a") as file:
        file.write(str(data))
        file.write("\n")

# Save the data into (updated) csv table
def save_to_csv_file():
    data = pd.read_csv('/home/seblap/Files/Arbeit/Data Analytics/GitHub repos/airflow_weather_data/temp/pipeline_temporary_df.csv')
    current_file = pd.read_csv('/home/seblap/Files/Arbeit/Data Analytics/GitHub repos/airflow_weather_data/data_collection/weather_data.csv')
    new_entry = data.reset_index()
    updated_file = pd.concat([current_file, new_entry], axis=0)
    updated_file = updated_file[['validdate', 'lat', 'lon', 't_2m:C', 'weather_symbol_1h:idx', 'uv:idx']]
    updated_file.to_csv('/home/seblap/Files/Arbeit/Data Analytics/GitHub repos/airflow_weather_data/data_collection/weather_data.csv', index=False)

### Airflow
# Define the Airflow DAG (Directed Acyclic Graph)
dag = DAG(
    'pipeline_weather_berlin',
    description='Pipeline Weather Berlin',
    schedule='@daily',
    start_date=dt.datetime.utcnow(),
)

# Define the tasks in the DAG
task_fetch_weather_data = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag
)

task_save_to_txt_file = PythonOperator(
    task_id='save_to_txt_file',
    python_callable=save_to_txt_file,
    op_kwargs={'data': '{{ ti.xcom_pull(task_ids="fetch_weather_data") }}'},  # Get data from the fetch task
    dag=dag
)

task_save_to_csv_file = PythonOperator(
    task_id='save_to_csv_file',
    python_callable=save_to_csv_file,
#    op_kwargs={'data': '{{ ti.xcom_pull(task_ids="fetch_weather_data") }}'},  # Get data from the fetch task - does not work due to DataFrame type (instead, temporarily saved and re-loaded a .csv file)
    dag=dag
)

# Define the dependencies between tasks
task_fetch_weather_data >> task_save_to_txt_file
