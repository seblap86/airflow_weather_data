# airflow_weather_data
Small project to get familiar with data pipelines and DAGs in Airflow.  

## Weather data pipeline
I programmed a simple pipeline that connects to a weather API and downloads current wheather data for an area in Berlin in .txt and .csv format. The pipeline works successfully.  
The idea and code basis comes from [this blog article](https://medium.com/@thallyscostalat/easy-data-pipeline-automation-with-apache-airflow-and-python-83a13e8f67e9).

## Next steps
The code was written in VSCode in an Anaconda environment. As this requires constant restarts of the webserver, I am planning to implement the pipeline in a Docker container environment next.
