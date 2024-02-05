# Airflow Weather Data
Small project to get familiar with data pipelines and DAGs in Airflow. 

![Cover photo](berlin_weather_photo.jpg)
 
## Weather data pipeline
I programmed a simple pipeline that connects to a weather API and downloads current wheather data for an area in Berlin in .txt and .csv format. The pipeline works successfully.  
Project and code are modified after [this blog article](https://medium.com/@thallyscostalat/easy-data-pipeline-automation-with-apache-airflow-and-python-83a13e8f67e9).

## Next steps
The code was written in VSCode in an Anaconda environment. As this requires constant restarts of the webserver, I am planning to implement the pipeline in a Docker container environment next.

## Acknowledgment
Cover photo by <a href="https://unsplash.com/@meshaal_hajali?utm_content=creditCopyText&utm_medium=referral&utm_source=unsplash">Meshaal Al Hajali</a> on <a href="https://unsplash.com/photos/a-tall-tower-with-a-gold-top-with-berlin-victory-column-in-the-background-Etufuqt627s?utm_content=creditCopyText&utm_medium=referral&utm_source=unsplash">Unsplash</a>
