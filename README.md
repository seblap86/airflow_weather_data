# Airflow Weather Data
Small project to get familiar with data pipelines and DAGs in Airflow. 

![Cover photo](berlin_weather_photo.jpg)
 
## Weather data pipeline
I programmed a simple pipeline that connects to a weather API and downloads current wheather data for an area in Berlin in .txt and .csv format. The pipeline works successfully.  
  
In this case, information about the current temperature, the weather symbol (summary information of the last hour), and the UV index are downloaded (see [here](https://www.meteomatics.com/en/api/available-parameters/#api-basic) for an overview of the 15 variables in the Meteomatics weather API). 
  
Project and code are modified after [this blog article](https://medium.com/@thallyscostalat/easy-data-pipeline-automation-with-apache-airflow-and-python-83a13e8f67e9).

## How to use
In order to rebuild the project, you need to set up and run Airflow, e.g., as described in the aforementioned blog article. To connect to the Meteomatics weather API, you can create a free test account [here](https://www.meteomatics.com/en/sign-up-weather-api-test-account/).

## Next steps
To avoid constant manual restarts of the Apache web server, I am planning to get familiar with Docker and run the code in a container next for further automation.

## Acknowledgment
Cover photo by <a href="https://unsplash.com/@meshaal_hajali?utm_content=creditCopyText&utm_medium=referral&utm_source=unsplash">Meshaal Al Hajali</a> on <a href="https://unsplash.com/photos/a-tall-tower-with-a-gold-top-with-berlin-victory-column-in-the-background-Etufuqt627s?utm_content=creditCopyText&utm_medium=referral&utm_source=unsplash">Unsplash</a>.
