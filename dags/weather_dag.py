from airflow import DAG
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd

WEATHER_API_KEY = Variable.get("WEATHER_API_KEY")
cities = ['Paris', 'Seoul', 'Tokyo', 'Seattle', 'Antananarivo']

def kelvin_to_celcius(temp_in_kelvin):
    temp_in_celcius = (temp_in_kelvin - 273.15)
    return temp_in_celcius

def transform_load_data(task_instance):
    transformed_datas = []

    for city in cities:

        data = task_instance.xcom_pull(task_ids=f'extract_weather_data_{city}')
        city_name = data['name']
        country = data['sys']['country']
        weather_description = data['weather'][0]['description']
        temp_celcius = kelvin_to_celcius(data['main']['temp'])
        feels_like_celcius =kelvin_to_celcius(data['main']['feels_like'])
        temp_min = kelvin_to_celcius(data['main']['temp_min'])
        temp_max = kelvin_to_celcius(data['main']['temp_max'])
        humidity = data['main']['humidity'] 
        pressure = data['main']['pressure']
        wind_speed_meterpersecond = data['wind']['speed']
        time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
        sunrise_local_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_local_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

        transformed_data = {"City": city_name,
                        "Country": country,
                        "Description": weather_description,
                        "Temperature (C)": temp_celcius,
                        "Feels Like (C)": feels_like_celcius,
                        "Minimun Temp (C)":temp_min,
                        "Maximum Temp (C)": temp_max,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed M/S": wind_speed_meterpersecond,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_local_time,
                        "Sunset (Local Time)": sunset_local_time                        
                        }
        
    

        transformed_datas.append(transformed_data)

    df_data = pd.DataFrame(transformed_datas)
    
    now = datetime.now()
    dt_string = now.strftime('%d%m%Y%H%M%S')
    filename = f'current_weather_data_multi_city_{dt_string}'

    df_data.to_csv(f's3://weather-data-etl-s3/{filename}.csv', index=False)



default_args = {
    'owner':'manjaka',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 25),
    'email': ['notarealmail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)

}

with DAG('weather_dag',
        default_args=default_args,
        schedule = '@daily',
        catchup=False) as dag:

        

        for city in cities:
            extract_weather_datas = []
            extract_tasks = []

            is_weather_api_ready = HttpSensor(
            task_id = f'is_weather_api_ready_{city}',
            http_conn_id='weathermap_api',
            endpoint=f'/data/2.5/weather?q={city}&APPID={WEATHER_API_KEY}'
            )

            extract_weather_data = HttpOperator(
                task_id = f'extract_weather_data_{city}',
                http_conn_id = 'weathermap_api',
                endpoint=f'/data/2.5/weather?q={city}&APPID={WEATHER_API_KEY}',
                method = 'GET',
                response_filter= lambda r: json.loads(r.text),
                log_response = True 
            )
            extract_weather_datas.append(extract_weather_data)

            is_weather_api_ready >> extract_weather_data 
            extract_tasks.append(extract_weather_data)

        transform_load_weather_data = PythonOperator(
            task_id = 'transform_load_weather_data',
            python_callable = transform_load_data
        )

        extract_tasks >> transform_load_weather_data