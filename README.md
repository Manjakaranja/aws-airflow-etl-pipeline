# AWS Airflow ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.0+-red.svg)
![AWS](https://img.shields.io/badge/AWS-EC2%20%7C%20S3-orange.svg)

A production-ready ETL pipeline that extracts weather data from OpenWeather API, transforms it, and loads it to AWS S3. Built with Apache Airflow and deployed on AWS EC2.

## Architecture
```
OpenWeather API → Apache Airflow (EC2) → AWS S3
```

## Features

- Multi-city weather data collection (Paris, Seoul, Tokyo, Seattle, Antananarivo)
- Automated daily pipeline execution
- Parallel task processing
- Temperature conversion (Kelvin to Celsius)
- Timestamped CSV storage in S3

## Tech Stack

- Apache Airflow 2.x
- Python 3.8+ (Pandas)
- AWS EC2 & S3
- OpenWeather API

## Pipeline Workflow

1. **API Health Check**: HttpSensor validates OpenWeather API availability for each city
2. **Extract**: HttpOperator fetches weather data in parallel for all 5 cities via GET requests
3. **Transform**: PythonOperator converts temperatures (K→C), processes timestamps (Unix→UTC), and structures data into DataFrame
4. **Load**: Pandas exports to timestamped CSV and uploads directly to S3 bucket

**Task Dependencies:**
```
API Check → Extract (parallel) → Transform & Load
```

## Project Structure
```
aws-airflow-etl-pipeline/
├── dags/
│   └── weather_dag.py      # Main ETL DAG
├── data/                    # CSV output files
└── logs/                    # Airflow logs
```

## Configuration

Airflow variables:
```bash
WEATHER_API_KEY=your_api_key
```

Airflow HTTP connection:
```
Conn Id: weathermap_api
Host: https://api.openweathermap.org
```

## Data Output

CSV files with columns: City, Country, Description, Temperature (C), Feels Like (C), Min/Max Temp, Pressure, Humidity, Wind Speed, Time of Record, Sunrise/Sunset times.

Stored in: `s3://weather-data-etl-s3/current_weather_data_multi_city_DDMMYYYYHHMMSS.csv`

## Author

**Manjakaranja**
- GitHub: [@Manjakaranja](https://github.com/Manjakaranja)

## License

MIT License