from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import snowflake.connector

def get_data():
    api_key = ""
    city = "Buenos Aires"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    
    data = requests.get(url).json()

    df = pd.DataFrame([{
        "city": data["name"],
        "temperature_c": data["main"]["temp"] - 273.15,
        "humidity": data["main"]["humidity"],
        "date": pd.to_datetime("now")
    }])
    df.to_csv("/tmp/weather.csv", index=False)
    print("Data retrieved and saved to CSV.")

def clean_data():
    df = pd.read_csv("/tmp/weather.csv")
    df["city"] = df["city"].str.title()  
    df.to_csv("/tmp/cleaned_weather.csv", index=False)
    print("Data cleaned and saved to cleaned CSV.")

def load_to_snowflake():
    df = pd.read_csv("/tmp/cleaned_weather.csv")
    
    conn = snowflake.connector.connect(
        user="",
        password="",
        account=""
    )
    cursor = conn.cursor()

    cursor.execute("USE DATABASE clima_db;")
    cursor.execute("USE SCHEMA clean_data;")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS current_weather (
        city STRING,
        temperature FLOAT,
        humidity FLOAT,
        date TIMESTAMP
    );
    """)
    
    for _, row in df.iterrows():
        cursor.execute(f"""
        INSERT INTO current_weather (city, temperature, humidity, date) 
        VALUES ('{row['city']}', {row['temperature_c']}, {row['humidity']}, '{row['date']}');
        """)

    print("Data loaded into Snowflake.")
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 25),
    'retries': 1,
}
dag = DAG(
    'weather_pipeline',
    default_args=default_args,
    schedule_interval='@hourly', 
)

task_get_data = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag,
)

task_clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

task_load_to_snowflake = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag,
)

task_get_data >> task_clean_data >> task_load_to_snowflake