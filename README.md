## **English**
![proyect cover](images/project_cover.png)
---
This project implements a pipeline for 
extracting, cleaning, and storing meteorological data. It uses Apache Airflow for orchestration, Pandas for data cleansing, and Snowflake for data cleansing.

## **ESPAÑOL**
![proyect cover](images/project_cover.png)
---
Este proyecto implementa un pipeline para la extracción, limpieza y almacenamiento de datos meteorológicos. Utiliza Apache Airflow para la orquestación, Pandas para la limpieza de datos y Snowflake como base de datos para almacenar los datos procesados.db 

# CÓMO FUNCIONA
---
Este proyecto utiliza un DAG de Apache Airflow llamado weather_dag.py, el cual está compuesto por tres tareas principales:

1. Extracción de datos: Se conecta a la API de OpenWeatherMap para obtener datos meteorológicos actuales.

2. Limpieza de datos: Utiliza Pandas para procesar y limpiar la información obtenida.

3. Carga de datos: Guarda los datos limpios en formato CSV y los carga en una tabla de Snowflake.
---
# CAPTURAS 

## Visualización del flujo en Airflow
![proyect cover](images/screenshots/airflow_setup.jpg)

## Visualización de los datos en Snowflake
![proyect cover](images/screenshots/snowflake_setup.png)
