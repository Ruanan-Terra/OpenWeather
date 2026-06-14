from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'openweather_medallion_pipeline',
    default_args=default_args,
    description='Pipeline completo da API OpenWeather ao Parquet tratado na Silver',
    schedule='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['openweather', 'production'],
) as dag:

    # Task 1: Ingestão API -> Bronze (JSON)
    task_bronze = DockerOperator(
        task_id='run_api_extractor',
        image='openweather-extractor:latest',
        api_version='auto',
        auto_remove='force',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        force_pull=False,
        environment={
            'OPENWEATHER_API_KEY': os.getenv('OPENWEATHER_API_KEY', ''),
            'AZURE_STORAGE_CONNECTION_STRING': os.getenv('AZURE_STORAGE_CONNECTION_STRING', ''),
        },
        mount_tmp_dir=False,
    )

    # Task 2: Processamento Bronze -> Silver (Parquet no ADLS)
    task_silver = DockerOperator(
        task_id='run_silver_transformer',
        image='openweather-spark:latest',
        api_version='auto',
        auto_remove='force',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        force_pull=False,
        environment={
            'AZURE_STORAGE_CONNECTION_STRING': os.getenv('AZURE_STORAGE_CONNECTION_STRING', ''),
            'AZURE_STORAGE_KEY1': os.getenv('AZURE_STORAGE_KEY1', ''),
        },
        mount_tmp_dir=False,
    )

    # Define a sequência: Primeiro Bronze, depois Silver
    task_bronze >> task_silver