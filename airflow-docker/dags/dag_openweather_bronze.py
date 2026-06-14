from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'openweather_bronze_ingestion',
    default_args=default_args,
    description='Ingestão de dados climáticos para a camada Bronze',
    schedule='@hourly', # Atualizado de schedule_interval para schedule (padrão das novas versões do Airflow)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['bronze', 'ingestion'],
) as dag:

    # A task que sobe o container isolado do extrator
    run_api_extractor = DockerOperator(
        task_id='run_api_extractor',
        image='openweather-extractor:latest',
        api_version='auto',
        auto_remove='force', # Remove o container logo após ele terminar de rodar (limpa a memória do Mac)
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        
        # 👇 INJETANDO AS CREDENCIAIS NO CONTAINER 👇
        # Puxa do Airflow e entrega para o seu script extract.py
        environment={
            'OPENWEATHER_API_KEY': os.getenv('OPENWEATHER_API_KEY', ''),
            'AZURE_STORAGE_CONNECTION_STRING': os.getenv('AZURE_STORAGE_CONNECTION_STRING', ''),
        },
        
        # 👇 A CONFIGURAÇÃO QUE RESOLVE O SEU ERRO NO MAC M5 👇
        # Impede que o Airflow tente espelhar uma pasta temporária inexistente no host
        mount_tmp_dir=False, 
    )

    run_api_extractor