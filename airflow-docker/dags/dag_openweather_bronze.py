from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'Ruanan',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'openweather_bronze_ingestion',
    default_args=default_args,
    description='Orquestra o container Docker do extrator OpenWeather para a Camada Bronze',
    schedule='@daily',  # Padrão moderno do Airflow 2.10+ / 3.0
    catchup=False,
    tags=['bronze', 'openweather', 'api'],
) as dag:

    run_extractor = DockerOperator(
        task_id='run_api_extractor',
        image='openweather-extractor:latest',
        api_version='auto',
        auto_remove='success',  # Comportamento de string exigido pelas versões novas
        command='python extract.py',
        #docker_url='unix://var/run/docker.sock',  # Ponte de comando com o Docker do Mac
        docker_url='unix:///var/run/docker.sock',
        
        # Injeta as credenciais salvas no Admin -> Connections de forma segura
        environment={
            'OPENWEATHER_API_KEY': "{{ conn.openweather_api.password }}",
            'AZURE_STORAGE_CONNECTION_STRING': "{{ conn.azure_datalake.password }}"
        },
    )

    run_extractor