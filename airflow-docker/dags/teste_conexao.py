from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='teste_conexao_openweather',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['openweather', 'teste'],
) as dag:

    inicio = EmptyOperator(task_id='inicio_do_pipeline')
    fim = EmptyOperator(task_id='fim_do_pipeline')

    inicio >> fim