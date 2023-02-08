from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

def cumprimentos():
    print("Boas-vindas ao Airflow!")

with DAG(
    "atividade_aula_4",
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:
    t1 = PythonOperator(
        task_id='atividade_aula_4',
        python_callable=cumprimentos,
    )

    t1