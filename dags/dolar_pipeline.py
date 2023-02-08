from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/home/ktulhux/Documentos/AirflowAlura")
from projetos.CotacaoMoedas.extrai_cotacoes import DollarPipeline

default_args = {
    "owner": "Wilberhg",
    "start_date": datetime(2022,2,7),
    "depends_on_past": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="dolar_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(hours=1)
)

dollar_pipeline = DollarPipeline()

collect_data_task = PythonOperator(
    task_id="collect_data",
    python_callable=dollar_pipeline.collect_data,
    op_kwargs={"currency": "USD"},
    dag=dag
)

clean_data_task = PythonOperator(
    task_id="clean_data",
    python_callable=dollar_pipeline.clean_data,
    dag=dag
)

store_data_task = PythonOperator(
    task_id="store_data",
    python_callable=dollar_pipeline.store_data,
    dag=dag
)

collect_data_task >> clean_data_task >> store_data_task