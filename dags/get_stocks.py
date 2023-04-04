import yfinance
from airflow.decorators import dag, task
from airflow.macros import ds_add
# ds_add tem como objetivo fazer calculos em datas
from pathlib import Path
import pendulum

TICKERS = [
    "AAPL",
    "MSFT",
    "GOOG",
    "TSLA"
]

@task()
def get_history(ticker: str, ds: str = None, ds_nodash: str = None):
    """_summary_

    Args:
        ticker (str): _description_
        ds (str, optional): Variavel do Airflow (em tempo de execuçao) que traz a data formatada visualmente. Defaults to None.
        ds_nodash (str, optional): Variavel do Airflow (em tempo de execuçao) que traz a data em formato de uso. Defaults to None.
    """
    file_path = f"/home/ktulhu/Desenvolvimento/Airflow/stocks/{ticker}/{ticker}_{ds_nodash}.csv"
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    yfinance.Ticker(ticker).history(
        period="1d",
        interval="1h",
        start = ds_add(ds, -1), # Subtrai um dia da variavel do Airflow
        end = ds,
        prepost = True
    ).to_csv(file_path)
    
@dag(
    schedule_interval="0 0 * * 2-6", # Meia noite, todos os dias do mes, de terca a sabado
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), # Data em que o Airflow comeca a executar a DAG
    catchup=True # Forca o Airflow a executar desde a data descrita acima (caso False, o mesmo roda em d0)
)
def get_stocks_dag():
    for ticker in TICKERS:
        get_history.override(task_id=ticker)(ticker)
        
dag = get_stocks_dag()