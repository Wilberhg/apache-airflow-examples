import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

data_inicio = data_inicio.strftime("%Y-%m-%d")
data_fim = data_fim.strftime("%Y-%m-%d")

city = "Boston"
key = os.environ.get("KEY")

URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')

dados = pd.read_csv(URL)

file_path = f"/home/ktulhux/Documentos/DataPipeline/semana={data_inicio}/"

os.makedirs(file_path, exist_ok=True)

dados.to_csv(file_path + 'dados_brutos.csv')
dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path+ 'temperaturas.csv')
dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
...
