import requests, csv, pandas as pd, json, logging, sys, http.client
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor 
from typing import List

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M',
    stream=sys.stdout
)

class DollarPipeline:
    def __init__(self):
        self.moeda_padrao = "BRL"

    def collect_data(self, currency="USD"):
        response = requests.get(f"https://open.er-api.com/v6/latest/{currency}")
        self.data = response.json()
        logging.debug(self.data)
        return self.data

    def clean_data(self):
        self.rate = self.data["rates"][self.moeda_padrao]
        self.currency = self.data["base_code"]
        logging.debug(f"{self.rate}, {self.currency}")
        return self.rate, self.currency

    def store_data(self):
        with open('exchange.csv', mode="w") as file:
            writer = csv.writer(file, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(["DATE", "CURRENCY", "RATE"])
            writer.writerow([self.data["time_last_update_utc"], self.currency, self.rate])

class DollarPipelineOptimized:
    def __init__(self):
        self.moeda_padrao = "BRL"
        
    lru_cache()
    def get_exchange_rate(self, currency):
        conn = http.client.HTTPSConnection("open.er-api.com")
        conn.request("GET", f"/v6/latest/{currency}")
        res = conn.getresponse()
        data = res.read()
        conn.close()
        logging.debug(data)
        return json.loads(data)

    def collect_data_threading(self, currencies: List[str] = ["USD","EUR"]):
        with ThreadPoolExecutor(max_workers=4) as executor:
            responses = [executor.submit(self.get_exchange_rate, currency) for currency in currencies]
        self.data = [json_response.result() for json_response in responses]
        logging.debug(self.data)

    def clean_data_pandas(self):
        df = pd.DataFrame(self.data)
        df = df.loc[:, ["base_code", "time_last_update_utc", "rates"]]
        df["rate"] = df["rates"].apply(lambda x: x["BRL"])
        df = df[["time_last_update_utc", "base_code", "rate"]]
        self.data = df
        logging.debug(self.data)

    def store_data(self):
        with open('exchange_rate_optimized.csv', mode='w', newline='') as file:
            writer = csv.writer(file, delimiter=";")
            writer.writerow(["DATE", "CURRENCY", "RATE"])
            for index, row in self.data.iterrows():
                writer.writerow([row["time_last_update_utc"], row["base_code"], row["rate"]])

from time import perf_counter
if __name__ == '__main__':
    start_time = perf_counter()
    obj = DollarPipeline()
    data = obj.collect_data()
    rate, currency = obj.clean_data()
    obj.store_data()
    end_time = perf_counter()
    print(f"A execução desde script levou {end_time - start_time} segundos.")

# if __name__ == '__main__':
#     start_time = perf_counter()
#     obj = DollarPipelineOptimized()
#     data = obj.collect_data_threading()
#     obj.clean_data_pandas()
#     obj.store_data()
#     end_time = perf_counter()
#     print(f"A execução desde script levou {end_time - start_time} segundos.")