from pipelines.facts.fact_compras.config  import COLUMN_MAPPING, TABLA_DESTINO
from pipelines.facts.fact_compras.extract import extract
from utils.starrocks_stream_loader import stream_load

def load():
    csv_path = extract()
    stream_load(csv_path, COLUMN_MAPPING, TABLA_DESTINO)

# if __name__ == "__main__":
#    load()

# python3 -m pipelines.facts.fact_compras.load
