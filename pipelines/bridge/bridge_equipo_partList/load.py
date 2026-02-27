from pipelines.bridge.bridge_equipo_partList.config  import COLUMN_MAPPING, TABLA_DESTINO
from pipelines.bridge.bridge_equipo_partList.extract import extract
from utils.starrocks_stream_loader import stream_load

def load():
    csv_path = extract()
    stream_load(csv_path, COLUMN_MAPPING, TABLA_DESTINO)

if __name__ == "__main__":
   load()

# python3 -m pipelines.bridge.bridge_equipo_partList.load
