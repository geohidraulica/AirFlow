from pipelines.dimensions.dim_site.config  import COLUMN_MAPPING, TABLA_DESTINO
from pipelines.dimensions.dim_site.extract import extract
from utils.stream_load import stream_load

def load():
    csv_path = extract()
    stream_load(csv_path, COLUMN_MAPPING, TABLA_DESTINO)

# if __name__ == "__main__":
#    load()

# python3 -m pipelines.dimensions.dim_site.load
