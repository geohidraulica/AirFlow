import os
import time
import base64
import requests
from config.settings import CONFIG
from load.dimensions.dim_estado_almacen.config import TMP_CSV, COLUMN_MAPPING
from load.dimensions.dim_estado_almacen.extrac import extract_to_csv

def stream_load():

    start_time = time.time()   # ⏱ inicio
    print("TMP_CSV:", TMP_CSV)
    print("Existe?", os.path.exists(TMP_CSV))


    print("Cargando datos a StarRocks (Stream Load)...")

    url = (
        f"http://{CONFIG['starrocks']['server']}:8040"
        f"/api/{CONFIG['starrocks']['database']}/DimEstadoAlmacen/_stream_load"
    )

    auth_str = f"{CONFIG['starrocks']['user']}:{CONFIG['starrocks']['pass']}"
    auth_base64 = base64.b64encode(auth_str.encode()).decode()

    headers = {
        "Authorization": f"Basic {auth_base64}",
        "label": f"dim_estado_almacen_{int(time.time())}",
        "format": "csv",
        "column_separator": "|",
        "columns": ",".join(COLUMN_MAPPING.values()),
        "Content-Type": "text/plain; charset=UTF-8",
        "Content-Length": str(os.path.getsize(TMP_CSV)),
        "Expect": "100-continue"
    }

    try:
        with open(TMP_CSV, "rb") as f:
            response = requests.put(
                url,
                headers=headers,
                data=f,
                timeout=600
            )

        end_time = time.time()   # ⏱ fin
        elapsed = end_time - start_time
        mins, secs = divmod(elapsed, 60)

        print("Respuesta StarRocks:")
        print(response.text)

        resp_json = response.json()

        if resp_json.get("Status") != "Success":
            raise Exception(f"Stream Load falló: {resp_json.get('Message')}")

        print(
            f"Stream Load exitoso: "
            f"{resp_json.get('NumberLoadedRows')} filas cargadas "
            f"en {int(mins)} min {secs:.2f} seg"
        )
    finally:
        if os.path.exists(TMP_CSV):
            os.remove(TMP_CSV)
            print("Archivo temporal eliminado:", TMP_CSV)


if __name__ == "__main__":

    extract_to_csv()
    stream_load()


#python -m load.dimensions.dim_estado_almacen.load
