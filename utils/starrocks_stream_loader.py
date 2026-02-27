import os
import time
import base64
import requests
from config.settings import CONFIG
from utils.mysql_manager import MySQLManager
from config.mysql_connector import MySQLConnector


def stream_load(csv_path, columns, table_name):
    """
    Carga un CSV a StarRocks usando Stream Load.

    :param csv_path: Ruta del archivo CSV a cargar
    :param columns: Lista de nombres de columnas en el CSV
    :param table_name: Nombre de la tabla destino en StarRocks
    """

    # ⏱ Inicio medición precisa
    start_time = time.perf_counter()

    # print("TMP_CSV:", csv_path)
    # print("Cargando datos a StarRocks (Stream Load)...")

    pyodbc = MySQLManager()
    mysql = MySQLConnector(CONFIG["starrocks"])

    # print(f"Truncando tabla {table_name}...")
    
    pyodbc.execute_sql(f"TRUNCATE TABLE {table_name}", mysql)

    url = (
        f"http://{CONFIG['starrocks']['server']}:8040"
        f"/api/{CONFIG['starrocks']['database']}/{table_name}/_stream_load"
    )

    auth_str = f"{CONFIG['starrocks']['user']}:{CONFIG['starrocks']['pass']}"
    auth_base64 = base64.b64encode(auth_str.encode()).decode()

    headers = {
        "Authorization": f"Basic {auth_base64}",
        "label": f"{table_name}_{int(time.time())}",
        "format": "csv",
        "column_separator": "|",
        "strict_mode": "false",
        "columns": ",".join([
            f"{col} = NULLIF({col}, '\\\\N')"
            for col in columns.values()
        ]),
        "Content-Type": "text/plain; charset=UTF-8",
        "Content-Length": str(os.path.getsize(csv_path)),
        "Expect": "100-continue"
    }

    try:
        with open(csv_path, "rb") as f:
            response = requests.put(
                url,
                headers=headers,
                data=f,
                timeout=600
            )

        # ⏱ Fin medición
        end_time = time.perf_counter()
        elapsed = end_time - start_time

        # 🔹 Métricas de tiempo
        elapsed_seconds = elapsed
        total_ms = int(elapsed * 1000)

        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        milliseconds = int((elapsed - int(elapsed)) * 1000)

        print("Respuesta StarRocks:")
        print(response.text)

        resp_json = response.json()

        if resp_json.get("Status") != "Success":
            raise Exception(f"Stream Load falló: {resp_json.get('Message')}")

        loaded_rows = int(resp_json.get("NumberLoadedRows", 0))

        print("\n========= RESULTADO STREAM LOAD =========")
        print(f"Filas cargadas       : {loaded_rows}")
        print(f"Tiempo exacto        : {elapsed_seconds:.3f} segundos")
        print(f"Tiempo formateado    : {hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}")
        print(f"Total milisegundos   : {total_ms} ms")
        print("=========================================\n")

    finally:
        if os.path.exists(csv_path):
            os.remove(csv_path)
            print("Archivo temporal eliminado:", csv_path)