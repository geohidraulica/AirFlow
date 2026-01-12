import os
import time
import base64
import requests
from config.settings import CONFIG
from utils.mysql_manager import MySQLManager
from config.mysql_connector import MySQLConnector

def stream_load(csv_path, columns, table_name):
    """
    Carga un CSV a StarRocks usando Stream pipelines.
    
    :param csv_path: Ruta del archivo CSV a cargar
    :param columns: Lista de nombres de columnas en el CSV
    :param table_name: Nombre de la tabla destino en StarRocks
    """
    start_time = time.time()   # ⏱ inicio

    print("TMP_CSV:", csv_path)
    
    print("Cargando datos a StarRocks (Stream Load)...")

    pyodbc = MySQLManager()
    mysql = MySQLConnector(CONFIG["starrocks"])

    print(f"Truncando tabla {table_name}...")
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
        "columns": ",".join(columns.values()),
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
        if os.path.exists(csv_path):
            os.remove(csv_path)
            print("Archivo temporal eliminado:", csv_path)
