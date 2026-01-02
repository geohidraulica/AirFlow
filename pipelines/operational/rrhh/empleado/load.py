import time
from config.settings import CONFIG
from config.sqlserver import SQLServerConnector
from utils.pyodbc_client import PyODBCManager
from pipelines.operational.rrhh.empleado.data import (SELECT_ORIGEN, SELECT_DESTINO, COLUMN_MAPPING, TABLA_DESTINO, PRIMARY_KEY)

def load():
    start_time = time.time()

    etl_mgr = PyODBCManager()

    sql_origen = SQLServerConnector(CONFIG["fuxion"])
    sql_destino = SQLServerConnector(CONFIG["garita"])

    # --------------------------------------------------------------
    # READ ORIGEN
    # --------------------------------------------------------------
    src_rows = sql_origen.fetch_all(SELECT_ORIGEN)

    # --------------------------------------------------------------
    # READ DESTINO
    # --------------------------------------------------------------
    dest_rows = sql_destino.fetch_all(SELECT_DESTINO)

    # --------------------------------------------------------------
    # MERGE LOGICO
    # --------------------------------------------------------------
    merged = etl_mgr.merge_data(
        source_rows=src_rows,
        dest_rows=dest_rows,
        key_columns=[PRIMARY_KEY],
        column_mapping=COLUMN_MAPPING
    )

    # --------------------------------------------------------------
    # CLASIFICACION
    # --------------------------------------------------------------
    rows_insert = []
    rows_update = []
    rows_delete = []

    for row in merged:
        src = row["src"]
        dest = row["dest"]

        if src and not dest:
            rows_insert.append(src)

        elif src and dest and src["hash"] != dest["hash"]:
            rows_update.append(src)

        elif dest and not src:
            rows_delete.append(dest)

    # --------------------------------------------------------------
    # INSERT
    # --------------------------------------------------------------
    etl_mgr.insert_data(rows_insert, TABLA_DESTINO, sql_destino)

    # --------------------------------------------------------------
    # UPDATE
    # --------------------------------------------------------------
    etl_mgr.update_data(rows_update, TABLA_DESTINO, [PRIMARY_KEY], sql_destino)

    # --------------------------------------------------------------
    # DELETE
    # --------------------------------------------------------------
    etl_mgr.delete_data(rows_delete, TABLA_DESTINO, [PRIMARY_KEY], sql_destino)

    print(f"=== ETL de {TABLA_DESTINO} completado en " f"{time.time() - start_time:.2f} segundos ===")

# if __name__ == "__main__":
#     load()

# python3 -m pipelines.operational.rrhh.empleado.load

# Spark Test

# import time
# from config.settings import CONFIG
# from utils.spark_client import SparkManager
# from config.sqlserver import SQLServerConnector
# from pipelines.operational.rrhh.empleado.data import (SELECT_ORIGEN, SELECT_DESTINO, COLUMN_MAPPING, TABLA_DESTINO, PRIMARY_KEY)

# def run_etl():

#     # print("=== Inicio ETL ===")
#     start_time = time.time()

#     # print("Inicializando SparkManager...")
#     spark_mgr = SparkManager(app_name=f"ETL_{TABLA_DESTINO}", jars=CONFIG["sql"]["jar"])
#     # print("SparkManager inicializado.")

#     sql_connector = SQLServerConnector(CONFIG['garita'])
#     # print("Conexión SQLServer creada.")

#     url_dest = CONFIG['garita']['url']
#     jdbc_props = {
#         "user": CONFIG['garita']['user'],
#         "password": CONFIG['garita']['pass'],
#         "driver": CONFIG['sql']['driver']
#     }

#     try:
#         # print("Leyendo datos de origen...")
#         df_src = spark_mgr.read_table(CONFIG['fuxion'], SELECT_ORIGEN, CONFIG['sql'])
#         # print(f"Datos de origen cargados: {df_src.count()} registros")

#         # print("Leyendo datos de destino...")
#         df_dest = spark_mgr.read_table(CONFIG['garita'], SELECT_DESTINO, CONFIG['sql'])
#         # print(f"Datos de destino cargados: {df_dest.count()} registros")

#         # print("Realizando merge y hash...")
#         df_merged = spark_mgr.merge_data(df_src, df_dest,[PRIMARY_KEY], COLUMN_MAPPING)
#         # print("Merge completado.")

#         # Determinar insert / update / delete
#         df_insert = df_merged.filter(f"dest.{PRIMARY_KEY} IS NULL").select("src.*")
#         df_update = df_merged.filter(f"""src.{PRIMARY_KEY} IS NOT NULL AND dest.{PRIMARY_KEY} IS NOT NULL AND src.hash <> dest.hash""").select("src.*")
#         df_delete = df_merged.filter(f"src.{PRIMARY_KEY} IS NULL").select(f"dest.{PRIMARY_KEY}")

#         # print("Insertando datos...")
#         spark_mgr.insert_data(df_insert, TABLA_DESTINO, url_dest, jdbc_props=jdbc_props)
#         # print("Insert completado.")

#         # print("Actualizando datos...")
#         spark_mgr.update_data(df_update, TABLA_DESTINO, {PRIMARY_KEY}, sql_connector)
#         # print("Update completado.")

#         # print("Eliminando datos...")
#         spark_mgr.delete_data(df_delete, TABLA_DESTINO, {PRIMARY_KEY}, sql_connector)
#         # print("Delete completado.")

#     finally:
#         spark_mgr.stop()
#         # print("Spark detenido.")

#     print(f"=== ETL de {TABLA_DESTINO} completado en {time.time() - start_time:.2f} segundos ===")

# if __name__ == "__main__":
#     run_etl()

