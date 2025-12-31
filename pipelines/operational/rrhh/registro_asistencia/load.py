import time
from config.settings import CONFIG
from utils.spark_client import SparkManager
from config.sqlserver import SQLServerConnector
from pipelines.operational.rrhh.registro_asistencia.data import (SELECT_ORIGEN, SELECT_DESTINO, COLUMN_MAPPING, TABLA_DESTINO, PRIMARY_KEY)

def run_etl():

    # print("=== Inicio ETL ===")
    start_time = time.time()

    # print("Inicializando SparkManager...")
    spark_mgr = SparkManager(app_name=f"ETL_{TABLA_DESTINO}", jars=CONFIG["sql"]["jar"])
    # print("SparkManager inicializado.")

    sql_connector = SQLServerConnector(CONFIG['garita'])
    # print("Conexión SQLServer creada.")

    url_dest = CONFIG['fuxion']['url']
    jdbc_props = {
        "user": CONFIG['fuxion']['user'],
        "password": CONFIG['fuxion']['pass'],
        "driver": CONFIG['sql']['driver']
    }

    try:
        # print("Leyendo datos de origen...")
        df_src = spark_mgr.read_table(CONFIG['garita'], SELECT_ORIGEN, CONFIG['sql'])
        # print(f"Datos de origen cargados: {df_src.count()} registros")

        # print("Leyendo datos de destino...")
        df_dest = spark_mgr.read_table(CONFIG['fuxion'], SELECT_DESTINO, CONFIG['sql'])
        # print(f"Datos de destino cargados: {df_dest.count()} registros")

        # print("Realizando merge y hash...")
        df_merged = spark_mgr.merge_data(df_src, df_dest,[PRIMARY_KEY], COLUMN_MAPPING)
        # print("Merge completado.")

        # Determinar insert / update / delete
        df_insert = df_merged.filter(f"dest.{PRIMARY_KEY} IS NULL").select("src.*")


        # print("Insertando datos...")
        spark_mgr.insert_data(df_insert, TABLA_DESTINO, url_dest, jdbc_props=jdbc_props)
        # print("Insert completado.")

    finally:
        spark_mgr.stop()
        # print("Spark detenido.")

    print(f"=== ETL de {TABLA_DESTINO} completado en {time.time() - start_time:.2f} segundos ===")

if __name__ == "__main__":
    run_etl()

