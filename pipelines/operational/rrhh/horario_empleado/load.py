import time
from config.settings import CONFIG
from utils.spark_client import SparkManager
from config.sqlserver import SQLServerConnector
from pipelines.operational.rrhh.horario_empleado.data import (SELECT_ORIGEN, COLUMN_MAPPING, TABLA_DESTINO, PRIMARY_KEY)

def run_etl():

    # print("=== Inicio ETL ===")
    start_time = time.time()

    # print("Inicializando SparkManager...")
    spark_mgr = SparkManager(app_name=f"ETL_{TABLA_DESTINO}", jars=CONFIG["sql"]["jar"])
    # print("SparkManager inicializado.")

    sql_connector = SQLServerConnector(CONFIG['garita'])
    # print("Conexión SQLServer creada.")

    url_dest = CONFIG['garita']['url']
    jdbc_props = {
        "user": CONFIG['garita']['user'],
        "password": CONFIG['garita']['pass'],
        "driver": CONFIG['sql']['driver']
    }

    try:
        df_src = spark_mgr.read_table(CONFIG['fuxion'], SELECT_ORIGEN, CONFIG['sql'])
    
        df_insert = spark_mgr.rename_columns(df_src, COLUMN_MAPPING)
        
        delete_sql = """
        DELETE FROM PROD.actividad_personal_det
        WHERE LEFT(idtiempo,6) = FORMAT(GETDATE(), 'yyyyMM')
        """ 

        spark_mgr.execute_sql(delete_sql, sql_connector)

        spark_mgr.insert_data(df_insert, TABLA_DESTINO, url_dest, jdbc_props=jdbc_props)

    finally:
        spark_mgr.stop()

    print(f"=== ETL de {TABLA_DESTINO} completado en {time.time() - start_time:.2f} segundos ===")

if __name__ == "__main__":
    run_etl()

