import subprocess
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import CONFIG

def read_source(spark):
    df = spark.read \
        .format("jdbc") \
        .option("url", CONFIG["fuxion"]['url']) \
        .option("user", CONFIG["fuxion"]['user']) \
        .option("password", CONFIG["fuxion"]['pass']) \
        .option("query", """
        SELECT
        idtiempo as IdDimTiempo,
        Fecha,
        anual,
        Mes,
        NMes,
        Dia,
        NDiaSemana,
        NTrimestre,
        NSemana
        FROM adm.tiempo
        WHERE anual BETWEEN YEAR(GETDATE()) - 2 AND YEAR(GETDATE()) + 2
        """) \
        .load()
    return df

def read_dest(spark):
    df = spark.read \
        .format("jdbc") \
        .option("url", CONFIG["starrocks"]['url']) \
        .option("user", CONFIG["starrocks"]['user']) \
        .option("password", CONFIG["starrocks"]['pass']) \
        .option("query", """
        SELECT
        IdDimTiempo, 
        Fecha, 
        Anio, 
        Mes, 
        NombreMes, 
        Dia, 
        NombreDia, 
        Trimestre, 
        Semana
        FROM DimTiempo
        """) \
        .load()
    return df

def sync_data(df_source, df_dest):    
    # MAPPING DE COLUMNAS
    column_mapping = {
        'IdDimTiempo': 'IdDimTiempo',
        'Fecha': 'Fecha',
        'anual': 'Anio', 
        'Mes': 'Mes',
        'NMes': 'NombreMes',
        'Dia': 'Dia',
        'NDiaSemana': 'NombreDia',
        'NTrimestre': 'Trimestre',
        'NSemana': 'Semana',
    }
    
    # Seleccionar solo la columna ID en destino
    df_dest_ids = df_dest.select("IdDimTiempo").distinct()
    
    # Filtrar solo los registros que NO existen en destino
    df_new = df_source.join(
        df_dest_ids,
        on="IdDimTiempo",
        how="left_anti"
    )
    
    new_count = df_new.count()

    if new_count > 0:
        # Preparar datos para insertar
        df_to_insert = df_new.select([
            col(orig_col).alias(dest_col) 
            for orig_col, dest_col in column_mapping.items()
        ])
        
        # Configuración de conexión destino
        url_dest = CONFIG['starrocks']['url']
        properties = {
            "user": CONFIG["starrocks"]['user'],
            "password": CONFIG["starrocks"]['pass'],
            "driver": CONFIG["mysql"]["driver"]
        }

        df_to_insert.write \
        .mode("append") \
        .option("batchsize", "5000") \
        .option("isolationLevel", "NONE") \
        .jdbc(
            url=url_dest,
            table="DimTiempo",
            properties=properties
        )


    else:
        print("No hay registros nuevos para insertar")
    
    return new_count

def run_etl():
    try:      
        start_time = time.time()

        spark = SparkSession.builder \
            .appName("ETL_TIEMPO") \
            .config("spark.jars", CONFIG["mysql"]["jar"]) \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
            
        # Leer datos
        df_src = read_source(spark)
        df_dest = read_dest(spark)
        
        # Sincronizar
        sync_data(df_src, df_dest)
    
        # Cerrar Spark
        spark.stop()

        end_time = time.time()
        elapsed = end_time - start_time
        mins, secs = divmod(elapsed, 60)
        print(f"ETL finalizado en {int(mins)} min {secs:.2f} seg")

    except Exception as e:
        raise


if __name__ == "__main__":
    run_etl()

#C:\spark\bin\spark-submit.cmd D:\flow_spark_prefect\etl\etl_tiempo.py ETL_TIEMPO
