from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.settings import CONFIG

def read_source(spark):
    df = spark.read \
        .format("jdbc") \
        .option("url", CONFIG["garita"]['url']) \
        .option("user", CONFIG["garita"]['user']) \
        .option("password", CONFIG["garita"]['pass']) \
        .option("driver", CONFIG["sql"]["driver"]) \
        .option("query", """
        SELECT 
        idregasis, 
        idpersonareg,
        fch_registro, 
        fch_sistema, 
        inout, 
        horario_reg, 
        tardanza
        FROM RRHH.registro_asistencia
        """) \
        .load()
    return df

def read_dest(spark):
    df = spark.read \
        .format("jdbc") \
        .option("url", CONFIG["fuxion"]['url']) \
        .option("user", CONFIG["fuxion"]['user']) \
        .option("password", CONFIG["fuxion"]['pass']) \
        .option("driver", CONFIG["sql"]["driver"]) \
        .option("query", """
        SELECT 
        idregasis, 
        idpersonareg,
        fch_registro, 
        fch_sistema, 
        inout, 
        horario_reg, 
        tardanza
        FROM RRHH.registro_asistencia
        """) \
        .load()
    return df

def sync_data(df_source, df_dest):    
    # MAPPING DE COLUMNAS
    column_mapping = {
        'idregasis': 'idregasis',
        'idpersonareg': 'idpersonareg',
        'fch_registro': 'fch_registro', 
        'fch_sistema': 'fch_sistema',
        'inout': 'inout',
        'horario_reg': 'horario_reg',
        'tardanza': 'tardanza',
    }
    
    # Seleccionar solo la columna ID en destino
    df_dest_ids = df_dest.select("idregasis").distinct()
    
    # Filtrar solo los registros que NO existen en destino
    df_new = df_source.join(
        df_dest_ids,
        on="idregasis",
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
        url_dest = CONFIG['fuxion']['url']
        properties = {
            "user": CONFIG["fuxion"]['user'],
            "password": CONFIG["fuxion"]['pass'],
            "driver": CONFIG["sql"]["driver"]
        }
        
        df_to_insert.write.jdbc(
            url=url_dest, 
            table="RRHH.registro_asistencia", 
            mode="append", 
            properties=properties
        )
    else:
        print("No hay registros nuevos para insertar")
    
    return new_count

def run_etl():
    try:      
        spark = SparkSession.builder \
            .appName("ETL_REGISTRO_ASISTENCIA") \
            .config("spark.jars", CONFIG["sql"]["jar"]) \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
            
        # Leer datos
        df_src = read_source(spark)
        df_dest = read_dest(spark)
        
        # Sincronizar
        sync_data(df_src, df_dest)
    
        # Cerrar Spark
        spark.stop()

    except Exception as e:
        raise
