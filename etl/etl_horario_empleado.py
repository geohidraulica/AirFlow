from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.settings import CONFIG
import pyodbc

def read_source(spark):
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", CONFIG["fuxion"]['url']) \
            .option("user", CONFIG["fuxion"]['user']) \
            .option("password", CONFIG["fuxion"]['pass']) \
            .option("query", 
            """
                SELECT
                    id_actividad_det,
                    id_actividad_cab,
                    idtiempo,
                    id_tecnico_det,
                    id_cod_maquina_det,
                    fch_reg_det,
                    id_usu_reg_det,
                    idperiodo,
                    hora_inidet,
                    hora_findet
                FROM PROD.actividad_personal_det
                WHERE LEFT(idtiempo,6) = FORMAT(GETDATE(), 'yyyyMM')
            """
            ) \
            .load()
        return df.orderBy("id_actividad_det")

    except Exception as e:
        raise

def sync_data(df_source):
    record_count = df_source.count()

    if record_count > 0:
        column_mapping = {
            'id_actividad_cab': 'id_actividad_cab',
            'idtiempo': 'idtiempo',
            'id_tecnico_det': 'id_tecnico_det',
            'id_cod_maquina_det': 'id_cod_maquina_det',
            'fch_reg_det': 'fch_reg_det',
            'id_usu_reg_det': 'id_usu_reg_det',
            'idperiodo': 'idperiodo',
            'hora_inidet': 'hora_inidet',
            'hora_findet': 'hora_findet'
        }

        # 1. ELIMINAR datos existentes
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={CONFIG['garita']['server']};"
            f"DATABASE={CONFIG['garita']['database']};"
            f"UID={CONFIG['garita']['user']};"
            f"PWD={CONFIG['garita']['pass']}"
        )
        
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM PROD.actividad_personal_det
                WHERE LEFT(idtiempo,6) = FORMAT(GETDATE(), 'yyyyMM')
            """)
            conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        # 2. PREPARAR datos para insertar
        df_to_insert = df_source.select([
            col(orig_col).alias(dest_col) 
            for orig_col, dest_col in column_mapping.items()
        ])
        
        # 3. INSERTAR nuevos datos
        url_dest = CONFIG['garita']['url']
        properties = {
            "user": CONFIG["garita"]['user'],
            "password": CONFIG["garita"]['pass'],
            "driver": CONFIG["sql"]["driver"]
        }
        
        df_to_insert.write.jdbc(
            url=url_dest, 
            table="PROD.actividad_personal_det",
            mode="append", 
            properties=properties
        )
        
    else:
        print("No hay registros para insertar")

def run_etl():
    try:       
        spark = (
            SparkSession.builder
            .appName("ETL_HORARIO_EMPLEADO")
            .config("spark.jars", CONFIG["sql"]["jar"])
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        df_src = read_source(spark)
        sync_data(df_src)
        spark.stop()

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise
