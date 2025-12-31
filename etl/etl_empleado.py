from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws
from config.settings import CONFIG
import pyodbc

def read_source(spark):
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", CONFIG["fuxion"]['url']) \
            .option("user", CONFIG["fuxion"]['user']) \
            .option("password", CONFIG["fuxion"]['pass']) \
            .option("driver", CONFIG["sql"]["driver"]) \
            .option("query", """
            SELECT 
                idempleado,
                cargo_trab, 
                fcese,
                mcese,
                estado_emp,
                nomb_apell,
                dni,
                id_areapert,
                tipo_control,
                induccion,
                tipo_empleado,
                fvalidacion,
                faptitud,
                estado_aptitud
            FROM RRHH.empleado
            """) \
            .load()
        return df
    except Exception as e:
        raise

def read_dest(spark):
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", CONFIG["garita"]['url']) \
            .option("user", CONFIG["garita"]['user']) \
            .option("password", CONFIG["garita"]['pass']) \
            .option("driver", CONFIG["sql"]["driver"]) \
            .option("query", """
            SELECT 
                idempleado,
                cargo_trab,
                fcese,
                mcese,
                estado_emp,
                nomb_apell,
                dni,
                id_areapert,
                tipo_control,
                induccion,
                tipo_empleado,
                fvalidacion,
                faptitud,
                estado_aptitud
            FROM RRHH.empleado
            """) \
            .load()
        return df
    except Exception as e:
        raise

def sync_data(df_source, df_dest):  
    column_mapping = {
        'idempleado': 'idempleado',           # origen: idempleado -> destino: idempleado
        'cargo_trab': 'cargo_trab',           # origen: cargo_trab -> destino: cargo_trab
        'fcese': 'fcese',                     # origen: fcese -> destino: fcese
        'mcese': 'mcese',                     # origen: mcese -> destino: mcese
        'estado_emp': 'estado_emp',           # origen: estado_emp -> destino: estado_emp
        'nomb_apell': 'nomb_apell',           # origen: nomb_apell -> destino: nomb_apell
        'dni': 'dni',                         # origen: dni -> destino: dni
        'id_areapert': 'id_areapert',         # origen: id_areapert -> destino: id_areapert
        'tipo_control': 'tipo_control',       # origen: tipo_control -> destino: tipo_control
        'induccion': 'induccion',             # origen: induccion -> destino: induccion
        'tipo_empleado': 'tipo_empleado',     # origen: tipo_empleado -> destino: tipo_empleado
        'fvalidacion': 'fvalidacion',         # origen: fvalidacion -> destino: fvalidacion
        'faptitud': 'faptitud',               # origen: faptitud -> destino: faptitud
        'estado_aptitud': 'estado_aptitud'    # origen: estado_aptitud -> destino: estado_aptitud
    }
    
    columnas_destino = list(column_mapping.values())

    df_source_renamed = df_source
    for orig_col, dest_col in column_mapping.items():
        if orig_col != dest_col:
            df_source_renamed = df_source_renamed.withColumnRenamed(orig_col, dest_col)
    
    df_source_hash = df_source_renamed.withColumn(
        "hash_src", 
        sha2(concat_ws("|", *[col(c) for c in columnas_destino]), 256)
    )
    
    df_dest_hash = df_dest.withColumn(
        "hash_dest", 
        sha2(concat_ws("|", *[col(c) for c in columnas_destino]), 256)
    )

    df_merged = df_source_hash.alias("src").join(
        df_dest_hash.alias("dest"),
        on=["idempleado"],  # La columna idempleado ya tiene el mismo nombre en ambos
        how="full_outer"
    )
    
    # INSERT: en origen pero no en destino
    df_insert = df_merged.filter(col("dest.idempleado").isNull()).select("src.*")
    
    # UPDATE: en ambos pero con cambios
    df_update = df_merged.filter(
        (col("src.idempleado").isNotNull()) & 
        (col("dest.idempleado").isNotNull()) & 
        (col("src.hash_src") != col("dest.hash_dest"))
    ).select("src.*")
    
    # DELETE: en destino pero no en origen
    df_delete = df_merged.filter(col("src.idempleado").isNull()).select("dest.idempleado")
    
    insert_count = df_insert.count()
    update_count = df_update.count()
    delete_count = df_delete.count()
        
    # CONFIGURACIÓN DE CONEXIÓN
    url_dest = CONFIG['garita']['url']
    jdbc_properties = {
        "user": CONFIG["garita"]['user'],
        "password": CONFIG["garita"]['pass'],
        "driver": CONFIG["sql"]["driver"]
    }
    
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={CONFIG['garita']['server']};"
        f"DATABASE={CONFIG['garita']['database']};"
        f"UID={CONFIG['garita']['user']};"
        f"PWD={CONFIG['garita']['pass']}"
    )
    
    # EJECUTAR OPERACIONES
    total_operations = insert_count + update_count + delete_count
    
    if total_operations == 0:
        print("\nNo se requieren cambios. Los datos ya están sincronizados.")
        return
    
    # 1. INSERT
    if insert_count > 0:
        columnas_para_insertar = [c for c in df_insert.columns if c != 'hash_src']
        # Seleccionar solo las columnas necesarias (sin el hash)
        df_insert_to_write = df_insert.select(*columnas_para_insertar)   
        # Insertar usando JDBC
        df_insert_to_write.write.jdbc(
            url=url_dest, 
            table="RRHH.empleado", 
            mode="append", 
            properties=jdbc_properties
        )
    
    # 2. UPDATE
    if update_count > 0:        
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Recolectar datos para update
            update_rows = []
            for row in df_update.collect():
                row_dict = row.asDict()
                updates = {}
                # Solo tomar las columnas del destino (sin hash)
                for col_name in columnas_destino:
                    if col_name in row_dict and col_name != 'idempleado' and col_name != 'hash_src':
                        updates[col_name] = row_dict[col_name]
                
                update_rows.append({
                    'idempleado': row_dict['idempleado'],
                    'updates': updates
                })
            
            # Ejecutar updates
            updated_count = 0
            for data in update_rows:
                if data['updates']:  # Solo si hay algo que actualizar
                    set_clauses = []
                    params = []
                    
                    for col_name, value in data['updates'].items():
                        set_clauses.append(f"{col_name} = ?")
                        params.append(value)
                    
                    params.append(data['idempleado'])
                    
                    update_sql = f"""
                    UPDATE RRHH.empleado
                    SET {', '.join(set_clauses)}
                    WHERE idempleado = ?
                    """
                    
                    cursor.execute(update_sql, params)
                    updated_count += 1
            
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
    
    # 3. DELETE
    if delete_count > 0:        
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Recolectar IDs para eliminar
            delete_ids = [str(row.idempleado) for row in df_delete.collect()]
            
            # Eliminar en lotes
            batch_size = 500
            deleted_total = 0
            
            for i in range(0, len(delete_ids), batch_size):
                batch = delete_ids[i:i + batch_size]
                if batch:
                    placeholders = ",".join("?" * len(batch))
                    delete_sql = f"DELETE FROM RRHH.empleado WHERE idempleado IN ({placeholders})"
                    cursor.execute(delete_sql, batch)
                    deleted_total += len(batch)
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

def run_etl():
    try:
        # Inicializar Spark
        spark = SparkSession.builder \
            .appName("ETL_EMPLEADO") \
            .config("spark.jars", CONFIG["sql"]["jar"]) \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        # Leer datos
        df_src = read_source(spark)
        df_dest = read_dest(spark)
        # Sicronizar
        sync_data(df_src, df_dest)
        # Cerrar Spark
        spark.stop()
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise
