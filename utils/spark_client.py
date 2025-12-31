from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws
from config.sqlserver import SQLServerConnector
from pyspark.sql import SparkSession

class SparkManager:
    def __init__(self, app_name, jars=None):
        builder = SparkSession.builder.appName(app_name)
        if jars:
            builder = builder.config("spark.jars", jars)
        self.spark = builder.getOrCreate()


    # def insert_data(self, df_insert, table_name, url, jdbc_props):
    #     if df_insert.count() > 0:
    #         columnas_insert = [c for c in df_insert.columns if c != "hash"]
    #         df_insert.select(*columnas_insert).write.jdbc(
    #             url=url,
    #             table=table_name,
    #             mode="append",
    #             properties=jdbc_props
    #         )

    # def insert_all(self, df_insert, table_name, url, jdbc_props):
    #     if df_insert.rdd.isEmpty():
    #         return
    #     df_insert.write.jdbc(
    #         url=url,
    #         table=table_name,
    #         mode="append",
    #         properties=jdbc_props
    #     )

    def insert_data(self, df, table_name, url, jdbc_props):
        if df.rdd.isEmpty():
            return

        columnas = df.columns

        if "hash" in columnas:
            columnas = [c for c in columnas if c != "hash"]
            df = df.select(*columnas)

        df.write.jdbc(
            url=url,
            table=table_name,
            mode="append",
            properties=jdbc_props
        )


    def update_data(self, df_update, table_name, key_columns, sql_connector: SQLServerConnector):
        if df_update.count() == 0:
            return
        
        conn, cursor = sql_connector.begin()
        try:
            for row in df_update.collect():
                row_dict = row.asDict()
                updates = {k: v for k, v in row_dict.items() if k not in key_columns + ["hash"]}
                if updates:
                    set_clause = ", ".join(f"{k}=?" for k in updates)
                    params = list(updates.values()) + [row_dict[k] for k in key_columns]
                    where_clause = " AND ".join(f"{k}=?" for k in key_columns)
                    sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                    cursor.execute(sql, params)
            sql_connector.commit(conn)
        except Exception as e:
            sql_connector.rollback(conn)
            raise

    def delete_data(self, df_delete, table_name, key_columns, sql_connector: SQLServerConnector, batch_size=500):
        if df_delete.count() == 0:
            return
        
        delete_ids = [tuple(row[k] for k in key_columns) for row in df_delete.collect()]
        conn, cursor = sql_connector.begin()
        try:
            for i in range(0, len(delete_ids), batch_size):
                batch = delete_ids[i:i+batch_size]
                placeholders = ",".join("(" + ",".join("?"*len(key_columns)) + ")" for _ in batch)
                flat_values = [v for tup in batch for v in tup]
                sql = f"DELETE FROM {table_name} WHERE ({','.join(key_columns)}) IN ({placeholders})"
                cursor.execute(sql, flat_values)
            sql_connector.commit(conn)
        except Exception as e:
            sql_connector.rollback(conn)
            raise

    def read_table(self, source_config, query, driver):
        return self.spark.read \
            .format("jdbc") \
            .option("url", source_config['url']) \
            .option("user", source_config['user']) \
            .option("password", source_config['pass']) \
            .option("driver", driver['driver']) \
            .option("query", query) \
            .load()

    def rename_columns(self, df, column_mapping):
        for orig, dest in column_mapping.items():
            if orig != dest:
                df = df.withColumnRenamed(orig, dest)
        return df

    def add_hash_column(self, df, columnas):
        return df.withColumn("hash", sha2(concat_ws("|", *[col(c) for c in columnas]), 256))

    def merge_data(self, df_source, df_dest, key_columns, column_mapping):
        # 1. Renombrar columnas según el mapping
        df_source_renamed = self.rename_columns(df_source, column_mapping)

        # 2. Columnas destino para hash
        columnas_destino = list(column_mapping.values())

        # 3. Agregar hash
        df_source_h = self.add_hash_column(df_source_renamed, columnas_destino)
        df_dest_h = self.add_hash_column(df_dest, columnas_destino)

        # 4. Merge con full outer join
        df_merged = df_source_h.alias("src").join(
            df_dest_h.alias("dest"),
            on=key_columns,
            how="full"
        )
        return df_merged
    
    def execute_sql(self, sql, sql_connector):
        conn, cursor = sql_connector.begin()
        try:
            cursor.execute(sql)
            sql_connector.commit(conn)
        except Exception:
            sql_connector.rollback(conn)
            raise

    def stop(self):
        self.spark.stop()
