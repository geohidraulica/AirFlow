from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws
from config.sql_connector import SQLServerConnector


class SparkManager:
    """
    Gestiona operaciones Spark para ETL:
    - Lectura JDBC
    - Inserción
    - Update / Delete incremental
    - Merge lógico mediante hash
    """

    def __init__(self, app_name: str, jars: str | None = None):
        """
        Inicializa la SparkSession y reduce el nivel de logging.
        """
        builder = SparkSession.builder.appName(app_name)

        if jars:
            builder = builder.config("spark.jars", jars)

        self.spark = builder.getOrCreate()

        # Reducir ruido de logs (importante para performance del driver)
        self.spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------------
    # INSERT
    # ------------------------------------------------------------------
    def insert_data(self, df, table_name: str, url: str, jdbc_props: dict):
        """
        Inserta datos vía JDBC ignorando la columna hash si existe.
        """
        if df.rdd.isEmpty():
            return

        if "hash" in df.columns:
            df = df.drop("hash")

        df.write.jdbc(
            url=url,
            table=table_name,
            mode="append",
            properties=jdbc_props
        )

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------
    def update_data(
        self,
        df_update,
        table_name: str,
        key_columns: list[str],
        sql_connector: SQLServerConnector
    ):
        """
        Actualiza registros existentes usando UPDATE dinámico.
        Mantiene lógica row-by-row (incremental).
        """
        if df_update.rdd.isEmpty():
            return

        conn, cursor = sql_connector.begin()

        try:
            for row in df_update.toLocalIterator():
                row_dict = row.asDict()

                updates = {
                    k: v for k, v in row_dict.items()
                    if k not in key_columns and k != "hash"
                }

                if not updates:
                    continue

                set_clause = ", ".join(f"{k}=?" for k in updates)
                where_clause = " AND ".join(f"{k}=?" for k in key_columns)

                params = (
                    list(updates.values()) +
                    [row_dict[k] for k in key_columns]
                )

                sql = f"""
                    UPDATE {table_name}
                    SET {set_clause}
                    WHERE {where_clause}
                """

                cursor.execute(sql, params)

            sql_connector.commit(conn)

        except Exception:
            sql_connector.rollback(conn)
            raise

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------
    def delete_data(
        self,
        df_delete,
        table_name: str,
        key_columns: list[str],
        sql_connector: SQLServerConnector,
        batch_size: int = 500
    ):
        """
        Elimina registros por lotes usando IN compuesto.
        """
        if df_delete.rdd.isEmpty():
            return

        conn, cursor = sql_connector.begin()

        try:
            batch = []

            for row in df_delete.toLocalIterator():
                batch.append(tuple(row[k] for k in key_columns))

                if len(batch) == batch_size:
                    self._execute_delete_batch(cursor, table_name, key_columns, batch)
                    batch.clear()

            if batch:
                self._execute_delete_batch(cursor, table_name, key_columns, batch)

            sql_connector.commit(conn)

        except Exception:
            sql_connector.rollback(conn)
            raise

    def _execute_delete_batch(self, cursor, table_name, key_columns, batch):
        """
        Ejecuta un DELETE por lote.
        """
        placeholders = ",".join(
            "(" + ",".join("?" * len(key_columns)) + ")"
            for _ in batch
        )

        flat_values = [v for row in batch for v in row]

        sql = f"""
            DELETE FROM {table_name}
            WHERE ({','.join(key_columns)}) IN ({placeholders})
        """

        cursor.execute(sql, flat_values)

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------
    def read_table(self, source_config: dict, query: str, driver: dict):
        """
        Lee datos desde SQL Server vía JDBC.
        """
        return (
            self.spark.read
            .format("jdbc")
            .option("url", source_config["url"])
            .option("user", source_config["user"])
            .option("password", source_config["pass"])
            .option("driver", driver["driver"])
            .option("query", query)
            .load()
        )

    # ------------------------------------------------------------------
    # TRANSFORMACIONES
    # ------------------------------------------------------------------
    @staticmethod
    def rename_columns(df, column_mapping: dict):
        """
        Renombra columnas según un mapping origen → destino.
        """
        for orig, dest in column_mapping.items():
            if orig != dest:
                df = df.withColumnRenamed(orig, dest)
        return df

    @staticmethod
    def add_hash_column(df, columns: list[str]):
        """
        Agrega columna hash SHA-256 para comparación de cambios.
        """
        return df.withColumn(
            "hash",
            sha2(concat_ws("|", *[col(c) for c in columns]), 256)
        )

    def merge_data(self, df_source, df_dest, key_columns, column_mapping):
        """
        Realiza un merge lógico FULL OUTER JOIN con hash.
        """
        df_src = self.rename_columns(df_source, column_mapping)

        columnas_destino = list(column_mapping.values())

        df_src_h = self.add_hash_column(df_src, columnas_destino)
        df_dest_h = self.add_hash_column(df_dest, columnas_destino)

        return df_src_h.alias("src").join(
            df_dest_h.alias("dest"),
            on=key_columns,
            how="full"
        )

    # ------------------------------------------------------------------
    # SQL UTIL
    # ------------------------------------------------------------------
    @staticmethod
    def execute_sql(sql: str, sql_connector: SQLServerConnector):
        """
        Ejecuta SQL directo con control transaccional.
        """
        conn, cursor = sql_connector.begin()
        try:
            cursor.execute(sql)
            sql_connector.commit(conn)
        except Exception:
            sql_connector.rollback(conn)
            raise

    # ------------------------------------------------------------------
    # STOP
    # ------------------------------------------------------------------
    def stop(self):
        """
        Finaliza la SparkSession.
        """
        self.spark.stop()
