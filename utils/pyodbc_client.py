import hashlib
from typing import List, Dict, Any
from config.sqlserver import SQLServerConnector


class PyODBCManager:
    """
    Gestiona operaciones ETL usando pyodbc:
    - Insert
    - Update incremental
    - Delete por batch
    - Merge lógico mediante hash
    """

    # ------------------------------------------------------------------
    # INSERT
    # ------------------------------------------------------------------
    def insert_data(
        self,
        rows: List[Dict[str, Any]],
        table_name: str,
        sql_connector: SQLServerConnector
    ):
        """
        Inserta datos ignorando la columna hash si existe.
        """
        if not rows:
            return

        rows = [
            {k: v for k, v in row.items() if k != "hash"}
            for row in rows
        ]

        columns = list(rows[0].keys())
        placeholders = ",".join("?" for _ in columns)
        col_clause = ",".join(columns)

        sql = f"""
            INSERT INTO {table_name} ({col_clause})
            VALUES ({placeholders})
        """

        conn, cursor = sql_connector.begin()
        try:
            cursor.executemany(
                sql,
                [tuple(row[c] for c in columns) for row in rows]
            )
            sql_connector.commit(conn)
        except Exception:
            sql_connector.rollback(conn)
            raise

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------
    def update_data(
        self,
        rows: List[Dict[str, Any]],
        table_name: str,
        key_columns: List[str],
        sql_connector: SQLServerConnector
    ):
        """
        UPDATE dinámico row-by-row (misma lógica que Spark).
        """
        if not rows:
            return

        conn, cursor = sql_connector.begin()
        try:
            for row in rows:
                updates = {
                    k: v for k, v in row.items()
                    if k not in key_columns and k != "hash"
                }

                if not updates:
                    continue

                set_clause = ", ".join(f"{k}=?" for k in updates)
                where_clause = " AND ".join(f"{k}=?" for k in key_columns)

                params = (
                    list(updates.values()) +
                    [row[k] for k in key_columns]
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
        rows: List[Dict[str, Any]],
        table_name: str,
        key_columns: List[str],
        sql_connector: SQLServerConnector,
        batch_size: int = 500
    ):
        """
        DELETE por batch con IN compuesto.
        """
        if not rows:
            return

        keys = [
            tuple(row[k] for k in key_columns)
            for row in rows
        ]

        conn, cursor = sql_connector.begin()
        try:
            for i in range(0, len(keys), batch_size):
                batch = keys[i:i + batch_size]
                self._execute_delete_batch(
                    cursor,
                    table_name,
                    key_columns,
                    batch
                )

            sql_connector.commit(conn)
        except Exception:
            sql_connector.rollback(conn)
            raise

    def _execute_delete_batch(
        self,
        cursor,
        table_name: str,
        key_columns: List[str],
        batch: List[tuple]
    ):
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
    # TRANSFORMACIONES
    # ------------------------------------------------------------------
    @staticmethod
    def rename_columns(
        rows: List[Dict[str, Any]],
        column_mapping: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """
        Renombra claves del diccionario (origen → destino).
        """
        renamed = []
        for row in rows:
            new_row = {}
            for k, v in row.items():
                new_row[column_mapping.get(k, k)] = v
            renamed.append(new_row)
        return renamed

    @staticmethod
    def add_hash_column(
        rows: List[Dict[str, Any]],
        columns: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Agrega hash SHA-256 concatenando columnas.
        """
        result = []
        for row in rows:
            concat = "|".join(str(row[c]) for c in columns)
            hash_value = hashlib.sha256(concat.encode()).hexdigest()
            row_copy = dict(row)
            row_copy["hash"] = hash_value
            result.append(row_copy)
        return result

    # ------------------------------------------------------------------
    # MERGE LOGICO
    # ------------------------------------------------------------------
    def merge_data(
        self,
        source_rows: List[Dict[str, Any]],
        dest_rows: List[Dict[str, Any]],
        key_columns: List[str],
        column_mapping: Dict[str, str]
    ):
        """
        Emula FULL OUTER JOIN + hash comparison.
        """
        src = self.rename_columns(source_rows, column_mapping)

        columnas_destino = list(column_mapping.values())

        src_h = self.add_hash_column(src, columnas_destino)
        dest_h = self.add_hash_column(dest_rows, columnas_destino)

        src_index = {
            tuple(row[k] for k in key_columns): row
            for row in src_h
        }

        dest_index = {
            tuple(row[k] for k in key_columns): row
            for row in dest_h
        }

        all_keys = set(src_index) | set(dest_index)

        merged = []
        for key in all_keys:
            merged.append({
                "src": src_index.get(key),
                "dest": dest_index.get(key)
            })

        return merged

    # ------------------------------------------------------------------
    # SQL UTIL
    # ------------------------------------------------------------------
    @staticmethod
    def execute_sql(sql: str, sql_connector: SQLServerConnector):
        conn, cursor = sql_connector.begin()
        try:
            cursor.execute(sql)
            sql_connector.commit(conn)
        except Exception:
            sql_connector.rollback(conn)
            raise
    
                
    def classify_rows(merged_rows):
        rows_insert = []
        rows_update = []
        rows_delete = []

        for row in merged_rows:
            src = row["src"]
            dest = row["dest"]

            if src and not dest:
                rows_insert.append(src)

            elif src and dest and src["hash"] != dest["hash"]:
                rows_update.append(src)

            elif dest and not src:
                rows_delete.append(dest)

        return rows_insert, rows_update, rows_delete
