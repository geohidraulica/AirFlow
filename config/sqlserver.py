# load/facts/fact_compras/connections.py

import pyodbc


class SQLServerConnector:
    def __init__(self, config: dict):
        self.config = config
        self.conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={config['pass']};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )
        
    # -------------------------
    # Conexión base
    # -------------------------
    def connect(self):
        return pyodbc.connect(self.conn_str)

    # -------------------------
    # SELECT
    # -------------------------
    def fetch_all(self, query: str, params=None):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        rows = cursor.fetchall()
        conn.close()
        return rows

    # -------------------------
    # INSERT / UPDATE / DELETE
    # -------------------------
    def execute(self, query: str, params=None, commit=True):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(query, params or [])

        if commit:
            conn.commit()

        conn.close()

    # -------------------------
    # TRANSACCIONES
    # -------------------------
    def begin(self):
        conn = self.connect()
        return conn, conn.cursor()

    def commit(self, conn):
        conn.commit()
        conn.close()

    def rollback(self, conn):
        conn.rollback()
        conn.close()
