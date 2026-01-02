import pyodbc

class SQLServerConnector:

    def __init__(self, config: dict):
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
    # SELECT → DEVUELVE dict
    # -------------------------
    def fetch_all(self, query: str, params=None):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(query, params or [])

        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        conn.close()
        return rows

    # -------------------------
    # INSERT / UPDATE / DELETE
    # -------------------------
    def execute(self, query: str, params=None):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        conn.commit()
        conn.close()

    def executemany(self, query: str, params: list):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.executemany(query, params)
        conn.commit()
        conn.close()

    # -------------------------
    # TRANSACCIONES (OPCIONAL)
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
