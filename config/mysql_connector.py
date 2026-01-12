import mysql.connector


class MySQLConnector:

    def __init__(self, config: dict):
        self.config = {
            "host": config["server"],
            "port": config.get("port", 9030),
            "database": config["database"],
            "user": config["user"],
            "password": config["pass"],
            "autocommit": False
        }

    def connect(self):
        return mysql.connector.connect(**self.config)

    def fetch_all(self, query: str, params=None):
        conn = self.connect()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params or ())
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows

    def execute(self, query: str, params=None):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        conn.commit()
        cursor.close()
        conn.close()

    def executemany(self, query: str, params: list):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.executemany(query, params)
        conn.commit()
        cursor.close()
        conn.close()