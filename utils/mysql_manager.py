class MySQLManager:

    @staticmethod
    def execute_sql(sql: str, mysql_connector):
        conn = mysql_connector.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
        finally:
            conn.close()
