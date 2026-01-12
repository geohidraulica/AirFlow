from utils.export_csv import export_query_to_csv
from pipelines.facts.fact_compras.config import TMP_CSV, COLUMN_MAPPING, SELECT_ORIGEN, UPDATE_RECIBIDO_QUERY
from config.sql_connector import SQLServerConnector
from utils.sqlserver_manager import PyODBCManager
from config.settings import CONFIG

def extract():

    connector = SQLServerConnector(CONFIG["fuxion"])
    conn = connector.connect()

    odbc = PyODBCManager()

    odbc.execute_sql(UPDATE_RECIBIDO_QUERY, connector)

    query = SELECT_ORIGEN

    source_columns = list(COLUMN_MAPPING.keys())  

    export_query_to_csv(conn, query, source_columns, TMP_CSV)

    conn.close()

    return TMP_CSV