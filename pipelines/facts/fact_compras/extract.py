from utils.export_csv import export_query_to_csv
from pipelines.facts.fact_compras.config import TMP_CSV, COLUMN_MAPPING, SELECT_ORIGEN
from config.sqlserver import SQLServerConnector
from config.settings import CONFIG

def extract():

    connector = SQLServerConnector(CONFIG["fuxion"])
    conn = connector.connect()

    query = SELECT_ORIGEN

    source_columns = list(COLUMN_MAPPING.keys())

    export_query_to_csv(conn, query, source_columns, TMP_CSV)

    conn.close()

    return TMP_CSV