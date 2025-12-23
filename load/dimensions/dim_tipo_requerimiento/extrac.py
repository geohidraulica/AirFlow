import pyodbc
import csv
from core.utils import clean_text
from config.settings import CONFIG
from load.dimensions.dim_tipo_requerimiento.config import TMP_CSV, COLUMN_MAPPING

def extract_to_csv():

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={CONFIG['fuxion']['server']};"
        f"DATABASE={CONFIG['fuxion']['database']};"
        f"UID={CONFIG['fuxion']['user']};"
        f"PWD={CONFIG['fuxion']['pass']}"
    )

    query = """
    SELECT DISTINCT 
    ISNULL(COMAPROREQCAB.id_origen_rq_ma00,0) AS id_origen_rq_ma00, 
    CASE
        WHEN COMAPROREQCAB.id_origen_rq_ma00 = 1 THEN 'SUGERIDO DE COMPRAS'
        WHEN COMAPROREQCAB.id_origen_rq_ma00 = 2 THEN 'CRONOGRAMA DE ENTREGA'
        WHEN COMAPROREQCAB.id_origen_rq_ma00 = 3 THEN 'CONTRATO MARCO'
        WHEN COMAPROREQCAB.id_origen_rq_ma00 = 4 THEN 'CONTRATO PROVEEDOR'
        ELSE 'COMPRA GENERAL'
    END AS F_TIPO_RQ
    FROM ALMPEDIDOCAB
    INNER JOIN COMAPROREQCAB ON COMAPROREQCAB.SEQPEDCAB = ALMPEDIDOCAB.SEQPEDCAB
    WHERE ALMPEDIDOCAB.SERVREPUESTO = 'R'
    """

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(query)

    source_columns = list(COLUMN_MAPPING.keys())

    with open(TMP_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(
            f,
            delimiter='|',
            quoting=csv.QUOTE_NONE,
            escapechar='\\'
        )

        for row in cursor:
            row_dict = dict(zip(source_columns, row))

            for col, value in row_dict.items():
                if isinstance(value, str):
                    row_dict[col] = clean_text(value)

            writer.writerow([row_dict[col] for col in source_columns])

    cursor.close()
    conn.close()
