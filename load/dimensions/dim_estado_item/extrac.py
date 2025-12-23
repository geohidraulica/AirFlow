import pyodbc
import csv
from core.utils import clean_text
from config.settings import CONFIG
from load.dimensions.dim_estado_item.config import TMP_CSV, COLUMN_MAPPING

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
        estados.IdEstados,
        UPPER(TRIM(Nombre)) AS Nombre
    FROM ALMPEDIDOCAB
    INNER JOIN COMAPROREQCAB ON COMAPROREQCAB.SEQPEDCAB = ALMPEDIDOCAB.SEQPEDCAB
    INNER JOIN COMAPROREQDET ON COMAPROREQDET.SEQAPROCAB = COMAPROREQCAB.SEQAPROCAB
    INNER JOIN niveles on niveles.SEQAPRODET = COMAPROREQDET.SEQAPRODET and COMAPROREQDET.codnivel = niveles.codnivel
    LEFT JOIN de20 ON de20.SEQAPRODET = COMAPROREQDET.SEQAPRODET
    LEFT JOIN estados ON estados.IdEstados = ISNULL(DE20.Estado, Niveles.Estado)
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