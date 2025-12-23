import pyodbc
import csv
from core.utils import clean_text
from config.settings import CONFIG
from load.dimensions.dim_producto.config import TMP_CSV, COLUMN_MAPPING

def extract_to_csv():

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={CONFIG['fuxion']['server']};"
        f"DATABASE={CONFIG['fuxion']['database']};"
        f"UID={CONFIG['fuxion']['user']};"
        f"PWD={CONFIG['fuxion']['pass']}"
    )

    query = """
    SELECT
        ma04.SEQMA04,
        ma04.Y04001,
        ma04.Y04002,
        TRIM(UPPER(arbol.descrip)) AS Familia,
        UPPER(TRIM(ma00.descrip))  AS UnidadMedida,
        ISNULL(clasificacion_abc.relacion_cabc,'C') AS ClasificacionABC,
        CASE 
            WHEN ESTADO4 = 1 THEN 'ACTIVO'
            ELSE 'INACTIVO'
        END AS DescripcionEstado
    FROM ma04
    INNER JOIN arbol ON arbol.ARBOL_ID = ISNULL(ma04.Y04031, ma04.arbol_id)
    INNER JOIN ma00 ON ma00.codigo = ma04.Y04033 AND ma00.clasif = '0003'
    LEFT JOIN CMP.formula_pto_reorden_mes ON CMP.formula_pto_reorden_mes.seqma04_fprm = ma04.SEQMA04
    LEFT JOIN CMP.mes_clasificadorabcxcodigo ON CMP.mes_clasificadorabcxcodigo.seqma04_mcc = formula_pto_reorden_mes.seqma04_fprm
    LEFT JOIN CMP.clasificacion_abc clasificacion_abc  ON frecuencia_1_mcc = periodo1_cabc AND frecuencia_2_mcc = periodo2_cabc
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