import pyodbc
import csv
from core.utils import clean_text
from config.settings import CONFIG
from load.facts.fact_compras.config import TMP_CSV, COLUMN_MAPPING

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
            COMAPROREQDET.SEQAPRODET,
            CAST(ALMPEDIDOCAB.FCHEDIT AS DATE) AS F_FECHA_EMISION_REQ,
            CAST(ISNULL(CA20.Y20006, CA20.fecha_emi_oc) AS DATE) AS F_FECHA_EMISION_OC,
            CAST(ISNULL(de20.FCHENTREGA, COMAPROREQDET.FCHENTREGA) AS DATE) AS F_FECHA_ENTREGA,
            --CAST(COMAPROREQCAB.SEQAREA AS INT) AS F_AREA,
            --CAST(COMAPROREQDET.SEQCCOSTOS AS INT) AS F_CECO,
            CAST(ISNULL(CA20.PROCESOO, COMAPROREQCAB.PROCESOR) AS INT) AS F_TIPO_FLUJO,
            MONEDA.SEQMA00 AS F_MONEDA,
            MA04.SEQMA04 AS F_PRODUCTO,
            MA02.cod_prov AS F_PROVEEDOR,
            ALMPEDIDOCAB.SITEIDORIG AS F_SITE,
            ISNULL(ALMPEDIDOCAB.IDUSER,COMAPROREQCAB.idusu_genreq_cmc) AS F_PGENERA,
            COMAPROREQDET.idusuario_ctz_crd AS F_PCOMPRADOR,
            NIVELES.usuario                 AS F_PBANDEJA,
            ISNULL(DE20.Estado, Niveles.Estado) AS F_ESTADO_ITEM,
            CA20.EstadoDoc AS F_ESTADO_OC,
            COMAPROREQDET.id_estado_aten_alm_crd AS F_ESTADO_ALMACEN,
            ISNULL(CA20.FORMAPAGO, ISNULL(COMAPROREQCAB.FORMAPAGO,0)) AS F_FPAGO,
            COMAPROREQDET.id_tipo_proyecto_ctz AS F_TIPO_PROYECTO, 
            ISNULL(COMAPROREQCAB.id_origen_rq_ma00,0) AS F_TIPO_REQ,
            ALMPEDIDOCAB.NROREQUISION AS F_NRO_REQ,
            CA20.CORRESMG AS F_NRO_OC,
            CAST(ISNULL(DE20.Y20084, COMAPROREQDET.CANTPED) AS DECIMAL(18,2)) AS F_CANT_PEDIDA,
            CAST(ISNULL(DE20.RECIBIDO,0) AS DECIMAL(18,2)) AS F_CANT_ATENDIDA, 
            CAST(ISNULL(DE20.CantDevolucion,0) AS DECIMAL(18,2)) AS F_CANT_DEVOLUCION, 
            CAST(ISNULL(DE20.Y20082, ISNULL(COMAPROREQDET.IMPPCOMPRA,0)) AS DECIMAL(18,4)) AS F_PRECIO_COMPRA,
            CAST(MA50.Y50C66 AS DECIMAL(18,4)) AS F_TIPO_CAMBIO,
            CASE WHEN COALESCE(DE20.afectoigv, COMAPROREQDET.afectoigv, 1) = 0 THEN ISNULL(tipo_igv_d20,0) ELSE 0 END AS F_IGV
        FROM ALMPEDIDOCAB
        INNER JOIN COMAPROREQCAB ON COMAPROREQCAB.SEQPEDCAB = ALMPEDIDOCAB.SEQPEDCAB
        INNER JOIN COMAPROREQDET ON COMAPROREQDET.SEQAPROCAB = COMAPROREQCAB.SEQAPROCAB 
        LEFT JOIN ma50 ON CONVERT(VARCHAR(10), ISNULL(COMAPROREQCAB.FCHCOTIZA,COMAPROREQCAB.FCHEDIT), 110) = CONVERT(VARCHAR(10), MA50.Y50006, 110)
        INNER JOIN niveles on niveles.SEQAPRODET = COMAPROREQDET.SEQAPRODET and COMAPROREQDET.codnivel = niveles.codnivel
        LEFT JOIN DE20 ON DE20.SEQAPRODET = COMAPROREQDET.SEQAPRODET
        LEFT JOIN CA20 ON CA20.SEQAPROCAB = COMAPROREQCAB.SEQAPROCAB AND DE20.id_oc = CA20.Y20005
        LEFT JOIN MA04 ON MA04.Y04001 = ISNULL(DE20.Y20001, COMAPROREQDET.SEQPRODUCTO)
        LEFT JOIN MA02 ON MA02.Y02011 = ISNULL(CA20.Y20011, COMAPROREQDET.SEQPROVEEDOR)
		LEFT JOIN MA00 MONEDA on MONEDA.codigo = CAST(ISNULL(NULLIF(CA20.Y20055,''), ISNULL(COMAPROREQDET.MONEDA,0)) AS INT)
		and MONEDA.clasif = '0002'
        WHERE ALMPEDIDOCAB.SERVREPUESTO = 'R'
        AND ALMPEDIDOCAB.FCHEDIT >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1)
        AND ALMPEDIDOCAB.FCHEDIT <  DATEFROMPARTS(YEAR(GETDATE()) + 1, 1, 1)
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
