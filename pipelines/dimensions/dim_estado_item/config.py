from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_estado_item"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'IdEstados': 'IdDimEstadoItem',
    'Nombre': 'DescripcionEstadoItem'
}

SELECT_ORIGEN = """
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

TABLA_DESTINO = "DimEstadoItem"