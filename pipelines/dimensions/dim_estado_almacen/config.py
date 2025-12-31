from utils.path_csv import get_tmp_csv

JOB_NAME = "dim_estado_almacen"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'id_estado_aten_alm_crd': 'IdDimEstadoAlmacen',
    'Nombre': 'DescripcionEstadoAlmacen'
}

SELECT_ORIGEN = """
    SELECT DISTINCT
        COMAPROREQDET.id_estado_aten_alm_crd,
        UPPER(TRIM(Nombre)) AS Nombre
    FROM ALMPEDIDOCAB
    INNER JOIN COMAPROREQCAB ON COMAPROREQCAB.SEQPEDCAB = ALMPEDIDOCAB.SEQPEDCAB
    INNER JOIN COMAPROREQDET ON COMAPROREQDET.SEQAPROCAB = COMAPROREQCAB.SEQAPROCAB
    INNER JOIN estados ON estados.IdEstados = id_estado_aten_alm_crd
    WHERE ALMPEDIDOCAB.SERVREPUESTO = 'R'
"""

TABLA_DESTINO = "DimEstadoAlmacen"