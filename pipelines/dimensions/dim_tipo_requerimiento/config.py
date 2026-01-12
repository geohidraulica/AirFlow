from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_tipo_requerimiento"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'id_origen_rq_ma00': 'IdDimTipoRequerimiento',
    'F_TIPO_RQ': 'DescripcionTipoRequerimiento'
}

SELECT_ORIGEN = """
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

TABLA_DESTINO = "DimTipoRequerimiento"