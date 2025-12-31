from utils.path_csv import get_tmp_csv

JOB_NAME = "dim_flujo_compra"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQPROCESO': 'IdDimFlujoCompra',
    'DESCRIPCION': 'DescripcionFlujoCompra'
}

SELECT_ORIGEN = """
    SELECT
    SEQPROCESO,
    UPPER(TRIM(DESCRIPCION)) AS DESCRIPCION
    FROM dbo.CONFPROCESO
"""

TABLA_DESTINO = "DimFlujoCompra"