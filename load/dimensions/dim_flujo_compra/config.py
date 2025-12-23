from core.paths import get_tmp_csv

JOB_NAME = "dim_flujo_compra"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQPROCESO': 'IdDimFlujoCompra',
    'DESCRIPCION': 'DescripcionFlujoCompra'
}
