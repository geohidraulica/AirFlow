from core.paths import get_tmp_csv

JOB_NAME = "dim_forma_pago"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimFormaPago',
    'nombre': 'DescripcionFormaPago'
}
