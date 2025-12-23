from core.paths import get_tmp_csv

JOB_NAME = "dim_moneda"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimMoneda',
    'descrip': 'DescripcionMoneda',
    'nombre': 'Simbolo', 
}
