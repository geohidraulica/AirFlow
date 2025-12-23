from core.paths import get_tmp_csv

JOB_NAME = "dim_estado_item"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'IdEstados': 'IdDimEstadoItem',
    'Nombre': 'DescripcionEstadoItem'
}
