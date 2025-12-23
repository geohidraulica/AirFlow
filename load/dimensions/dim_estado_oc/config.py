from core.paths import get_tmp_csv

JOB_NAME = "dim_estado_oc"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'IdEstados': 'IdDimEstadoOc',
    'Nombre': 'DescripcionEstadoOc'
}
