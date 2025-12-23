from core.paths import get_tmp_csv

JOB_NAME = "dim_tipo_proyecto"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'codigo': 'IdDimTipoProyecto',
    'nombre': 'DescripcionTipoProyecto'
}
