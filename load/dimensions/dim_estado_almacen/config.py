from core.paths import get_tmp_csv

JOB_NAME = "dim_estado_almacen"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'id_estado_aten_alm_crd': 'IdDimEstadoAlmacen',
    'Nombre': 'DescripcionEstadoAlmacen'
}
