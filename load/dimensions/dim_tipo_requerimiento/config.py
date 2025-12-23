from core.paths import get_tmp_csv

JOB_NAME = "dim_tipo_requerimiento"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'id_origen_rq_ma00': 'IdDimTipoRequerimiento',
    'F_TIPO_RQ': 'DescripcionTipoRequerimiento'
}
