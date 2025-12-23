from core.paths import get_tmp_csv

JOB_NAME = "dim_proveedor"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'cod_prov': 'IdDimProveedor',
    'ruc': 'RUC',
    'razonsocial': 'RazonSocial', 
}
