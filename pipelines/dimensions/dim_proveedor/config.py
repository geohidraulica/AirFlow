from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_proveedor"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'cod_prov': 'IdDimProveedor',
    'ruc': 'RUC',
    'razonsocial': 'RazonSocial', 
}

SELECT_ORIGEN = """
    SELECT
        cod_prov,
        UPPER(TRIM(Y02011)) AS ruc, 
        UPPER(TRIM(Y02002)) AS razonsocial
    FROM MA02
"""

TABLA_DESTINO = "DimProveedor"