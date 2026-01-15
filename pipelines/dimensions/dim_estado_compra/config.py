from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_estado_compra"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'IdEstados': 'IdDimEstadoCompra',
    'Nombre': 'DescripcionEstadoCompra'
}

SELECT_ORIGEN = """
    SELECT DISTINCT
        estados.IdEstados,
        UPPER(TRIM(Nombre)) AS Nombre
    FROM estados 
"""

TABLA_DESTINO = "DimEstadoCompra"