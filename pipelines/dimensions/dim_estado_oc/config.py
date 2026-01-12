from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_estado_oc"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'IdEstados': 'IdDimEstadoOc',
    'Nombre': 'DescripcionEstadoOc'
}

SELECT_ORIGEN = """
    SELECT DISTINCT
        estados.IdEstados,
        UPPER(TRIM(Nombre)) AS Nombre
    FROM CA20 
    INNER JOIN estados ON estados.IdEstados = CA20.EstadoDoc
    WHERE CA20.Y20004 = 'P'
"""

TABLA_DESTINO = "DimEstadoOc"