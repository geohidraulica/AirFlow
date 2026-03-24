from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_orden_fabricacion"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idordenfabricab': 'IdDimOrdenFabricacion',
    'correorden': 'CorrelativoFabricacion',
    'fila_of': 'FilaFabricacion',
    'fecha_creacion_of': 'FechaCreacionFabricacion'
}

SELECT_ORIGEN = """
    SELECT
        PROD.orden_fabri_cab.idordenfabricab,
        PROD.orden_fabri_cab.correorden,
        UPPER(of_fila.nombre) AS fila_of,
        CAST(fcreaorden AS DATE) AS fecha_creacion_of
    FROM PROD.orden_fabri_cab
    LEFT JOIN ma00 of_fila on of_fila.codigo = PROD.orden_fabri_cab.cod_fila_ofc AND of_fila.clasif = 'OF_TIPOFILA' AND of_fila.estado = 1
"""

TABLA_DESTINO = "DimOrdenFabricacion"