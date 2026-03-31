from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_partlist_material"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idmaterialesxbomba_det' : 'IdDimPartListMaterial',
    'idmaterialesxbomba_cab_det' : 'IdFactPartListDetalle',
    'seqma04_mxbo_det' : 'IdDimProducto',
    'cantidad_mxbo_det' : 'CantidadMaterial',
    'estado_mxbo' : 'EstadoMaterial',
}

SELECT_ORIGEN = """
    SELECT 
        idmaterialesxbomba_det,
        idmaterialesxbomba_cab_det,
        seqma04_mxbo_det,
        cantidad_mxbo_det,
        CASE WHEN idestado_mxbo_det = 1 THEN 'ACTIVO' ELSE 'NO-ACTIVO' END AS estado_mxbo
    FROM PROD.materiales_x_bomba_det
"""

TABLA_DESTINO = "DimPartListMaterial"