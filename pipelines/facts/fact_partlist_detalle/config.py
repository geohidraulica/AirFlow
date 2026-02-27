from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "fact_partlist_detalle"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idmaterialesxbomba': 'IdFactPartListDetalle',
    'idequipoxmodelos': 'IdDimPartList',
    'seqma04_mxbo': 'IdDimProducto',
    # 'seqma04_mxbo_det': 'IdDimProductoComponente',  
    'cantidad_mxbo': 'Cantidad',
    # 'cantidad_mxbo_det': 'CantidadComponente'
}

SELECT_ORIGEN = """
    SELECT 
        PROD.materiales_x_bomba.idmaterialesxbomba,
        TEMP.equipoxmodelos.idequipoxmodelos,
        PROD.materiales_x_bomba.seqma04_mxbo,
        --PROD.materiales_x_bomba_det.seqma04_mxbo_det,
        --CASE 
        --    WHEN PROD.materiales_x_bomba_det.seqma04_mxbo_det IS NOT NULL THEN 1
        --    ELSE 0
        --END AS EsDetalle,
        PROD.materiales_x_bomba.cantidad_mxbo
        --PROD.materiales_x_bomba_det.cantidad_mxbo_det
    FROM PROD.materiales_x_bomba
    INNER JOIN TEMP.equipoxmodelos ON TEMP.equipoxmodelos.id_configuracion_exm = PROD.materiales_x_bomba.cod_configuracion_mxb
    WHERE PROD.materiales_x_bomba.idestado_mxbo = 1
"""

TABLA_DESTINO = "FactPartListDetalle"