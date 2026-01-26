from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "fact_stock_producto"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'MA08PK': 'IdFactStockProducto',
    'IDMA04': 'IdDimProducto',
    'STOCKGENERAL': 'StockGeneral',
    'DISPONIBLE': 'StockDisponible'
}

SELECT_ORIGEN = """
    ;WITH StockCuarentena AS (
        SELECT 
            sxu.codigo_sxu,
            sxu.almacen_sxu,
            SUM(sxu.stock_sxu) AS stock_cuarentena
        FROM ALM.stock_x_ubicacion sxu
        INNER JOIN MAE.ubicacion_almacen ua ON ua.idubicacionalmacen = sxu.ubicacion_sxu
        AND ua.nombre_ua LIKE '%CUARE%' AND ua.idestado_ua = 1
        GROUP BY sxu.codigo_sxu, sxu.almacen_sxu
    )
    SELECT 
        m08.MA08PK,
        m04.SEQMA04 AS IDMA04,
        m08.Y08056 AS STOCKGENERAL,
        m08.Y08056 - ISNULL(sc.stock_cuarentena, 0) AS DISPONIBLE
    FROM dbo.ma08 m08
    INNER JOIN dbo.ma04 m04 ON m08.Y08001 = m04.Y04001
    LEFT JOIN StockCuarentena sc ON sc.codigo_sxu = m04.Y04001 AND sc.almacen_sxu = m08.Y08596
    WHERE m08.Y08596 = '001'
    AND m08.Y08056 > 0
"""

TABLA_DESTINO = "FactStockProducto"