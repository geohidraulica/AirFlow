from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "fact_kardex"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'F_DETGUIA': 'IdFactKardex',
    'F_FCHMOV': 'FechaMovimientoKardex',
    'F_PRODUCTO': 'IdDimProducto',
    'F_TIPO_MOVIMIENTO': 'IdDimTipoMovimiento',
    'F_COMPRAS':'IdFactCompras',
    'F_CANTIDAD':'CantidadKardex',
}

SELECT_ORIGEN = """
    SELECT --SOLO SALIDAS
        CAST(DE10.IdDetGuia AS INT) AS F_DETGUIA,
        CAST(DE10.FCHMOV AS DATE) AS F_FCHMOV,
        MA04.SEQMA04 AS F_PRODUCTO,
        ma00.SEQMA00 AS F_TIPO_MOVIMIENTO,
        ISNULL(DE10.seqaprodet_de10, DE10.SEQLIQ) AS F_COMPRAS,
        DE10.Y10079 AS F_CANTIDAD
    FROM CA10 
    INNER JOIN DE10 ON DE10.IdCabGuia = CA10.IdCabGuia
    INNER JOIN ma04 ON ma04.Y04001 = de10.Y10001
    LEFT JOIN dbo.ma00 ON dbo.ca10.Y10075 = dbo.ma00.codigo
    AND ma00.clasif = ( CASE WHEN dbo.ca10.Y10004 = 'I' THEN '0004' ELSE '0005' END )
"""

TABLA_DESTINO = "FactKardex"