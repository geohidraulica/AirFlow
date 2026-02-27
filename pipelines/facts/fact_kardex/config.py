from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "fact_kardex"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'F_IDDETGUIA': 'IdFactKardex',
    'F_FMOV': 'FechaMovimientoKardex',
    'F_CODIGO_PRODUCTO': 'IdDimProducto',
    'F_TIPO_MOVIMIENTO': 'IdDimTipoMovimiento',
    'F_COD_COSTOS': 'IdDimCentroCosto',
    'F_IDEQUIPO': 'IdEquipoBomba',
    'F_IDORDENTRABAJO': 'IdDimOrdenTrabajo',
    'F_CANTIDAD': 'CantidadKardex',
    'F_PRECIO_UNITARIO_SOL': 'PrecioUnitario',
}

SELECT_ORIGEN = """
    SELECT 
        DE10.IdDetGuia AS F_IDDETGUIA,
        CAST(DE10.FCHMOV AS DATE) AS F_FMOV,
        MA04.SEQMA04 AS F_CODIGO_PRODUCTO,
        MA00.SEQMA00 AS F_TIPO_MOVIMIENTO,
        DE10.CodCostos AS F_COD_COSTOS,
        activos2.idactivos AS F_IDEQUIPO,
        MTTO.orden_trabajo_cab.id_orden_trab_cab AS F_IDORDENTRABAJO,
        --CAST(DE10.Y10079 * DE10.PrecioUnitario_Soles AS DECIMAL(18,2)) AS SUB_TOTAL_SOL,
        DE10.Y10079 AS F_CANTIDAD,
        DE10.PrecioUnitario_Soles AS F_PRECIO_UNITARIO_SOL
    FROM DE10
    INNER JOIN CA10 ON CA10.IdCabGuia = DE10.IdCabGuia
    LEFT JOIN ALM.block_guiaremi_cab ON block_guiaremi_cab.idblockguiaremicab = CA10.idblockguiaremicab_ca10
    LEFT JOIN MA00 ON codigo = CASE WHEN block_guiaremi_cab.tipo_guia_bgc = 'OC' THEN 7 ELSE ca10.Y10075 END 
    AND clasif = CASE WHEN DE10.Y10004 = 'S' THEN '0005' ELSE '0004' END
    LEFT JOIN MA04 ON MA04.Y04001 = DE10.Y10001
    ---Jhon
    LEFT JOIN MTTO.orden_trabajo_cab ON CAST(MTTO.orden_trabajo_cab.id_orden_trab_fis_cab AS varchar(100)) = CAST(CA10.WONUM AS varchar(100))
    LEFT JOIN MTTO.activos activos2 ON Isnull(CA10.MODELOAUX, DE10.SEQEQUIPO) = activos2.codsmg
    WHERE
    de10.Y10073 = '001'
"""

TABLA_DESTINO = "FactKardex"