from utils.path_csv import get_tmp_csv

JOB_NAME = "dim_producto"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    "SEQMA04": "IdDimProducto",
    "Y04001": "CodigoProducto",
    "Y04002": "DescripcionProducto",
    "Familia": "Familia",
    "UnidadMedida": "UnidadMedida",
    "ClasificacionABC": "ClasificacionABC",
    "DescripcionEstado": "DescripcionEstado"
}

SELECT_ORIGEN = """
    SELECT
        ma04.SEQMA04,
        ma04.Y04001,
        ma04.Y04002,
        TRIM(UPPER(arbol.descrip)) AS Familia,
        UPPER(TRIM(ma00.descrip))  AS UnidadMedida,
        ISNULL(clasificacion_abc.relacion_cabc,'C') AS ClasificacionABC,
        CASE 
            WHEN ESTADO4 = 1 THEN 'ACTIVO'
            ELSE 'INACTIVO'
        END AS DescripcionEstado
    FROM ma04
    INNER JOIN arbol ON arbol.ARBOL_ID = ISNULL(ma04.Y04031, ma04.arbol_id)
    INNER JOIN ma00 ON ma00.codigo = ma04.Y04033 AND ma00.clasif = '0003'
    LEFT JOIN CMP.formula_pto_reorden_mes ON CMP.formula_pto_reorden_mes.seqma04_fprm = ma04.SEQMA04
    LEFT JOIN CMP.mes_clasificadorabcxcodigo ON CMP.mes_clasificadorabcxcodigo.seqma04_mcc = formula_pto_reorden_mes.seqma04_fprm
    LEFT JOIN CMP.clasificacion_abc clasificacion_abc  ON frecuencia_1_mcc = periodo1_cabc AND frecuencia_2_mcc = periodo2_cabc
"""

TABLA_DESTINO = "DimProducto"