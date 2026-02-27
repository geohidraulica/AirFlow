from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_centro_costo"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'id_cc': 'IdDimCentroCosto',
    'cod_cc': 'CodigoCentroCosto',
    'descripcion_cc': 'DescripcionCentroCosto'
}

SELECT_ORIGEN = """
    SELECT 
        arbol_cc.ARBOL_ID as id_cc,
        p1.sub1 + '.' + p2.sub1 + '.' + RIGHT('00' + TRY_CAST(TRY_CAST(SUBSTRING(arbol_cc.sub1, 0, 5) AS INT) AS VARCHAR(20)), 2)  AS cod_cc,
        TRIM(arbol_cc.descrip) AS descripcion_cc
    FROM dbo.arbol p1 --ON p1.tipo = 'C'
    INNER JOIN dbo.arbol p2 ON p1.ARBOL_ID = p2.CO_PADRE AND p2.tipo = 'C' AND p1.tipo = 'C'
    INNER JOIN dbo.arbol arbol_cc ON arbol_cc.tipo = 'C' AND p2.ARBOL_ID = arbol_cc.CO_PADRE
"""

TABLA_DESTINO = "DimCentroCosto"