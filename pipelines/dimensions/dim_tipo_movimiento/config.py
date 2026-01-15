from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_tipo_movimiento"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimTipoMovimiento',
    'codigo': 'TipoMovimiento',
    'descrip': 'DescripcionMovimiento'
}

SELECT_ORIGEN = """
    SELECT 
    ma00.SEQMA00,
    CASE WHEN clasif ='0004' THEN 'I' ELSE 'S' END AS codigo, 
    UPPER(TRIM(descrip)) AS descrip
    FROM ma00 
    WHERE clasif in ('0004','0005')
"""

TABLA_DESTINO = "DimTipoMovimiento"