from utils.path_csv import get_tmp_csv

JOB_NAME = "dim_moneda"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimMoneda',
    'descrip': 'DescripcionMoneda',
    'nombre': 'Simbolo', 
}

SELECT_ORIGEN = """
    SELECT
    SEQMA00,
    UPPER(descrip) AS descrip,
    nombre
    FROM ma00
    WHERE clasif = '0002' and estado = 1
"""

TABLA_DESTINO = "DimMoneda"