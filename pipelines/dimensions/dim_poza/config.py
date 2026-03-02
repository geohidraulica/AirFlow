from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_poza"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimPoza',
    'nombre': 'NombrePoza',
}

SELECT_ORIGEN = """
    SELECT
        SEQMA00, 
        nombre
    FROM ma00
    WHERE clasif = 'PLANPOZA'
    AND replicated IS NOT NULL
    AND estado = 1
"""

TABLA_DESTINO = "DimPoza"