from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_zona"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimZona',
    'nombre': 'NombreZona'
}

SELECT_ORIGEN = """
    SELECT 
        SEQMA00,
        nombre
    FROM ma00
    WHERE clasif = 'PLANZONA'
    AND replicated IS NOT NULL
    AND estado = 1
"""

TABLA_DESTINO = "DimZona"