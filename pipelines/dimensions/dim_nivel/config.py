from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_nivel"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimNivel',
    'nombre': 'NombreNivel',
}

SELECT_ORIGEN = """
    SELECT 
        SEQMA00, 
        nombre
    FROM ma00
    WHERE clasif = 'PLANENIVEL'
    AND replicated IS NOT NULL
    AND estado = 1
"""

TABLA_DESTINO = "DimNivel"