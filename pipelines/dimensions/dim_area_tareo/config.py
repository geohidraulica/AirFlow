from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_area_tareo"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimAreaTareo',
    'nombre': 'NombreAreaTareo'
}

SELECT_ORIGEN = """
    SELECT
        SEQMA00, 
        UPPER(nombre) AS nombre
    FROM ma00
    WHERE clasif = 'PLANAREA'
    AND replicated IS NOT NULL
"""

TABLA_DESTINO = "DimAreaTareo"