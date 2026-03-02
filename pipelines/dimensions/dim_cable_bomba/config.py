from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_cable_bomba"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimCableBomba',
    'nombre': 'NombreCableBomba'
}

SELECT_ORIGEN = """
    SELECT 
        SEQMA00,
        nombre
    FROM ma00
    WHERE clasif IN ('MTTOPPTC2', 'MTTOPPTC3', 'MTTOPPTC1') AND  estado = 1
"""

TABLA_DESTINO = "DimCableBomba"