from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_estado_bomba"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idestado': 'IdDimEstadoBomba',
    'nom_estado': 'NombreEstadoBomba',
}

SELECT_ORIGEN = """
    SELECT 
        idestado,
        UPPER(nom_estado) AS nom_estado
    FROM MTTO.estado_bomba
    WHERE estado = 1
"""

TABLA_DESTINO = "DimEstadoBomba"