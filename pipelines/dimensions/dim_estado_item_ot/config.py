from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_estado_item_ot"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'F_IdEstado': 'IdDimEstadoItemOt',
    'F_Estado': 'DescripcionEstadoItemOt'
}

SELECT_ORIGEN = """
    SELECT 
        MA00.SEQMA00 AS F_IdEstado,
        UPPER(MA00.nombre) AS F_Estado
    FROM MA00
    WHERE clasif = 'MTTODESTADET'
"""

TABLA_DESTINO = "DimEstadoItemOt"