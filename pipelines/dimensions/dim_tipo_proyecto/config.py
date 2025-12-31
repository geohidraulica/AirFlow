from utils.path_csv import get_tmp_csv

JOB_NAME = "dim_tipo_proyecto"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'codigo': 'IdDimTipoProyecto',
    'nombre': 'DescripcionTipoProyecto'
}

SELECT_ORIGEN = """
    SELECT 
        CAST(codigo AS INT) AS codigo,  
        UPPER(nombre) AS nombre
    FROM ma00
    WHERE clasif  = 'TIPROYECTLOG'
"""

TABLA_DESTINO = "DimTipoProyecto"