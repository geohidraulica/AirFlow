from utils.csv_path_manager import get_tmp_csv

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
    UNION
    SELECT 
    	CAST(0 AS INT) AS codigo,  
        UPPER('Sin Asignar') AS nombre
"""

TABLA_DESTINO = "DimTipoProyecto"