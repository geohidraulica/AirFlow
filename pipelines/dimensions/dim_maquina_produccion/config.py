from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_maquina_produccion"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'codigo_maquina_chr': 'IdDimMaquinaProduccion',
    'descrip_maquina_chr': 'DescripcionMaquina'
}

SELECT_ORIGEN = """
    SELECT DISTINCT 
        codigo_maquina_chr,
        descrip_maquina_chr
    FROM PROD.consolidado_hojaruta 
"""

TABLA_DESTINO = "DimMaquinaProduccion"