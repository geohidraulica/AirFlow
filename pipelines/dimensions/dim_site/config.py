from utils.path_csv import get_tmp_csv

JOB_NAME = "dim_site"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'Y06001': 'IdDimSite',
    'Y06002': 'DescripcionSite'
}

SELECT_ORIGEN = """
    SELECT 
        Y06001,
        UPPER(TRIM(Y06002)) AS Y06002 
    FROM MA06
"""

TABLA_DESTINO = "DimSite"