from core.paths import get_tmp_csv

JOB_NAME = "dim_site"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'Y06001': 'IdDimSite',
    'Y06002': 'DescripcionSite'
}
