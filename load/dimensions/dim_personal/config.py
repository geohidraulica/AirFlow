from core.paths import get_tmp_csv

JOB_NAME = "dim_personal"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'id_personal': 'IdDimPersonal',
    'nombres': 'NombreApellido',
    'F_CARGO': 'Cargo', 
}
