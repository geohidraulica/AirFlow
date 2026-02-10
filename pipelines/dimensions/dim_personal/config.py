from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_personal"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'id_personal': 'IdDimPersonal',
    'nombres': 'NombreApellido',
    'F_CARGO': 'Cargo', 
}

SELECT_ORIGEN = """
    SELECT 
        id_personal,
        TRIM(UPPER(CONCAT(nombres,' ' , apellidos))) as nombres, 
        UPPER(CONFNIVELPROCESO.DESCRIPCION) AS F_CARGO
    FROM dbo.personal
    LEFT JOIN RRHH.empleado ON RRHH.empleado.dni = dbo.personal.nro_doc
	LEFT JOIN CONFNIVELPROCESO ON CONFNIVELPROCESO.SEQNIVELPROC = personal.CODNIVELAPROBADOR
    UNION
    SELECT
        0 AS id_personal,
        'SIN ASIGNAR' AS nombres,
        'N/A' AS F_CARGO
"""

TABLA_DESTINO = "DimPersonal"