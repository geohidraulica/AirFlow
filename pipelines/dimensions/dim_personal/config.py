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
        '-' AS F_CARGO
    FROM dbo.personal
    LEFT JOIN RRHH.empleado ON RRHH.empleado.dni = dbo.personal.nro_doc
    UNION
    SELECT
        0 AS id_personal,
        'SIN ASIGNAR' AS nombres,
        '-' AS F_CARGO
    --WHERE estado = 1
"""

TABLA_DESTINO = "DimPersonal"