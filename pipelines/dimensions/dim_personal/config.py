from utils.path_csv import get_tmp_csv

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
        TRIM(UPPER(ISNULL(RRHH.empleado.cargo_trab,personal.cargo))) AS F_CARGO
    FROM dbo.personal
    LEFT JOIN RRHH.empleado ON RRHH.empleado.dni = dbo.personal.nro_doc
    WHERE estado = 1
"""

TABLA_DESTINO = "DimPersonal"