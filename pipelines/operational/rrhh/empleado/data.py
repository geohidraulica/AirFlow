SELECT_ORIGEN = """
    SELECT 
        idempleado,
        cargo_trab, 
        fcese,
        mcese,
        estado_emp,
        nomb_apell,
        dni,
        id_areapert,
        tipo_control,
        induccion,
        tipo_empleado,
        fvalidacion,
        faptitud,
        estado_aptitud
    FROM RRHH.empleado
"""

SELECT_DESTINO = """
    SELECT 
        idempleado,
        cargo_trab,
        fcese,
        mcese,
        estado_emp,
        nomb_apell,
        dni,
        id_areapert,
        tipo_control,
        induccion,
        tipo_empleado,
        fvalidacion,
        faptitud,
        estado_aptitud
    FROM RRHH.empleado
"""

COLUMN_MAPPING = {
    'idempleado': 'idempleado',
    'cargo_trab': 'cargo_trab',
    'fcese': 'fcese',       
    'mcese': 'mcese',
    'estado_emp': 'estado_emp',
    'nomb_apell': 'nomb_apell',
    'dni': 'dni',       
    'id_areapert': 'id_areapert',
    'tipo_control': 'tipo_control',
    'induccion': 'induccion',   
    'tipo_empleado': 'tipo_empleado',
    'fvalidacion': 'fvalidacion',
    'faptitud': 'faptitud',
    'estado_aptitud': 'estado_aptitud'
}

TABLA_DESTINO = "RRHH.empleado"

PRIMARY_KEY = "idempleado" # SIEMPRE DEL DESTINO -> LADO DERECHO DE COLUMN_MAPPING