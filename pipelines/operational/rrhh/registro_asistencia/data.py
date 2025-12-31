SELECT_ORIGEN = """
    SELECT 
        idregasis, 
        idpersonareg,
        fch_registro, 
        fch_sistema, 
        inout, 
        horario_reg, 
        tardanza
    FROM RRHH.registro_asistencia
"""

SELECT_DESTINO = """
    SELECT
        idregasis, 
        idpersonareg,
        fch_registro, 
        fch_sistema, 
        inout, 
        horario_reg, 
        tardanza
    FROM RRHH.registro_asistencia
"""

COLUMN_MAPPING = {
    'idregasis': 'idregasis',
    'idpersonareg': 'idpersonareg',
    'fch_registro': 'fch_registro', 
    'fch_sistema': 'fch_sistema',
    'inout': 'inout',
    'horario_reg': 'horario_reg',
    'tardanza': 'tardanza',
}

TABLA_DESTINO = "RRHH.registro_asistencia"

PRIMARY_KEY = "idregasis" # SIEMPRE DEL DESTINO -> LADO DERECHO DE COLUMN_MAPPING