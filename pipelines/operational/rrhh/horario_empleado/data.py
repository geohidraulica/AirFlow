SELECT_ORIGEN = """
    SELECT
        id_actividad_cab,
        idtiempo,
        id_tecnico_det,
        id_cod_maquina_det,
        fch_reg_det,
        id_usu_reg_det,
        idperiodo,
        hora_inidet,
        hora_findet
    FROM PROD.actividad_personal_det
    WHERE LEFT(idtiempo,6) = FORMAT(GETDATE(), 'yyyyMM')
"""

COLUMN_MAPPING = {
    # 'id_actividad_det': 'id_actividad_det',
    'id_actividad_cab': 'id_actividad_cab',
    'idtiempo': 'idtiempo',
    'id_tecnico_det': 'id_tecnico_det',
    'id_cod_maquina_det': 'id_cod_maquina_det',
    'fch_reg_det': 'fch_reg_det',
    'id_usu_reg_det': 'id_usu_reg_det',
    'idperiodo': 'idperiodo',
    'hora_inidet': 'hora_inidet',
    'hora_findet': 'hora_findet'
}

TABLA_DESTINO = "PROD.actividad_personal_det"

PRIMARY_KEY = "id_actividad_det" # SIEMPRE DEL DESTINO -> LADO DERECHO DE COLUMN_MAPPING