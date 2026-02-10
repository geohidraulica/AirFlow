from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_equipo_bomba"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idactivos': 'IdEquipoBomba',
    'F_CodigoBomba': 'CodigoEquipo',
    'F_Tipo': 'TipoEquipo',
    'F_Potencia': 'PotenciaEquipo',
    'F_Marca': 'MarcaEquipo',
    'F_Modelo': 'ModeloEquipo',
    'F_Estandar': 'Estandar',
    'F_TipoEquipamiento': 'TipoEquipamiento',
}

SELECT_ORIGEN = """
    SELECT DISTINCT
        activos.idactivos,
        UPPER(activos.codsmg) as F_CodigoBomba,
        UPPER(PLATIPBO.nombre) AS F_Tipo,
        UPPER(PLAPOTEN.nombre) AS F_Potencia,
        UPPER(PLAMARC.nombre) AS F_Marca,
        UPPER(PLAMODE.nombre) AS F_Modelo,
        UPPER(ESTANDAR.nombre) AS F_Estandar,
        UPPER(EQUIPAMIENTO.nombre) AS F_TipoEquipamiento
    FROM MTTO.activos
    LEFT JOIN MA00 PLAMODE ON PLAMODE.codigo = MTTO.activos.modelo AND PLAMODE.clasif = 'PLAMODE'        AND PLAMODE.estado = 1
    LEFT JOIN MA00 PLAPOTEN ON PLAPOTEN.codigo = MTTO.activos.potencia AND PLAPOTEN.clasif = 'PLAPOTEN'  AND PLAPOTEN.estado = 1
    LEFT JOIN MA00 PLAMARC ON PLAMARC.codigo = MTTO.activos.marca AND PLAMARC.clasif = 'PLAMARC'         AND PLAMARC.estado = 1
    LEFT JOIN MA00 PLATIPBO ON PLATIPBO.codigo = MTTO.activos.categoria AND PLATIPBO.clasif = 'PLATIPBO' AND PLATIPBO.estado = 1
    LEFT JOIN MA00 EQUIPAMIENTO ON EQUIPAMIENTO.codigo = ISNULL(MTTO.activos.tipo_equipamiento_pp, 0) AND EQUIPAMIENTO.clasif = 'MTTOTIPEQUIP'
    LEFT JOIN MA00 ESTANDAR ON ESTANDAR.codigo = ISNULL(MTTO.activos.estandar_pp, 0) AND ESTANDAR.clasif = 'MTTOESTANDAR'
    --INNER JOIN MTTO.orden_trabajo_cab on MTTO.activos.idactivos = MTTO.orden_trabajo_cab.ot_bomba --AND MTTO.activos.idordentrabajocab_a = MTTO.orden_trabajo_cab.id_orden_trab_cab 
    --WHERE orden_trabajo_cab.ot_fech_sist >= '2023-01-01' --AND MTTO.activos.site_destino = '001'
"""

TABLA_DESTINO = "DimEquipoBomba"