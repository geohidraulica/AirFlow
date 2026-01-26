from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "fact_mantenimiento"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'F_IDOTDET': 'IdFactMantenimiento',
    'F_REGISTRO_OT': 'IdDimFechaRegistro',
    'F_IDOTCAB': 'IdDimOrdenTrabajo',
    'F_ESTADOITEMOT': 'IdDimEstadoItemOt',
    'F_PRODUCTO': 'IdDimProducto',
    'F_EQUIPO': 'IdEquipoBomba',
    'F_CANT_SOLI': 'CantidadSolicitada',
    'F_CANT_ATEN': 'CantidadAtendida',

}

SELECT_ORIGEN = """
    SELECT
        MTTO.orden_trabajo_det.id_orden_trabajo_det AS F_IDOTDET,
        CAST(CONVERT(char(8), ot_fech_sist, 112) AS int) AS F_REGISTRO_OT,
        MTTO.orden_trabajo_det.id_orden_trab_cab AS F_IDOTCAB,
        ESTADOOTDET.SEQMA00 AS F_ESTADOITEMOT,
        MTTO.orden_trabajo_det.otd_prdiddet AS  F_PRODUCTO,
        MTTO.activos.idactivos AS F_EQUIPO,
        otd_cantidad AS F_CANT_SOLI,
        ISNULL(otd_cnt_atendida,0) AS F_CANT_ATEN
    FROM MTTO.orden_trabajo_det
    INNER JOIN MTTO.orden_trabajo_cab ON MTTO.orden_trabajo_cab.id_orden_trab_cab = MTTO.orden_trabajo_det.id_orden_trab_cab
    INNER JOIN MTTO.activos on MTTO.activos.idactivos = MTTO.orden_trabajo_cab.ot_bomba AND MTTO.activos.idordentrabajocab_a = MTTO.orden_trabajo_cab.id_orden_trab_cab 
    INNER JOIN MA00 ESTADOOTDET ON ESTADOOTDET.codigo = ISNULL(MTTO.orden_trabajo_det.otd_idestadodet, 1) AND ESTADOOTDET.clasif = 'MTTODESTADET'
    WHERE orden_trabajo_cab.ot_fech_sist >= '2025-01-01' AND MTTO.activos.site_destino = '001'
"""

TABLA_DESTINO = "FactMantenimiento"