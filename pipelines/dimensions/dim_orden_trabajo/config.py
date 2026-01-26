from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_orden_trabajo"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'F_IDOT': 'IdDimOrdenTrabajo',
    'F_NROOT': 'NroOrdenTrabajo',
    'F_TipoOT': 'TipoOT',
    'F_ClasdificacionOt': 'ClasdificacionOt',
    'F_EstadoOT': 'EstadoOT',
}

SELECT_ORIGEN = """
    SELECT 
        MTTO.orden_trabajo_cab.id_orden_trab_cab AS F_IDOT,
        MTTO.orden_trabajo_cab.id_orden_trab_fis_cab AS F_NROOT,
        CASE WHEN MTTO.orden_trabajo_cab.ot_idtipoequip=1 THEN 'RB'  
        WHEN MTTO.orden_trabajo_cab.ot_idtipoequip=2 THEN 'FB' 
        WHEN MTTO.orden_trabajo_cab.ot_idtipoequip=3 THEN 'RT'
        WHEN MTTO.orden_trabajo_cab.ot_idtipoequip=4 THEN 'RM' 
        WHEN MTTO.orden_trabajo_cab.ot_idtipoequip=5 THEN 'FT' END AS F_TipoOT,
        CASE WHEN (PROD.orden_fabri_cab.correorden IS NULL) 
            THEN 'REPARACION'
            ELSE CASE WHEN  tsolictud.idtipofabri_sfbo = 1 THEN  'CORRIENTE' ELSE 'VENTA' END
        END AS F_ClasdificacionOt,
        UPPER(ma00estado.nombre) AS F_EstadoOT
    FROM MTTO.orden_trabajo_cab
    INNER JOIN MTTO.activos on MTTO.activos.idactivos = MTTO.orden_trabajo_cab.ot_bomba AND MTTO.activos.idordentrabajocab_a = MTTO.orden_trabajo_cab.id_orden_trab_cab 
    LEFT JOIN PROD.orden_fabri_cab ON PROD.orden_fabri_cab.idordenfabricab = MTTO.orden_trabajo_cab.idordenfabricab_otc
    OUTER APPLY
    (
        SELECT MAX(nro_requerimiento_cmc_sp) AS producto_solici, MAX(idtipofabri_sfbo) AS idtipofabri_sfbo
        FROM PROD.solicixordenfabcab 
        INNER JOIN CMP.solicitud_produccion_det ON solicitud_produccion_det.seqaprodet_spd = solicixordenfabcab.seqaprodet_spd_so
        INNER JOIN CMP.solicitud_produccion ON solicitud_produccion.seqaprocab_sp = solicitud_produccion_det.seqaprocab_spd
        INNER JOIN PLANE.solicitud_feature_bomba ON PLANE.solicitud_feature_bomba.seqaprocab_sfbo = CMP.solicitud_produccion.seqaprocab_sp
        WHERE PROD.solicixordenfabcab.idordenfabricab_so = PROD.orden_fabri_cab.idordenfabricab
        GROUP BY PROD.solicixordenfabcab.idordenfabricab_so
    ) AS tsolictud
    INNER JOIN dbo.ma00 ma00estado ON ma00estado.codigo = MTTO.orden_trabajo_cab.ot_estado_general AND ma00estado.clasif = 'MTTESTAOT'
    WHERE MTTO.orden_trabajo_cab.ot_idtipoequip IS NOT NULL
    AND orden_trabajo_cab.ot_fech_sist >= '2025-01-01' AND MTTO.activos.site_destino = '001'
"""

TABLA_DESTINO = "DimOrdenTrabajo"