from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "fact_tareo"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idtareodet': 'IdFactTareo',
    'idfecha_tad': 'IdDimFechaTareo',
    'idactivo_tad': 'IdDimEquipoBomba',
    'idsite_td': 'IdDimSite',
    'id_zona': 'IdDimZona',
    'id_nivel': 'IdDimNivel',
    'id_poza': 'IdDimPoza',
    'id_area': 'IdDimAreaTareo',
    'id_cable1': 'IdDimCableBomba1',
    'id_cable2': 'IdDimCableBomba2',
    'idestado_tad': 'IdDimEstadoBomba',
    'idperiodoxsite': 'IdDimPeriodoTareo',
    'ca1_mts_pp': 'MetrajeCable1',
    'ca2_mts_pp': 'MetrajeCable2'
}

SELECT_ORIGEN = """
    SELECT
        PLANE.tareo_det.idtareodet,
        PLANE.tareo_det.idfecha_tad,
        PLANE.tareo_det.idactivo_tad,
        PLANE.tareo_det.idsite_td,
        ma00zona.SEQMA00 as id_zona,
        ma00nivel.SEQMA00 as id_nivel,
        ma00poza.SEQMA00 AS id_poza,
        ma00area.SEQMA00 AS id_area,
        ma00cable1.SEQMA00 AS id_cable1,
        ma00cable2.SEQMA00 AS id_cable2,
        PLANE.tareo_det.idestado_tad,
        PLANE.periodoxsite.idperiodoxsite,
        MTTO.protocolo_prueba_cab.ca1_mts_pp,
	    MTTO.protocolo_prueba_cab.ca2_mts_pp
    FROM PLANE.tareo_det
    LEFT JOIN ma00 ma00zona ON idzona_td=ma00zona.codigo AND ma00zona.clasif='PLANZONA' AND ma00zona.[replicated]=idsite_td
    LEFT JOIN ma00 ma00nivel ON idnivel_td=ma00nivel.codigo AND ma00nivel.clasif='PLANENIVEL' AND ma00nivel.[replicated] = idsite_td
    LEFT JOIN ma00 ma00poza ON idpoza_td=ma00poza.codigo AND ma00poza.clasif='PLANPOZA' AND ma00poza.[replicated] = idsite_td
    LEFT JOIN ma00 ma00area ON idarea_td=ma00area.codigo AND ma00area.clasif='PLANAREA' AND ma00area.[replicated]=idsite_td
    LEFT JOIN ALM.det_guia_remi ON ALM.det_guia_remi.iddetguiaremi = PLANE.tareo_det.iddetguiaremi_td
    LEFT JOIN MTTO.protocolo_prueba_cab ON MTTO.protocolo_prueba_cab.idprotocolopruebacab =  ALM.det_guia_remi.idprotocolocab_dgr AND MTTO.protocolo_prueba_cab.flag_evaluacion_ppc = 1
    LEFT JOIN ma00 ma00cable1 ON ma00cable1.codigo = MTTO.protocolo_prueba_cab.idmetraje1_ppc AND ma00cable1.clasif = idtipo_cable1_ppc AND ma00cable1.estado = 1
    LEFT JOIN ma00 ma00cable2 ON ma00cable2.codigo = MTTO.protocolo_prueba_cab.idmetraje2_ppc AND ma00cable2.clasif = idtipo_cable2_ppc AND ma00cable2.estado = 1
    LEFT JOIN PLANE.periodoxsite ON periodoxsite.site = PLANE.tareo_det.idsite_td 
    AND PLANE.tareo_det.idperiodo_td = periodoxsite.idperiodo AND periodoxsite.anual = LEFT(PLANE.tareo_det.idfecha_tad,4)
    WHERE YEAR(LEFT(PLANE.tareo_det.idfecha_tad,4)) >= 2026
"""

UPDATE_RECIBIDO_QUERY = """
    UPDATE PLANE.tareo_det
    SET 		
        PLANE.tareo_det.idzona_td = NULL,
		PLANE.tareo_det.idnivel_td = NULL,
		PLANE.tareo_det.idpoza_td = NULL,
		PLANE.tareo_det.idarea_td = NULL
	WHERE idestado_tad = 21
"""

TABLA_DESTINO = "FactTareo"

