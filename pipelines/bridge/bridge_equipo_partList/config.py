from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "bridge_equipo_partList"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idactivos': 'IdEquipoBomba',
    'idequipoxmodelos': 'IdDimPartList',
}

SELECT_ORIGEN = """
    SELECT DISTINCT
        A.idactivos,
        EXM.idequipoxmodelos
    FROM MTTO.activos A
    INNER JOIN MA00 M_MODEL ON M_MODEL.codigo = A.modelo  AND M_MODEL.clasif = 'PLAMODE'  AND M_MODEL.estado = 1 
    INNER JOIN MA00 M_POT  ON M_POT.codigo = A.potencia  AND M_POT.clasif = 'PLAPOTEN' AND M_POT.estado = 1
    INNER JOIN MA00 M_MARCA  ON M_MARCA.codigo = A.marca AND M_MARCA.clasif = 'PLAMARC' AND M_MARCA.estado = 1
    INNER JOIN MA00 M_TIPO ON M_TIPO.codigo = A.categoria AND M_TIPO.clasif = 'PLATIPBO' AND M_TIPO.estado = 1
    INNER JOIN TEMP.equipoxmodelos EXM ON M_MODEL.codigo = EXM.id_modelo_exm 
    AND M_POT.codigo   = EXM.id_potencia_exm
    AND M_MARCA.codigo = EXM.id_marca_exm
    AND M_TIPO.codigo  = EXM.id_tipo_bomba_exm
    INNER JOIN PROD.orden_fabri_cab ON PROD.orden_fabri_cab.idordenfabricab = a.idordenfabricab
    --LEFT JOIN MTTO.orden_trabajo_cab ON MTTO.orden_trabajo_cab.id_orden_trab_cab = PROD.orden_fabri_cab.idordenfabricab
    WHERE PROD.orden_fabri_cab.idestadood = 3
"""

TABLA_DESTINO = "BridgeEquipoPartList"