from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_partlist"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idequipoxmodelos' : 'IdDimPartList',
    'fch_updated_em' : 'FechaPartList',
    'nom_configuracion_exm' : 'NombrePartList',
    # 'F_Tipo' : 'F_Tipo',
    # 'F_Potencia' : 'F_Potencia',
    # 'F_Marca' : 'F_Marca',
    # 'F_Modelo' : 'F_Modelo',
    # 'nom_estado_exm' : 'NomEstadoEXM',
}

SELECT_ORIGEN = """
    SELECT
        idequipoxmodelos,
        CAST(fch_updated_em AS DATE) AS fch_updated_em,
        nom_configuracion_exm
        --,UPPER(PLATIPBO.nombre) AS F_Tipo,
        --UPPER(PLAPOTEN.nombre) AS F_Potencia,
        --UPPER(PLAMARC.nombre) AS F_Marca,
        --UPPER(PLAMODE.nombre) AS F_Modelo,
        --nom_estado_exm
    FROM TEMP.equipoxmodelos
    LEFT JOIN MA00 PLAMODE ON PLAMODE.codigo = TEMP.equipoxmodelos.id_modelo_exm AND PLAMODE.clasif = 'PLAMODE'        AND PLAMODE.estado = 1
    LEFT JOIN MA00 PLAPOTEN ON PLAPOTEN.codigo = TEMP.equipoxmodelos.id_potencia_exm AND PLAPOTEN.clasif = 'PLAPOTEN'  AND PLAPOTEN.estado = 1
    LEFT JOIN MA00 PLAMARC ON PLAMARC.codigo = TEMP.equipoxmodelos.id_marca_exm AND PLAMARC.clasif = 'PLAMARC'         AND PLAMARC.estado = 1
    LEFT JOIN MA00 PLATIPBO ON PLATIPBO.codigo = TEMP.equipoxmodelos.id_tipo_bomba_exm AND PLATIPBO.clasif = 'PLATIPBO' AND PLATIPBO.estado = 1
    WHERE TEMP.equipoxmodelos.idestado_partlist = 1
"""

TABLA_DESTINO = "DimPartList"