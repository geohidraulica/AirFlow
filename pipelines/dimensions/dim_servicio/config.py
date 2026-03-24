from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_servicio"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMASERV': 'IdDimServicio',
    'Y04001': 'CodigoServicio',
    'Y04002': 'DescripcionServicio',
    'familia': 'FamiliaServicio',
    'sub_familia': 'SubFamiliaServicio',
    'flujo_servicio': 'TipoFlujoServicio'
}

SELECT_ORIGEN = """
    SELECT DISTINCT 
        dbo.maestro_servicios.SEQMASERV, 
        dbo.maestro_servicios.Y04001, 
        UPPER(dbo.maestro_servicios.Y04002) AS Y04002,
        UPPER(dbo.arbol.descrip) AS familia,
        UPPER(MAE.clase.nomclase) AS sub_familia,
        UPPER(CONFPROCESO.DESCRIPCION) AS flujo_servicio
    FROM dbo.maestro_servicios
    LEFT JOIN dbo.arbol ON dbo.arbol.ARBOL_ID = Y04031 AND dbo.arbol.tipo = 'S'
    LEFT JOIN MAE.clase ON clase.idclase = maestro_servicios.idclase
    LEFT JOIN dbo.ma00 ON maestro_servicios.estado=dbo.ma00.codigo AND dbo.ma00.clasif='ESTIT'
    LEFT JOIN CONFPROCESO ON CONFPROCESO.SEQPROCESO = dbo.maestro_servicios.proceso_ms AND (ISNULL(param2, '') = 'S' or  ISNULL(param5, '') = 'S')
"""

TABLA_DESTINO = "DimServicio"