from core.paths import get_tmp_csv

JOB_NAME = "dim_producto"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    "SEQMA04": "IdDimProducto",
    "Y04001": "CodigoProducto",
    "Y04002": "DescripcionProducto",
    "Familia": "Familia",
    "UnidadMedida": "UnidadMedida",
    "ClasificacionABC": "ClasificacionABC",
    "DescripcionEstado": "DescripcionEstado"
}
