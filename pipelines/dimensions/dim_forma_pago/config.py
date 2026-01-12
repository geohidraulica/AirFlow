from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_forma_pago"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQMA00': 'IdDimFormaPago',
    'nombre': 'DescripcionFormaPago'
}

SELECT_ORIGEN = """
    SELECT 
    SEQMA00,
    TRIM(UPPER(nombre)) as nombre
    FROM ma00 
    WHERE clasif = 'FPAGO'
"""

TABLA_DESTINO = "DimFormaPago"