from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_periodo_tareo"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idperiodoxsite': 'IdDimPeriodoTareo',
    'anual': 'AnioPeriodo',
    'mes': 'MesPeriodo',
    'fchinicio': 'FechaInicioPeriodo',
    'fchfin': 'FechaFinPeriodo',
}

SELECT_ORIGEN = """
    SELECT DISTINCT
        idperiodoxsite,
        anual,
        mes,
        fchinicio,
        fchfin
    FROM PLANE.periodoxsite
"""

TABLA_DESTINO = "DimPeriodoTareo"