from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "dim_tiempo"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'IdDimFecha': 'IdDimFecha',
    'Fecha': 'Fecha',
    'Anio': 'Anio',
    'Semestre': 'Semestre',
    'Trimestre': 'Trimestre',
    'MesAnio': 'MesAnio',
    'NroMes': 'NroMes',
    'Semana': 'Semana',
    'Dia': 'Dia',
    'NroDiaSemana': 'NroDiaSemana',
    'EsFinSemana': 'EsFinSemana',
    'NombreMes': 'NombreMes',
}

SELECT_ORIGEN = """
    DECLARE @FechaInicio DATE = '2000-01-01';
    DECLARE @FechaFin    DATE = '2050-12-31';

    SET DATEFIRST 1; -- Lunes

    ;WITH Calendario AS (
        SELECT @FechaInicio AS Fecha
        UNION ALL
        SELECT DATEADD(DAY, 1, Fecha)
        FROM Calendario
        WHERE Fecha < @FechaFin
    )
    SELECT
        CONVERT(INT, CONVERT(CHAR(8), Fecha, 112)) AS IdDimFecha,
        CAST(Fecha AS DATE) AS Fecha,
        CAST(YEAR(Fecha) AS VARCHAR(10)) AS Anio,
        'S' + CAST(CASE WHEN MONTH(Fecha) <= 6 THEN 1 ELSE 2 END AS VARCHAR(10)) AS Semestre,
        'T' + CAST(DATEPART(QUARTER, Fecha) AS VARCHAR(10)) AS Trimestre,
        CONVERT(VARCHAR(7), Fecha, 120) AS MesAnio,
        RIGHT('0' + CAST(MONTH(Fecha) AS VARCHAR(10)), 2) AS NroMes,
        'Sem' + CAST(DATEPART(ISO_WEEK, Fecha) AS VARCHAR(10)) AS Semana,
        CAST(DATENAME(WEEKDAY, Fecha) AS VARCHAR(10)) AS Dia,
        CAST(DATEPART(WEEKDAY, Fecha) AS VARCHAR(10)) AS NroDiaSemana,
        CASE 
            WHEN DATENAME(WEEKDAY, Fecha) IN ('sábado','domingo') THEN 'Sí'
            ELSE 'No'
        END AS EsFinSemana,
		DATENAME(MONTH, Fecha) as NombreMes
    FROM Calendario
    OPTION (MAXRECURSION 0);

"""

TABLA_DESTINO = "DimTiempo"