from utils.csv_path_manager import get_tmp_csv

JOB_NAME = "fact_hoja_ruta"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'idconsolidado_chr': 'IdFactHojaRuta',
	'idpkorigenfab_chr': 'IdDimOrdenFabricacion',
	'idpkorigenma04_chr': 'IdDimProducto',
	'idpkorigenServicio_chr': 'IdDimServicio',
	'idactivos': 'IdDimEquipoBomba',
	'codigo_maquina_chr': 'IdDimMaquinaProduccion',
	'correlativohr_chr': 'CorrelativoHR',
	'secuencia_hr_chr': 'SecuenciaHR',
	'Semana': 'SemanaProgramadaHR',
	'semana_calculada_chr': 'SemanaCalculadaHR',
	'cantotalpiezas_chr': 'CantidadTotalHR',
	'horas_acumuladas_chr': 'HorasAcumuladasHR',
	'cantprogramada_chr': 'CantidadProgramadaHR',
	'tiempopreparado_chr': 'TiempoPreparadoHR',
	'tiempomaquinado_chr': 'TiempoMaquinadoHR',
	'cnt_completado_chr': 'CantidadCompletadaHR',
	'cnt_pieza_chr': 'CantidadFaltanteHR',
	'horastotales_chr': 'HorasTotalesHR',
	'estado_hr_chr': 'EstadoHR',
	'orden_secuencia_chr': 'OrdenHR'
}

SELECT_ORIGEN = """
    SELECT DISTINCT
		PROD.consolidado_hojaruta.idconsolidado_chr ,  --IdFactHojaRuta
		PROD.consolidado_hojaruta.idpkorigenfab_chr, --IdDimOrdenFabricacion
		PROD.consolidado_hojaruta.idpkorigenma04_chr, --IdDimProducto
		PROD.consolidado_hojaruta.idpkorigenServicio_chr, --IdDimServicio
		MTTO.activos.idactivos, --IdDimEquipoBomba
		PROD.consolidado_hojaruta.codigo_maquina_chr, --IdDimMaquinaProduccion
		PROD.consolidado_hojaruta.correlativohr_chr, --CorrelativoHR
		CAST(secuencia_hr_chr AS INT) AS secuencia_hr_chr,  --SecuenciaHR
		ADM.tiempo.Semana,-- SemanaProgramada
		PROD.consolidado_hojaruta.semana_calculada_chr, --SemanaCalculadaHR
		PROD.consolidado_hojaruta.cantotalpiezas_chr, --CantidadTotalHR
		PROD.consolidado_hojaruta.horas_acumuladas_chr, --HorasAcumuladasHR
		ISNULL(PROD.consolidado_hojaruta.cantprogramada_chr,0) AS cantprogramada_chr, --CantidadProgramadaHR
		ISNULL(PROD.consolidado_hojaruta.tiempopreparado_chr,0) AS tiempopreparado_chr,--TiempoPreparadoHR
		ISNULL(PROD.consolidado_hojaruta.tiempomaquinado_chr,0) AS tiempomaquinado_chr, --TiempoMaquinadoHR
		ISNULL(PROD.consolidado_hojaruta.cnt_completado_chr, 0) AS cnt_completado_chr, --CantidadCompletadaHR
		ISNULL(PROD.consolidado_hojaruta.cnt_pieza_chr, 0) AS cnt_pieza_chr,--CantidadFaltanteHR
		ISNULL(PROD.consolidado_hojaruta.horastotales_chr,0) AS horastotales_chr, --HorasTotalesHR
		ma00estado.nombre AS estado_hr_chr, --EstadoHR
		ISNULL(PROD.consolidado_hojaruta.orden_secuencia_chr, 0) AS orden_secuencia_chr -- OrdenHR
	FROM PROD.consolidado_hojaruta
	LEFT JOIN ADM.tiempo ON tiempo.Fecha = fecha_semana_chr
	LEFT JOIN ma00 ma00estado ON ma00estado.codigo = consolidado_hojaruta.id_estado_hr_chr AND ma00estado.clasif = 'ESTADOHRU'
	LEFT JOIN PROD.HojaRutaCab ON correlativohr_chr = correlativo_hruc
	LEFT JOIN ALM.conf_serv_ot ON idordenfabricab_hruc = idordenfabcab_csot
	LEFT JOIN MTTO.activos ON idactivos = idactivo_csot
	WHERE estado_programado_chr = 1
	AND ISNULL(estado_oculto_chr,0) = 0
"""

TABLA_DESTINO = "FactHojaRuta"