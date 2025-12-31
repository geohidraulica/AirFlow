from utils.path_csv import get_tmp_csv

JOB_NAME = "fact_compras"

TMP_CSV = get_tmp_csv(JOB_NAME)

COLUMN_MAPPING = {
    'SEQAPRODET': 'IdFactCompras',
    'F_FECHA_EMISION_REQ': 'FechaRequerimiento',
    'F_FECHA_EMISION_OC': 'FechaOrdenCompra',
    'F_FECHA_ENTREGA': 'FechaEntregaItem',
    #'F_AREA': 'IdDimArea',
    #'F_CECO': 'IdDimCentroCosto',
    'F_TIPO_FLUJO': 'IdDimFlujoCompra',
    'F_MONEDA': 'IdDimMoneda',
    'F_PRODUCTO': 'IdDimProducto',
    'F_PROVEEDOR': 'IdDimProveedor',
    'F_SITE': 'IdDimSite',
    'F_PGENERA': 'IdDimPersonalGenera',
    'F_PCOMPRADOR': 'IdDimPersonalComprador',
    'F_PBANDEJA': 'IdDimPersonalBandeja',
    'F_ESTADO_ITEM': 'IdDimEstadoItem',
    'F_ESTADO_OC': 'IdDimEstadoOC',
    'F_ESTADO_ALMACEN': 'IdDimEstadoAlmacen',
    'F_FPAGO': 'IdDimFormaPago',
    'F_TIPO_PROYECTO': 'IdDimTipoProyecto',
    'F_TIPO_REQ': 'IdDimTipoRequerimiento',
    'F_NRO_REQ': 'NroRequisicion',
    'F_NRO_OC': 'NroOrdenCompra',
    'F_CANT_PEDIDA': 'CantPedida',
    'F_CANT_ATENDIDA': 'CantAtendida',
    'F_CANT_DEVOLUCION': 'CantDevolucion',
    'F_PRECIO_COMPRA': 'PrecioCompra',
    'F_TIPO_CAMBIO': 'TipoCambio',
    'F_IGV': 'IGV'
}
