from zeep import Client
from datetime import date, timedelta
from typing import Optional, Dict, Union
import logging

# Configurar logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def get_exchange_rate_for_date(fecha: date) -> float:
    """
    Obtiene el tipo de cambio de compra para una fecha específica.
    Si no hay tipo de cambio para la fecha dada, busca el tipo de cambio
    más reciente anterior a esa fecha.
    """
    # Intentar obtener el tipo de cambio para la fecha especificada
    resultado = get_exchange_rate(fecha)
    if resultado:
        return resultado['compra']
    
    # Si no hay resultado, buscar en fechas anteriores
    current_date = fecha
    while current_date > fecha - timedelta(days=30):  # Límite de 30 días hacia atrás
        current_date -= timedelta(days=1)
        if is_valid_business_date(current_date):
            resultado = get_exchange_rate(current_date)
            if resultado:
                print(f"Usando tipo de cambio del {current_date} para {fecha}")
                return resultado['compra']
    
    return None


def is_valid_business_date(fecha: date) -> bool:
    """
    Verifica si la fecha es un día hábil (no fin de semana)
    """
    return fecha.weekday() < 5

def get_exchange_rate(fecha: date) -> Optional[Dict[str, Union[date, float]]]:
    """
    Obtiene el tipo de cambio del BCU para una fecha específica.
    
    Args:
        fecha (date): Fecha para la cual se quiere obtener el tipo de cambio
        
    Returns:
        Optional[Dict[str, Union[date, float]]]: Diccionario con la fecha y los valores de compra/venta,
        o None si hay un error
    """
    # Verificar si es día hábil
    if not is_valid_business_date(fecha):
        logger.warning(f"Fecha no válida: {fecha} (debe ser día hábil)")
        return None

    try:
        # Crear cliente SOAP
        wsdl_url = "https://cotizaciones.bcu.gub.uy/wscotizaciones/servlet/awsbcucotizaciones?wsdl"
        client = Client(wsdl_url)

        fecha_str = fecha.strftime("%Y-%m-%d")

        # Parámetros de entrada
        params = {
            'Entrada': {
                'Moneda': {'item': 2225},  # Código de DÓLAR USA
                'FechaDesde': fecha_str,
                'FechaHasta': fecha_str,
                'Grupo': 2  # Cotizaciones locales
            }
        }

        # Llamar al servicio
        response = client.service.Execute(**params)

        # Procesar la respuesta
        if response and hasattr(response, 'respuestastatus'):
            if response.respuestastatus.status == 1:
                if (hasattr(response, 'datoscotizaciones') and 
                    hasattr(response.datoscotizaciones, 'datoscotizaciones.dato')):
                    
                    datos = response.datoscotizaciones['datoscotizaciones.dato']
                    if datos:
                        if isinstance(datos, list):
                            dato = datos[0]
                        else:
                            dato = datos
                            
                        return {
                            'fecha': dato.Fecha,
                            'compra': float(dato.TCC),
                            'venta': float(dato.TCV)
                        }
                    
        return None
    except Exception as e:
        logger.error(f"Error al consultar el servicio para {fecha}: {str(e)}")
        return None

if __name__ == "__main__":
    # Ejemplo de uso directo del módulo
    fecha_consulta = date(2024, 5, 13)  # Usar una fecha conocida que tiene datos
    resultado = get_exchange_rate(fecha_consulta)
    
    if resultado:
        print(f"Fecha: {resultado['fecha']}")
        print(f"Compra: {resultado['compra']:.3f}")
        print(f"Venta: {resultado['venta']:.3f}")
    else:
        print("No se pudo obtener el tipo de cambio para la fecha especificada")
