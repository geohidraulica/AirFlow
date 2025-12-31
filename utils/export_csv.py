import csv
from utils.clear_text import clean_text

def export_query_to_csv(conn, query, columns, csv_path, delimiter='|'):
    """
    Ejecuta una consulta SQL y exporta el resultado a un CSV.
    
    :param conn: Conexión a la base de datos (pyodbc connection)
    :param query: Consulta SQL a ejecutar
    :param columns: Lista de columnas en el orden deseado
    :param csv_path: Ruta del CSV de salida
    :param delimiter: Separador de columnas (por defecto '|')
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()  # Consumimos todos los resultados de inmediato

        # Abrimos el archivo CSV
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(
                f,
                delimiter=delimiter,
                quoting=csv.QUOTE_NONE,
                escapechar='\\'
            )
            
            for row in rows:
                row_dict = dict(zip(columns, row))
                # Limpiar texto si es necesario
                for col, value in row_dict.items():
                    if isinstance(value, str):
                        row_dict[col] = clean_text(value)
                writer.writerow([row_dict[col] for col in columns])

    print(f"CSV generado en: {csv_path}")
