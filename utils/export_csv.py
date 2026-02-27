import csv
import time
from utils.text_cleaner import clean_text

NULL_TOKEN = r"\N"

def export_query_to_csv(conn, query, columns, csv_path, delimiter='|'):
    # ⏱ inicio medición
    start_time = time.perf_counter()

    total_rows = 0

    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(
                f,
                delimiter=delimiter,
                quoting=csv.QUOTE_NONE,
                lineterminator="\n"
            )

            for row in rows:
                row_dict = dict(zip(columns, row))

                for col, value in row_dict.items():

                    # 1️⃣ NULL real
                    if value is None:
                        row_dict[col] = NULL_TOKEN
                        continue

                    # 2️⃣ Normalizar todo a string
                    value_str = str(value).strip()

                    # 3️⃣ Vacío → NULL
                    if value_str == "":
                        row_dict[col] = NULL_TOKEN
                    else:
                        # proteger el token NULL para StarRocks
                        if value_str == r"\N":
                            row_dict[col] = NULL_TOKEN
                        else:
                            row_dict[col] = clean_text(value_str)

                writer.writerow([row_dict[col] for col in columns])
                total_rows += 1

    # ⏱ fin medición
    end_time = time.perf_counter()
    elapsed = end_time - start_time

    # 🔹 métricas de tiempo
    total_ms = int(elapsed * 1000)

    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)
    milliseconds = int((elapsed - int(elapsed)) * 1000)

    rows_per_second = total_rows / elapsed if elapsed > 0 else 0

    print("\n========= EXPORT CSV =========")
    print(f"Archivo generado      : {csv_path}")
    print(f"Filas exportadas      : {total_rows}")
    print(f"Tiempo exacto         : {elapsed:.3f} segundos")
    print(f"Tiempo formateado     : {hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}")
    print(f"Total milisegundos    : {total_ms} ms")
    print(f"Rendimiento           : {rows_per_second:,.0f} filas/segundo")
    print("=========================================\n")