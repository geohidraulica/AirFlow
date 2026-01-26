import csv
from utils.text_cleaner import clean_text

NULL_TOKEN = r"\N"

def export_query_to_csv(conn, query, columns, csv_path, delimiter='|'):
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(
                f,
                delimiter=delimiter,
                quoting=csv.QUOTE_NONE
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
                        row_dict[col] = clean_text(value_str)

                writer.writerow([row_dict[col] for col in columns])

    print(f"CSV generado en: {csv_path}")
