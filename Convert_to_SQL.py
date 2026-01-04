import csv

with open("combined_output_Main.csv", newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    headers = next(reader)

    with open("main_output.sql", "w", encoding="utf-8") as out:
        # Create table
        out.write(
            "CREATE TABLE my_table (" +
            ", ".join(h + " TEXT" for h in headers) +
            ");\n\n"
        )

        # Insert statements
        for row in reader:
            values = ", ".join("'" + v.replace("'", "''") + "'" for v in row)
            out.write(f"INSERT INTO my_table VALUES ({values});\n")
