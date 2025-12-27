import sqlite3
import arcpy

# Ścieżki do baz danych
gdb_path = r"G:\Praca_Inż\Dane\baza_danych.gdb"
table_gdb = "pulki_dyslokacja"

sqlite_path = r"G:\Praca_Inż\Dane\vertabelo\baza_danych -nowa.sqlite"
table_sqlite = "jednostka_wojskowa"

# Połączenie z SQLite
conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

# Pola do pobrania z GDB (bez OBJECTID!)
fields_gdb = ["unitName", "place", "division", "militaryType", "place_1", "X", "Y"]
new_data = []

# Odczyt danych z GDB
with arcpy.da.SearchCursor(f"{gdb_path}/{table_gdb}", fields_gdb) as cursor_gdb:
    for row in cursor_gdb:
        nazwa, miejscowosc, dywizja, rodzaj, miejsca_stacj, x, y = row

        new_data.append((
            nazwa,
            miejscowosc,
            dywizja,
            rodzaj,
            miejsca_stacj,
            float(x) if x not in [None, ""] else None,
            float(y) if y not in [None, ""] else None
        ))
        print(f"+ DODANO: {nazwa}, {miejscowosc} ({x or '-'}, {y or '-'})")

# Wstawienie danych do SQLite (z X, Y)
if new_data:
    cursor.executemany(f"""
        INSERT INTO {table_sqlite}
        (Nazwa, Miejscowosc, Dywizja, Rodzaj, Miejsca_stacjonowania, X, Y)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, new_data)
    conn.commit()
    print(f"\nWstawiono {len(new_data)} rekordów.")
else:
    print("\nBrak danych do wstawienia.")

conn.close()
