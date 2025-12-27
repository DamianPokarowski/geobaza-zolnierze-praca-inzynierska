import sqlite3
import arcpy

# Ścieżki do baz danych
gdb_path = r"G:\Praca_Inż\Dane\baza_danych.gdb"
sqlite_path = r"G:\Praca_Inż\Dane\vertabelo\baza_danych -nowa.sqlite"

table_gdb = "cmentarze"
table_sqlite = "miejsce_pochowku"

# Połączenie z bazą SQLite
conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

# Pobieranie danych z GDB (w tym X i Y z tabeli atrybutów)
fields_gdb = ["miasto", "ulica", "gmina", "powiat", "wojewodztw", "r_mogily", "l_pochowan", "administra", "X", "Y"]
data = []

with arcpy.da.SearchCursor(f"{gdb_path}/{table_gdb}", fields_gdb) as cursor_gdb:
    for row in cursor_gdb:
        miasto, ulica, gmina, powiat, wojewodztw, r_mogily, l_pochowan, administra, x, y = row
        data.append((miasto, ulica, gmina, powiat, wojewodztw, r_mogily, l_pochowan, administra, x, y))

# Wstawianie danych do SQLite
cursor.executemany(f"""
    INSERT INTO {table_sqlite} (Miejscowosc, Ulica, Gmina, Powiat, Wojewodztwo, Rodzaj_mogily, Liczba_pochowan, Administracja, X, Y)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", data)

conn.commit()
conn.close()
print("Dane zostały pomyślnie skopiowane.")
