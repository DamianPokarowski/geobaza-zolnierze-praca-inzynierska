# nie wczytuje duplikatów czyli kazda miejsowosc wystepuje tylko raz!

import sqlite3
import arcpy

# Ścieżki do baz danych
gdb_path = r"G:\Praca_Inż\Dane\baza_danych.gdb"
sqlite_path = r"G:\Praca_Inż\Dane\vertabelo\baza_danych -nowa.sqlite"

table_gdb = "dane_zolnierze_popr"
table_sqlite = "miejsce_zgonu"

# Funkcja normalizująca nazwy (spacja, wielkość liter)
def normalize_name(name):
    if not name:
        return ""
    return name.strip().lower()

# Połączenie z SQLite
conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

# Wczytanie już istniejących miejscowości
cursor.execute(f"SELECT DISTINCT Miejscowosc FROM {table_sqlite}")
existing_cities = set(normalize_name(row[0]) for row in cursor.fetchall())

# Pobranie danych z GDB
fields_gdb = ["miejsce_zgonu"]
new_data = []

with arcpy.da.SearchCursor(f"{gdb_path}/{table_gdb}", fields_gdb) as cursor_gdb:
    for row in cursor_gdb:
        miejsc_zgonu = row[0]
        if miejsc_zgonu:
            norm_name = normalize_name(miejsc_zgonu)
            if norm_name not in existing_cities:
                new_data.append((miejsc_zgonu.strip(),))
                existing_cities.add(norm_name)

# Wstawienie unikalnych
if new_data:
    cursor.executemany(f"""
        INSERT INTO {table_sqlite} (Miejscowosc)
        VALUES (?)
    """, new_data)
    print(f"Wstawiono {len(new_data)} nowych miejscowości.")
else:
    print("Brak nowych miejscowości do wstawienia.")

conn.commit()
conn.close()
