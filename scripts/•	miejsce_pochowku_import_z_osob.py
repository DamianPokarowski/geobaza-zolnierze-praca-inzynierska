import sqlite3
import arcpy

# Ścieżki
gdb_path = r"G:\Praca_Inż\Dane\baza_danych.gdb"
sqlite_path = r"G:\Praca_Inż\Dane\vertabelo\baza_danych -nowa.sqlite"

table_gdb = "dane_zolnierze_popr"
table_sqlite = "Miejsce_pochowku"

def extract_name_and_street(full_name):
    if not isinstance(full_name, str):
        return "", None
    full_name = full_name.strip()
    if "ul." in full_name:
        parts = full_name.split("ul.", 1)
        nazwa = parts[0].strip(" ,")
        ulica = parts[1].strip()
        return nazwa, ulica
    return full_name.strip(), None

def record_exists(cursor, name, ulica, x, y):
    # Sprawdzenie po miejscowości i ulicy
    if ulica:
        cursor.execute(f"""
            SELECT 1 FROM {table_sqlite}
            WHERE Miejscowosc = ? AND Ulica = ?
        """, (name, ulica))
    else:
        cursor.execute(f"""
            SELECT 1 FROM {table_sqlite}
            WHERE Miejscowosc = ? AND Ulica IS NULL
        """, (name,))
    if cursor.fetchone():
        return True

    # Sprawdzenie po współrzędnych (jeśli istnieją)
    if x is not None and y is not None:
        try:
            x = float(x)
            y = float(y)
        except:
            return False

        cursor.execute(f"""
            SELECT 1 FROM {table_sqlite}
            WHERE X = ? AND Y = ?
        """, (x, y))
        if cursor.fetchone():
            return True

    return False

def already_in_batch(data, name, ulica, x, y, epsilon=0.0000001):
    for row in data:
        n, u, *_rest, dx, dy = row
        # Sprawdzenie po miejscowości i ulicy
        if n == name and u == ulica:
            return True
        # Sprawdzenie po współrzędnych
        if x is not None and y is not None and dx is not None and dy is not None:
            try:
                if abs(float(x) - dx) < epsilon and abs(float(y) - dy) < epsilon:
                    return True
            except:
                continue
    return False

# Import danych

conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

fields_gdb = ["adres_cmentarza", "X_wgs", "Y_wgs"]
data = []

with arcpy.da.SearchCursor(f"{gdb_path}/{table_gdb}", fields_gdb) as cursor_gdb:
    for row in cursor_gdb:
        miejsce, x, y = row
        nazwa, ulica = extract_name_and_street(miejsce)

        if not nazwa:
            print(f"! POMINIĘTO (brak nazwy): {miejsce}")
            continue

        if already_in_batch(data, nazwa, ulica, x, y):
            print(f"! POMINIĘTO (duplikat w batchu): {nazwa}, {ulica or '-'} ({x or '-'}, {y or '-'})")
            continue

        if record_exists(cursor, nazwa, ulica, x, y):
            print(f"! POMINIĘTO (już w bazie): {nazwa}, {ulica or '-'} ({x or '-'}, {y or '-'})")
            continue

        data.append((
            nazwa,
            ulica,
            None, None, None, None, None, None,
            float(x) if x not in [None, ""] else None,
            float(y) if y not in [None, ""] else None
        ))
        print(f"+ DODANO: {nazwa}, {ulica or '-'} ({x or '-'}, {y or '-'})")

# Wstawienie do bazy danych

if data:
    cursor.executemany(f"""
        INSERT INTO {table_sqlite}
        (Miejscowosc, Ulica, Gmina, Powiat, Wojewodztwo,
         Rodzaj_mogily, Liczba_pochowan, Administracja, X, Y)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)
    conn.commit()

print(f"\nZakończono. Dodano {len(data)} nowych rekordów.")
conn.close()
