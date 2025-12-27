import sqlite3
import arcpy
from datetime import datetime

# Ścieżki
dbf_path = r"G:\Praca_Inż\Dane\baza_danych.gdb\dane_zolnierze_popr"
sqlite_path = r"G:\Praca_Inż\Dane\vertabelo\baza_danych -nowa.sqlite"

# Połączenie z bazą SQLite
conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = OFF")

# Funkcja do konwersji dat
def parse_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, str):
        try:
            if value.startswith("00.00."):
                rok = value[-4:]
                return f"{rok}-01-01"
            return datetime.strptime(value, "%d.%m.%Y").date().isoformat()
        except:
            return None
    return None

# Rozbijanie adresu cmentarza
def extract_name_and_street(adres):
    if not adres:
        return "", None
    if "ul." in adres:
        parts = adres.split("ul.", 1)
        return parts[0].strip(" ,"), parts[1].strip()
    return adres.strip(), None

# Pola z DBF
fields = [
    "nazwisko", "imie1", "imie2", "data_urodzenia", "stopien",
    "pulk", "Kolumna1", "data_zgonu", "miejsce_zgonu", "id_zgon_prng",
    "nr_kwatery", "adres_cmentarza", "Gmina", "Powiat",
    "Wojewodztwo", "Powiat1939", "Wojewodztwo1939"
]

data_to_insert = []
weryfikacja_cache = {}  # zapamiętuje już utworzone wpisy "DO WERYFIKACJI – ..."

with arcpy.da.SearchCursor(dbf_path, fields) as cursor_dbf:
    for row in cursor_dbf:
        nazwisko, imie1, imie2, data_urodzenia, stopien, pulk_txt, kolumna1_txt, data_zgonu_raw, \
        miejsce_zgonu_txt, id_zgon_prng, nr_kwatery, adres_cmentarza, \
        gmina, powiat, woj, powiat_1939, woj_1939 = row

        data_urodzenia = parse_date(data_urodzenia)
        data_zgonu = parse_date(data_zgonu_raw)

        miejsce_zgonu_id = None
        if miejsce_zgonu_txt:
            cursor.execute("SELECT ID FROM miejsce_zgonu WHERE Miejscowosc = ?", (miejsce_zgonu_txt,))
            result = cursor.fetchone()
            if result:
                miejsce_zgonu_id = result[0]

        miejscowosc_pochowku, ulica = extract_name_and_street(adres_cmentarza)
        miejsce_pochowku_id = None
        if miejscowosc_pochowku:
            if ulica:
                cursor.execute("""
                    SELECT ID FROM miejsce_pochowku
                    WHERE Miejscowosc = ? AND Ulica = ?
                """, (miejscowosc_pochowku, ulica))
            else:
                cursor.execute("""
                    SELECT ID FROM miejsce_pochowku
                    WHERE Miejscowosc = ? AND Ulica IS NULL
                """, (miejscowosc_pochowku,))
            result = cursor.fetchone()
            if result:
                miejsce_pochowku_id = result[0]

        pulk_id = None
        if kolumna1_txt:
            kolumna1_clean = kolumna1_txt.strip()
            # Spróbuj znaleźć identyczną nazwę
            cursor.execute("""
                SELECT ID FROM jednostka_wojskowa
                WHERE TRIM(LOWER(Nazwa)) = TRIM(LOWER(?))
            """, (kolumna1_clean,))
            result = cursor.fetchone()
            if result:
                pulk_id = result[0]
            else:
                # Jeśli zawiera "pułk", twórz relację DO WERYFIKACJI – ...
                if "pułk" in kolumna1_clean.lower():
                    nowa_nazwa = f"DO WERYFIKACJI – {kolumna1_clean}"
                    if nowa_nazwa in weryfikacja_cache:
                        pulk_id = weryfikacja_cache[nowa_nazwa]
                    else:
                        # Sprawdź, czy istnieje już w bazie
                        cursor.execute("""
                            SELECT ID FROM jednostka_wojskowa
                            WHERE TRIM(LOWER(Nazwa)) = TRIM(LOWER(?))
                        """, (nowa_nazwa,))
                        result = cursor.fetchone()
                        if result:
                            pulk_id = result[0]
                        else:
                            cursor.execute("INSERT INTO jednostka_wojskowa (Nazwa) VALUES (?)", (nowa_nazwa,))
                            pulk_id = cursor.lastrowid
                        weryfikacja_cache[nowa_nazwa] = pulk_id
                else:
                    # Przypisanie "INNE"
                    if "INNE" in weryfikacja_cache:
                        pulk_id = weryfikacja_cache["INNE"]
                    else:
                        cursor.execute("""
                            SELECT ID FROM jednostka_wojskowa
                            WHERE TRIM(LOWER(Nazwa)) = 'inne'
                        """)
                        result = cursor.fetchone()
                        if result:
                            pulk_id = result[0]
                            weryfikacja_cache["INNE"] = pulk_id

        przyn_zrodlowe = pulk_txt.strip() if pulk_txt else None

        data_to_insert.append((
            nazwisko, imie1, imie2, data_urodzenia, None, stopien,
            przyn_zrodlowe, pulk_id, nr_kwatery, None, data_zgonu,
            miejsce_zgonu_id, id_zgon_prng, miejsce_pochowku_id,
            gmina, powiat, woj, powiat_1939, woj_1939
        ))

cursor.executemany("""
    INSERT INTO osoba (
        Nazwisko, Imie1, Imie2, Data_urodzenia, Zawod, Stopien_wojskowy,
        Przynaleznosc_dane_zrodlowe, Pulk_ID, Numer_kwatery, Bitwa, Data_zgonu,
        Miejsce_zgonu_ID, ID_zgonu_PRNG, Miejsce_pochowku_ID,
        Gmina, Powiat, Wojewodztwo, Powiat_1939, Wojewodztwo_1939
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", data_to_insert)

conn.commit()
print(f"\nWstawiono {len(data_to_insert)} osób.")
conn.close()
