# sprawdza czy juz istnieje informacja o wsp w tabeli, jezeli tak to pomija ten wiersz
# jezeli skrypt nie przypisuje wsp nalezy chwile odczekac poniewaz api GeoNames ma limit zapytań
# skrypt warto odpalić kilka razy z pwodu duzej liczby rekordow..
import sqlite3
import requests

# GeoNames konfiguracja
BASE_URL = "http://api.geonames.org/searchJSON"
USERNAME = "damianpokarowski"

# Ścieżka do bazy
db_path = r"G:\Praca_Inż\Dane\vertabelo\baza_danych -nowa.sqlite"

# Czyszczenie nazw miejscowości
def clean_city_name(city_name):
    if not isinstance(city_name, str):
        return ""
    txt = city_name.strip().split('(')[0].split('-')[0]
    prefixes = ["rejon", "rej.", "okolice", "pod", "koło", "blisko"]
    for p in prefixes:
        if txt.lower().startswith(p):
            txt = txt[len(p):].strip()
    return txt

# Zapytanie do GeoNames
def query_geonames(query, username):
    try:
        response = requests.get(BASE_URL, params={
            'q': query,
            'maxRows': 1,
            'username': username,
            'country': 'PL',
        }, timeout=10)
        data = response.json()
        if 'geonames' in data and data['geonames']:
            return data['geonames'][0]['lat'], data['geonames'][0]['lng']
    except Exception as e:
        print(f"Błąd zapytania: {e}")
    return None, None

# Główna funkcja
def update_coordinates():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # pobieramy wszystkie rekordy
    cursor.execute("SELECT ID, Miejscowosc, X, Y FROM miejsce_zgonu ORDER BY ID")
    rows = cursor.fetchall()

    for row in rows:
        id_, miasto, x, y = row

        # jeśli już są współrzędne — pomiń
        if x is not None and y is not None:
            print(f"= ID={id_}: {miasto} → współrzędne już istnieją, pominięto")
            continue

        miasto_clean = clean_city_name(miasto)

        if not miasto_clean:
            print(f"- ID={id_}: NIE ZNALEZIONO → brak nazwy")
            continue

        query = f"{miasto_clean}, Polska"
        lat, lon = query_geonames(query, USERNAME)

        if lat and lon:
            cursor.execute("UPDATE miejsce_zgonu SET X = ?, Y = ? WHERE ID = ?", (lat, lon, id_))
            print(f"+ ID={id_}: {miasto} → X={lat}, Y={lon}")
        else:
            print(f"- ID={id_}: NIE ZNALEZIONO → {miasto}")

    conn.commit()
    conn.close()
    print("\nAktualizacja zakończona.")

# Uruchomienie
update_coordinates()
