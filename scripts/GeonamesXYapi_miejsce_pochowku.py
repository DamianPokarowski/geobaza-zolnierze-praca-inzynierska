import sqlite3
import requests

# GeoNames API
BASE_URL = "http://api.geonames.org/searchJSON"
USERNAME = "damianpokarowski"

def clean_text(text):
    if not isinstance(text, str):
        return ""
    return text.strip()


def clean_city_name(city_name):
    return clean_text(city_name.split('(')[0].split('-')[0])


def clean_street_name(street):
    if not isinstance(street, str) or not street.strip():
        return None
    street = street.strip()
    if street.lower().startswith("ul. "):
        return street[4:].strip()
    elif street.lower().startswith("ul."):
        return street[3:].strip()
    return street


def clean_voivodeship(name):
    if not isinstance(name, str):
        return ""
    name = clean_text(name.lower())
    name = name.replace(" - ", "-")
    name = name.replace("kujawsko - pomorskie", "kujawsko-pomorskie")
    return name.title()


def get_country_code_and_clean_name(name):
    if not isinstance(name, str):
        return "PL", ""

    name = name.strip()

    # Znane prefiksy
    prefixes = {
        "Ukraina": "UA",
        "Białoruś": "BY",
        "Litwa": "LT",
        "Polska": "PL",
    }

    for prefix, code in prefixes.items():
        if name.startswith(prefix + ","):
            cleaned = name.replace(prefix + ",", "", 1).strip()
            return code, cleaned

    # Domyślnie Polska
    return "PL", name.strip()


def query_geonames(query, username, country_code="PL"):
    try:
        response = requests.get(BASE_URL, params={
            'q': query,
            'maxRows': 1,
            'username': username,
            'country': country_code,
        }, timeout=10)
        data = response.json()
        if 'geonames' in data and data['geonames']:
            return data['geonames'][0]['lat'], data['geonames'][0]['lng']
    except Exception as e:
        print(f"Błąd zapytania: {e}")
    return None, None


def get_coordinates(city, street, gmina, powiat, wojewodztwo):
    # Rozpoznaje kraj i oczyszcza nazwe
    country_code, city = get_country_code_and_clean_name(city)

    city = clean_city_name(city)
    gmina = clean_text(gmina)
    powiat = clean_text(powiat)
    wojewodztwo = clean_voivodeship(wojewodztwo)
    street = clean_street_name(street)

    def query(q):
        return query_geonames(q, USERNAME, country_code)

    if street:
        query1 = f"{street}, {city}, {gmina}, {wojewodztwo}"
        lat, lon = query(query1)
        if lat and lon:
            return lat, lon, query1

        query1b = f"{street}, {city}, {wojewodztwo}"
        lat, lon = query(query1b)
        if lat and lon:
            return lat, lon, query1b

    if gmina or powiat or wojewodztwo:
        query2 = f"{city}, {gmina}, {wojewodztwo}".strip(", ")
        lat, lon = query(query2)
        if lat and lon:
            return lat, lon, query2

        query3 = f"{city}, {powiat}, {wojewodztwo}".strip(", ")
        lat, lon = query(query3)
        if lat and lon:
            return lat, lon, query3

    #próba tylko po nazwie miejscowości
    query4 = city
    lat, lon = query(query4)
    if lat and lon:
        return lat, lon, query4

    return None, None, None

# AKTUALIZACJA BAZY

def update_database_with_coordinates(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT ID, Miejscowosc, Ulica, Gmina, Powiat, Wojewodztwo, X, Y
        FROM Miejsce_pochowku
    """)
    rows = cursor.fetchall()

    nieznalezione_rows = []

    for row in rows:
        id_, miasto, ulica, gmina, powiat, wojew, x, y = row

        # Jeśli współrzędne już istnieją - pomijam
        if x is not None and y is not None:
            print(f"- POMINIĘTO: ID={id_} → współrzędne już istnieją")
            continue

        lat, lon, used_query = get_coordinates(miasto, ulica, gmina, powiat, wojew)

        if lat and lon:
            cursor.execute("UPDATE Miejsce_pochowku SET X = ?, Y = ? WHERE ID = ?", (lat, lon, id_))
            print(f"+ ID={id_}: {miasto}, {ulica or '-'} → X={lat}, Y={lon} (zapytanie: {used_query})")
        else:
            print(f"- NIE ZNALEZIONO: ID={id_}: {miasto}, {ulica or '-'}, {gmina}, {powiat}, {wojew or '-'}")
            nieznalezione_rows.append(row)

    conn.commit()
    conn.close()

    print("\nAktualizacja zakończona.")

    if nieznalezione_rows:
        print("\nWiersze bez współrzędnych:\n")
        for row in nieznalezione_rows:
            print(" | ".join(str(val) if val is not None else "" for val in row))
    else:
        print("\nWszystkie współrzędne zostały uzupełnione.")


#Uruchamianie
update_database_with_coordinates(r"G:\Praca_Inż\Dane\vertabelo\baza_danych -nowa.sqlite")
