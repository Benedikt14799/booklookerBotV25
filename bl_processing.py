import re  # Modul für reguläre Ausdrücke
from bs4 import BeautifulSoup


class PropertyToDatabase:
    """
    Klasse: PropertyToDatabase
    ---------------------------
    Verarbeitet PropertyItems aus einer Webseite und speichert sie
    in die passende Spalte der `library`-Datenbank.
    """

    # Aktualisiertes Mapping der Property-Namen zu den entsprechenden Spalten in der Tabelle
    PROPERTY_MAPPING = {
        "Titel:": "Buchtitel",
        "Titel:": "Title",
        "Zustand:": "Condition_ID",
        "Verlag:": "Verlag",
        "Format:": "CFormat",
        "Auflage:": "Ausgabe",
        "Ort:": "Location",
        "Sprache:": "Sprache",
        "Stichwörter:": "Thematik",
        "Autor/in:": "Autor",
        "vom Autor signiert:": "Signiert_von",
        "Einband:": "Produktart",
        "Erschienen:": "Erscheinungsjahr"
    }

    @staticmethod
    def truncate_title(title: str, max_length: int = 80) -> str:
        """
        Kürzt den Titel so, dass Wörter nicht zerschnitten werden.
        Wenn der Titel länger als `max_length` ist, wird er auf das letzte vollständige Wort
        innerhalb der maximalen Länge gekürzt.

        Parameter:
        - title (str): Der Originaltitel.
        - max_length (int): Maximale Länge des Titels (Standard: 80 Zeichen).

        Rückgabe:
        - str: Der gekürzte Titel.
        """
        if len(title) <= max_length:
            return title

        # Kürze den Titel auf die maximale Länge
        truncated = title[:max_length]

        # Schneide bis zum letzten Leerzeichen, um ein vollständiges Wort zu erhalten
        if " " in truncated:
            return truncated.rsplit(" ", 1)[0]
        else:
            return truncated

    @staticmethod
    async def process_and_save(soup: BeautifulSoup, num: int, db_pool):
        """
        Führt die Extraktion und Speicherung von PropertyItems in einem Schritt aus.
        """
        # Extrahiere die Properties aus dem HTML
        properties = PropertyExtractor.extract_property_items(soup)
        if properties:
            # Übergib die extrahierten Properties an die Datenbankfunktion
            await PropertyToDatabase.insert_properties_to_db(properties, num, db_pool)

    @staticmethod
    async def insert_properties_to_db(properties: dict, num: int, db_pool):
        """
        Funktion: insert_properties_to_db
        ---------------------------------
        Speichert die extrahierten PropertyItems in die passenden Spalten der `library`-Tabelle.
        Kürzt den Titel nur für die Spalte `Title`. Der `Buchtitel` bleibt ungekürzt.

        Parameter:
        - properties (dict): Ein Wörterbuch mit den extrahierten PropertyItems.
        - num (int): Die ID des Artikels in der Datenbank.
        - db_pool: Verbindung zur Datenbank.

        Rückgabe:
        - bool: True, wenn erfolgreich, sonst False.
        """
        try:
            # Zu speichernde Daten vorbereiten
            db_columns = []
            db_values = []

            # Iteriere durch alle Mappings und prüfe, ob die Property vorhanden ist
            for property_name, db_column in PropertyToDatabase.PROPERTY_MAPPING.items():
                # Falls die Property im `properties`-Dictionary fehlt, nutze "Keine Angabe"
                property_value = properties.get(property_name, "Keine Angabe")

                # Titel für die Spalte `Title` kürzen, `Buchtitel` bleibt ungekürzt
                if property_name == "Titel:":
                    db_columns.append("Title")
                    db_values.append(PropertyToDatabase.truncate_title(property_value))  # Kürzung für Title
                    db_columns.append("Buchtitel")
                    db_values.append(property_value)  # Originalwert für Buchtitel
                # Zustand verarbeiten
                elif property_name == "Zustand:":
                    property_value = PropertyToDatabase.map_condition(property_value)
                    db_columns.append(db_column)
                    db_values.append(property_value)
                else:
                    db_columns.append(db_column)
                    db_values.append(property_value)

            if not db_columns:
                print(f"Keine gültigen Spalten für Artikel {num} gefunden.")
                return False

            # SQL-Query dynamisch erstellen
            sql_query = f"""
                UPDATE library
                SET {', '.join(f"{col} = ${i + 1}" for i, col in enumerate(db_columns))}
                WHERE id = ${len(db_columns) + 1}
            """

            # Query ausführen
            async with db_pool.acquire() as conn:
                await conn.execute(sql_query, *db_values, num)

            print(f"PropertyItems erfolgreich für Artikel {num} gespeichert.")
            return True

        except Exception as e:
            print(f"Fehler beim Speichern der PropertyItems für Artikel {num}: {e}")
            return False

    @staticmethod
    def map_condition(condition: str) -> str:
        """
        Funktion: map_condition
        ------------------------
        Ordnet eine Zustandsbeschreibung der entsprechenden ID zu.

        Parameter:
        - condition (str): Die Zustandsbeschreibung (z. B. "wie neu").

        Rückgabe:
        - str: Die zugehörige ID als String (z. B. "2750") oder "Keine Angabe", falls nicht zuordenbar.
        """
        if not condition:
            return "Keine Angabe"

        # Normalisiere die Eingabe
        normalized_condition = condition.strip().lower()

        # Mapping-Tabelle mit normalisierten Werten
        normalized_mapping = {
            "neu, aktuelle ausgabe": "1000",
            "neuware": "1000",
            "wie neu": "2750",
            "leichte gebrauchsspuren": "4000",
            "deutliche gebrauchsspuren": "5000",
            "stark abgenutzt": "6000"
        }

        return normalized_mapping.get(normalized_condition, "Keine Angabe")




class PropertyExtractor:
    """
    Klasse: PropertyExtractor
    --------------------------
    Verwaltet die Extraktion von PropertyItems aus einer Webseite.
    """

    @staticmethod
    def extract_property_items(soup: BeautifulSoup) -> dict:
        """
        Funktion: extract_property_items
        --------------------------------
        Extrahiert alle PropertyItems aus einer Webseite und gibt sie als
        Schlüssel-Wert-Paare in einem Wörterbuch zurück.

        Parameter:
        - soup (BeautifulSoup): Das BeautifulSoup-Objekt der Seite.

        Rückgabe:
        - dict: Ein Wörterbuch mit den extrahierten PropertyItems
                (Eigenschaftsname → Wert).
        """
        try:
            properties = {}  # Initialisiere ein leeres Wörterbuch für die Eigenschaften

            # Suche nach allen div-Tags mit Klassen, die mit 'propertyItem_' beginnen
            property_items = soup.find_all(class_=re.compile(r"propertyItem_\d+"))

            for item in property_items:
                try:
                    # Suche nach dem Element mit dem Eigenschaftsnamen
                    property_name_elem = item.find(class_="propertyName")
                    # Suche nach dem Element mit dem Eigenschaftswert
                    property_value_elem = item.find(class_="propertyValue")

                    # Wenn entweder Name oder Wert fehlt, überspringen
                    if not property_name_elem or not property_value_elem:
                        continue

                    # Extrahiere und bereinige den Text für Name und Wert
                    property_name = property_name_elem.text.strip()
                    property_value = property_value_elem.get_text(separator=" ").strip()

                    # Speichere die Eigenschaft im Wörterbuch
                    properties[property_name] = property_value
                except Exception as e:
                    # Fehlerprotokollierung für einzelne PropertyItems
                    print(f"Fehler beim Extrahieren eines PropertyItems: {e}")

            # Rückgabe des Wörterbuchs mit allen extrahierten Eigenschaften
            return properties
        except Exception as e:
            # Allgemeine Fehlerprotokollierung für die gesamte Extraktion
            print(f"Fehler beim Extrahieren der PropertyItems: {e}")
            return {}
