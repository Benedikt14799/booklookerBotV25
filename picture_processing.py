import aiohttp
from bs4 import BeautifulSoup


class PictureProcessing:
    """
    Klasse: PictureProcessing
    --------------------------
    Verwaltet die Extraktion und Speicherung von Bildern aus einer Webseite
    und prüft auf Verfügbarkeit eines DNB-Titelbilds.
    """

    @staticmethod
    async def get_pictures_with_dnb(soup: BeautifulSoup, num: int, db_pool, isbn: str):
        """
        Funktion: get_pictures_with_dnb
        --------------------------------
        Extrahiert Bilder aus einer Webseite und prüft, ob ein DNB-Titelbild
        verfügbar ist. Speichert das DNB-Bild in der Datenbank, auch wenn
        keine Booklooker-Bilder verfügbar sind.

        Parameter:
        - soup (BeautifulSoup): Das BeautifulSoup-Objekt der Seite.
        - num (int): Die ID des Artikels in der Datenbank.
        - db_pool: Verbindung zur Datenbank.
        - isbn (str): Die ISBN des Artikels.

        Rückgabe:
        - str: Die verkettete Liste der Bild-Links oder ein leerer String bei Fehlern.
        """
        try:
            # Basis-URL für DNB-Titelbild
            dnb_cover_url = f"https://portal.dnb.de/opac/mvb/cover?isbn={isbn}"
            picture_links = []

            # Prüfen, ob DNB-Bild verfügbar ist
            async with aiohttp.ClientSession() as session:
                async with session.get(dnb_cover_url) as response:
                    if response.status == 200:
                        # Titelbild ist verfügbar
                        picture_links.append(dnb_cover_url)
                        print(f"DNB-Titelbild für Artikel {num} hinzugefügt: {dnb_cover_url}")
                    else:
                        print(f"DNB-Titelbild für ISBN {isbn} nicht verfügbar (Status: {response.status}).")

            # Alle Bilder von Booklooker extrahieren (falls vorhanden)
            preview_images = soup.find_all(class_="previewImage")
            if not preview_images:
                print(f"Keine zusätzlichen Bilder für Artikel {num} gefunden.")
            else:
                for idx, pic in enumerate(preview_images):
                    if idx >= 24:  # Maximal 24 Bilder
                        break

                    # Hochauflösende Bild-URL erstellen
                    if 'src' in pic.attrs:
                        src = pic['src'].replace("/t/", "/x/")
                        picture_links.append(src)
                        print(f"Bild {idx + 1} für Artikel {num} hinzugefügt: {src}")
                    else:
                        print(f"Warnung: Kein 'src'-Attribut für Bild gefunden bei Artikel {num}, Bild {idx + 1}.")

            # Verkettete Links erstellen, getrennt durch "|"
            result = "|".join(picture_links) if picture_links else dnb_cover_url

            # Ergebnis in die Datenbank speichern
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE library SET photo = $1 WHERE id = $2", result, num)

            # Log und Rückgabe
            print(f"Extrahierte Bilder für Artikel {num}: {result}")
            return result

        except Exception as e:
            print(f"Ein unerwarteter Fehler beim Verarbeiten der Bilder für Artikel {num}: {e}")
            return ''
