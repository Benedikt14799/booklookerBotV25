import asyncio
import logging
import re

import aiohttp
from bs4 import BeautifulSoup

import database
import isbn_processing
import price_processing
import bl_processing
from picture_processing import PictureProcessing
from price_processing import PriceProcessing

logger = logging.getLogger(__name__)
number_pattern = re.compile(r"\d+")

"""
Funktion: insert_links_into_sitetoscrape
----------------------------------------
- Verarbeitet und speichert neue Links in der Tabelle `sitetoscrape`.
- Prüft, ob die Links bereits existieren.
- Scrapt Daten von neuen Links (Anzahl Seiten, Anzahl Bücher).
- Speichert die Ergebnisse in der Datenbank.
"""
async def insert_links_into_sitetoscrape(links_to_scrape, db_pool):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT link FROM sitestoscrape")
        existing_links = set(row['link'] for row in rows)

    new_links = [link for link in links_to_scrape if link not in existing_links]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_process(session, link) for link in new_links]
        results = await asyncio.gather(*tasks)

    insert_data = [result for result in results if result is not None]

    async with db_pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO sitestoscrape (link, anzahlSeiten, numbersOfBooks) VALUES ($1, $2, $3)",
            insert_data
        )


"""
Funktion: fetch_and_process
---------------------------
- Lädt und parst Inhalte einer einzelnen URL.
- Extrahiert die Anzahl Bücher und die maximale Seitenzahl.
- Gibt die Daten für die Speicherung in `sitetoscrape` zurück.
"""
async def fetch_and_process(session, link):
    try:
        # HTML-Inhalt abrufen
        html_content = await fetch_html(session, link)
        soup = BeautifulSoup(html_content, 'html.parser')

        # Anzahl der Bücher extrahieren
        result_count_div = soup.find('div', class_='resultlist_count')
        if result_count_div:
            result_count_text = result_count_div.text
            match = number_pattern.search(result_count_text)
            books_count = int(match.group()) if match else 0
        else:
            books_count = 0

        # Höchste Seitenzahl extrahieren
        page_numbers = [int(item.text) for item in soup.find_all(class_='PageNavNumItem') if item.text.isdigit()]
        highest_page_number = max(page_numbers) if page_numbers else 1

        return link, highest_page_number, books_count

    except Exception as e:
        # Fehler beim Abrufen oder Verarbeiten loggen
        logger.error(f"Fehler bei '{link}': {e}")
        return None


"""
Funktion: scrape_and_save_pages
-------------------------------
- Scrapt und speichert Inhalte von Links, die in der Tabelle `sitetoscrape` gespeichert sind.
- Baut URLs mit Seitennummern auf.
- Scrapt Inhalte von allen Seiten und speichert die Ergebnisse in der Tabelle `library`.
- Setzt Fremdschlüssel zwischen `sitetoscrape` und `library`.
"""
async def scrape_and_save_pages(db_pool):
    try:
        async with db_pool.acquire() as conn:
            results = await conn.fetch("SELECT id, link, anzahlSeiten FROM sitestoscrape;")
        tasks = []

        async with aiohttp.ClientSession() as session:
            for row in results:
                site_id, site_link, num_pages = row['id'], row['link'], row['anzahlseiten']
                for page_number in range(1, num_pages + 1):
                    link_final = f'{site_link}?setMediaType=0&page={page_number}'
                    tasks.append(fetch_and_parse(session, link_final, db_pool))

            await asyncio.gather(*tasks)

        await database.set_foreignkey(db_pool)
    except Exception as e:
        logger.error(f"Ein Fehler ist aufgetreten in scrape_and_save_pages: {e}")


"""
Funktion: fetch_and_parse
-------------------------
- Lädt und speichert einzelne Buch-Details von einer Seite.
- Extrahiert Buch-Links aus der Webseite und speichert sie in der Tabelle `library`.
"""
async def fetch_and_parse(session, url, db_pool):
    try:
        html_content = await fetch_html(session, url)
        souped = BeautifulSoup(html_content, "html.parser")
        results_desc = souped.find_all(class_="resultList_desc")
        insert_data = []

        for item in results_desc:
            if item.a:
                link_part_one = item.a.get('href')
                final_link = 'https://www.booklooker.de' + link_part_one
                insert_data.append((final_link,))
            else:
                logger.warning(f"Kein <a>-Tag gefunden für das Element auf Seite: {url}")

        async with db_pool.acquire() as conn:
            await conn.executemany("INSERT INTO library (LinkToBL) VALUES ($1)", insert_data)

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Seite {url}: {e}")

"""
Funktion: extract_properties
----------------------------
- Extrahiert Eigenschaften aus einem BeautifulSoup-Objekt.
- Durchsucht HTML-Elemente mit spezifischen Klassen und sammelt Eigenschaftsnamen und Werte.
- Gibt ein Wörterbuch mit den extrahierten Eigenschaften zurück.
"""
def extract_properties(soup):
    properties = {}
    property_items = soup.find_all(class_=re.compile(r"propertyItem_\d+"))

    for item in property_items:
        try:
            # Elemente für Name und Wert extrahieren
            property_name_elem = item.find(class_="propertyName")
            property_value_elem = item.find(class_="propertyValue")

            # Validierung: Elemente müssen vorhanden sein
            if not property_name_elem or not property_value_elem:
                logger.warning(f"Element hat fehlende Name- oder Wert-Felder: {item}")
                continue

            # Text bereinigen und speichern
            property_name = property_name_elem.text.strip()
            property_value = property_value_elem.text.strip()
            properties[property_name] = property_value
        except Exception as e:
            # Fehler loggen mit zusätzlichem Kontext
            logger.error(f"Fehler beim Extrahieren der Eigenschaft aus Element {item}: {e}")
    return properties


"""
Funktion: process_library_links_async
------------------------------------------------------------
- Ruft die ISBNs und Buch-Links (`LinkToBL`) aus der Tabelle `library` ab.
- Erstellt asynchrone Tasks zur Verarbeitung der Links mit `process_book`.
- Nutzt parallele Verarbeitung mit `asyncio.gather`, um die Effizienz zu maximieren.
"""
async def process_library_links_async(db_pool):
    try:
        # Abrufen der Buch-Links und IDs aus der Tabelle `library`
        async with db_pool.acquire() as conn:
            results = await conn.fetch("SELECT id, LinkToBL FROM library")

        async with aiohttp.ClientSession() as session:
            for row in results:
                num, link = row['id'], row['linktobl']

                # Verarbeite jeden Eintrag und erhalte die ISBN (falls vorhanden)
                has_isbn, isbn = await isbn_processing.process_entry(session, link, num, db_pool)

                if has_isbn:
                    logger.info(f"Artikel {num} hat eine gültige ISBN: {isbn}. Weitere Verarbeitung wird gestartet.")

                    # Versuche, den HTML-Inhalt abzurufen
                    html_content = await fetch_html(session, link)
                    if html_content:
                        soup = BeautifulSoup(html_content, "html.parser")

                        # Preisberechnung aufrufen und Bilder mit ISBN verarbeiten
                        await PriceProcessing.get_price(soup, num, db_pool)
                        await PictureProcessing.get_pictures_with_dnb(soup, num, db_pool, isbn)

                        # Eigenschaften in die Datenbank speichern
                        await bl_processing.PropertyToDatabase.process_and_save(soup, num, db_pool)

                        # await ExcelExporter.export_to_excel(db_pool, "output.xlsx")
                    else:
                        logger.error(f"HTML-Inhalt konnte für Artikel {num} nicht geladen werden. Überspringe Verarbeitung.")
                else:
                    logger.warning(f"Artikel {num} hat keine gültige ISBN. Wird nicht weiter verarbeitet.")
    except Exception as e:
        logger.error(f"Fehler in process_library_links_async: {e}")



"""
Hilfsfunktionen:
 
Funktion: fetch_html
--------------------
- Führt eine HTTP-GET-Anfrage an die angegebene URL (`link`) aus.
- Überprüft den HTTP-Statuscode, um sicherzustellen, dass die Anfrage erfolgreich war.
- Gibt den HTML-Quelltext der Seite als String zurück.

Parameter:
- session (aiohttp.ClientSession): Die HTTP-Sitzung, die für die Anfrage verwendet wird.
- link (str): Die URL, von der der HTML-Inhalt abgerufen werden soll.

Rückgabe:
- str: Der HTML-Quelltext der Seite.
"""
async def fetch_html(session, link):
    async with session.get(link) as response:
        response.raise_for_status()
        return await response.text()


"""
Funktion: perform_webscrape_async
---------------------------------
- Führt die gesamte Webscraping-Pipeline aus.
- Schritte:
  1. Füllt die Tabelle `library` mit statischen Daten (`prefill_db_with_static_data`).
  2. Verarbeitet Buch-Links und ruft zusätzliche Daten ab
     (`get_isbn_and_link_to_different_scrape_actions_async`).
"""
async def perform_webscrape_async(db_pool):
    await database.prefill_db_with_static_data(db_pool)
    await process_library_links_async(db_pool)




