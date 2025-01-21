import asyncio
import logging
import re

import aiohttp
from bs4 import BeautifulSoup
import scrape
import price_processing

logger = logging.getLogger(__name__)

async def process_entry(session, link, num, db_pool):
    try:
        # HTML-Inhalt laden und parsen
        html_content = await scrape.fetch_html(session, link)
        soup = BeautifulSoup(html_content, "html.parser")
        properties = scrape.extract_properties(soup)

        # ISBN verarbeiten
        isbn = properties.get("ISBN:")
        if not isbn:
            logger.warning(f"Keine ISBN für Artikel {num} gefunden. Lösche den Datensatz.")
            # Datensatz löschen, falls keine ISBN vorhanden ist
            async with db_pool.acquire() as conn:
                await conn.execute("DELETE FROM library WHERE id = $1", num)
            return
        try:
            isbn = get_isbn(isbn)
            if not isbn:
                raise ValueError("Ungültige ISBN")

            # Wenn die ISBN gültig ist, schreibe sie in die Datenbank
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE library SET ISBN = $1 WHERE id = $2", isbn, num)

            logger.info(f"Verarbeitete und gespeicherte ISBN für Artikel {num}: {isbn}")

            # Preis berechnen und speichern
            # final_price = await price_processing.get_price(soup, num, db_pool)
            # if final_price:
            #     logger.info(f"Preis für Artikel {num} erfolgreich berechnet: {final_price}")


        except ValueError as ve:
            logger.warning(f"Ungültige ISBN für Artikel {num}: {ve}. Lösche den Datensatz.")
            # Datensatz löschen, falls die ISBN ungültig ist
            async with db_pool.acquire() as conn:
                await conn.execute("DELETE FROM library WHERE id = $1", num)
            return
    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten des Buches mit id {num}: {e}")

def get_isbn(isbn):
    """
    Extrahiert eine gültige ISBN-13 oder ISBN-10 aus einem gegebenen String.

    :param isbn: Der rohe ISBN-String.
    :return: Die bereinigte ISBN als String, oder None, wenn keine gültige ISBN gefunden wurde.
    """
    # Muster für ISBN-13 und ISBN-10
    isbn13_pattern = re.compile(r"\b(?:978|977)[0-9]{10}\b")
    isbn10_pattern = re.compile(r"\b[0-9]{9}[0-9X]\b")

    # Entferne Bindestriche und prüfe auf Muster
    clean_isbn = isbn.replace('-', '')
    match1 = isbn13_pattern.search(clean_isbn)
    match2 = isbn10_pattern.search(clean_isbn)

    # ISBN-13 priorisieren, ansonsten ISBN-10 zurückgeben
    if match1:
        return match1.group()
    elif match2:
        return match2.group()
    else:
        return None