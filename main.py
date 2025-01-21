import asyncio
import logging
import time

import openpyxl
import asyncpg
import pandas as pd  # Neu hinzugefügt

import database
import scrape

# Logging konfigurieren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Asynchrone Hauptfunktion
async def main():
    start_time = time.time()

    # Verbindung zur Datenbank herstellen
    db_pool = await asyncpg.create_pool(
        host="localhost",
        database="postgres",
        user="postgres",
        password="1204",
        port=5432
    )

    # Links aus der Excel-Datei einlesen
    try:
        df_links = pd.read_excel('links.xlsx', header=None)  # Excel-Datei einlesen
        links_to_scrape = df_links.iloc[:, 0].dropna().tolist()  # Erste Spalte in Liste konvertieren und leere Zellen entfernen
        if not links_to_scrape:
            logger.error("Die Excel-Datei enthält keine Links in der ersten Spalte.")
            return
        logger.info(f"{len(links_to_scrape)} Links wurden aus der Excel-Datei eingelesen.")
    except Exception as e:
        logger.error(f"Fehler beim Einlesen der Excel-Datei: {e}")
        return

    # Tabelle erstellen
    await database.create_table(db_pool)

    # Datenbank mit Links füllen
    await scrape.insert_links_into_sitetoscrape(links_to_scrape, db_pool)
    await scrape.scrape_and_save_pages(db_pool)
    await scrape.perform_webscrape_async(db_pool)


    # Datenbankverbindung schließen
    await db_pool.close()

    end_time = time.time()
    execution_time = end_time - start_time
    logger.info("Die Ausführungszeit beträgt: {:.2f} Sekunden".format(execution_time))

# Hauptprogramm starten
if __name__ == "__main__":
    asyncio.run(main())