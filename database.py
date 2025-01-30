import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Klasse: DatabaseManager
    ------------------------
    Verwaltet die Datenbankerstellung und das Einfügen neuer Scraping-Daten.
    Ermöglicht eine Benutzereingabe für die Kategorie.
    """

    @staticmethod
    async def create_table(db_pool):
        """
        Erstellt die benötigten Tabellen, falls sie noch nicht existieren.
        Bestehende Tabellen bleiben erhalten, damit die IDs fortlaufend bleiben.
        """
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sitestoscrape (
                    id SERIAL PRIMARY KEY,
                    link TEXT UNIQUE NOT NULL,
                    anzahlSeiten INTEGER,
                    numbersOfBooks INTEGER
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS library (
                    id SERIAL PRIMARY KEY,
                    sitestoscrape_id INTEGER REFERENCES sitestoscrape(id),
                    Action VARCHAR(255),
                    Custom_label_SKU VARCHAR(255),
                    CategoryID INTEGER,
                    CategoryName VARCHAR(255),
                    Title VARCHAR(255),
                    Relationship VARCHAR(255),
                    RelationshipDetails VARCHAR(255),
                    ISBN VARCHAR(255),
                    EPID VARCHAR(255),
                    Start_price NUMERIC,
                    Margin NUMERIC,
                    Quantity INTEGER DEFAULT 1,
                    photo TEXT,
                    VideoID VARCHAR(255),
                    Condition_ID VARCHAR(255),
                    Description TEXT,
                    Format VARCHAR(255),
                    Duration VARCHAR(255),
                    Buy_It_Now_price NUMERIC,
                    Best_Offer_Enabled INTEGER DEFAULT 1,
                    Best_Offer_Auto_Accept_Price NUMERIC,
                    Minimum_Best_Offer_Price NUMERIC,
                    VAT_percent NUMERIC,
                    Immediate_pay_required BOOLEAN DEFAULT FALSE,
                    Location VARCHAR(255),
                    Shipping_service_1_option VARCHAR(255),
                    Shipping_service_1_cost NUMERIC,
                    Shipping_service_1_priority INTEGER,
                    Shipping_service_2_option VARCHAR(255),
                    Shipping_service_2_cost NUMERIC,
                    Shipping_service_2_priority INTEGER,
                    Max_dispatch_time VARCHAR(255),
                    Returns_accepted_option VARCHAR(255),
                    Returns_within_option VARCHAR(255),
                    Refund_option VARCHAR(255),
                    Return_shipping_cost_paid_by VARCHAR(255),
                    Shipping_profile_name VARCHAR(255),
                    Return_profile_name VARCHAR(255),
                    Payment_profile_name VARCHAR(255),
                    ProductCompliancePolicyID VARCHAR(255),
                    Regional_ProductCompliancePolicies VARCHAR(255),
                    EconomicOperator_CompanyName VARCHAR(255),
                    EconomicOperator_AddressLine1 VARCHAR(255),
                    EconomicOperator_AddressLine2 VARCHAR(255),
                    EconomicOperator_City VARCHAR(255),
                    EconomicOperator_Country VARCHAR(255),
                    EconomicOperator_PostalCode VARCHAR(255),
                    EconomicOperator_StateOrProvince VARCHAR(255),
                    EconomicOperator_Phone VARCHAR(255),
                    EconomicOperator_Email VARCHAR(255),
                    Autor VARCHAR(255),
                    Buchtitel TEXT,
                    Sprache VARCHAR(255),
                    Thematik TEXT,
                    Buchreihe TEXT,
                    Genre TEXT,
                    Verlag TEXT,
                    Erscheinungsjahr VARCHAR(255),
                    CFormat VARCHAR(255),
                    Originalsprache VARCHAR(255),
                    Herstellungsland_und_region VARCHAR(255),
                    Produktart TEXT,
                    Literarische_Gattung TEXT,
                    Zielgruppe TEXT,
                    Signiert_von VARCHAR(255),
                    Literarische_Bewegung TEXT,
                    Ausgabe TEXT,
                    LinkToBL TEXT
                );
            """)

    @staticmethod
    async def insert_library_entry(db_pool, properties: dict):
        """
        Fügt neue Bücher in die `library`-Tabelle ein.
        Jedes neue Scraping-Ergebnis wird als neuer Eintrag gespeichert.
        """
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO library 
                    (Autor, Buchtitel, Sprache, Thematik, Verlag, Erscheinungsjahr, 
                     CFormat, Produktart, Ausgabe, Description) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                                   properties.get("Autor", ""),
                                   properties.get("Buchtitel", ""),
                                   properties.get("Sprache", ""),
                                   properties.get("Thematik", ""),
                                   properties.get("Verlag", ""),
                                   properties.get("Erscheinungsjahr", ""),
                                   properties.get("CFormat", ""),
                                   properties.get("Produktart", ""),
                                   properties.get("Ausgabe", ""),
                                   properties.get("Description", "")
                                   )
                logger.info(f"Neu hinzugefügt: {properties.get('Buchtitel')} von {properties.get('Autor')}")
        except Exception as e:
            logger.error(f"Fehler beim Einfügen von {properties.get('Buchtitel')}: {e}")

    @staticmethod
    async def set_foreignkey(db_pool):
        """
        Setzt den Fremdschlüssel `sitetoscrape_id` in der `library`-Tabelle,
        indem die IDs von `sitetoscrape` auf die passenden Bücher gesetzt werden.
        """
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT id FROM sitestoscrape")

                for row in rows:
                    sitestoscrape_id = row["id"]

                    # Setzt sitestoscrape_id nur, wenn es noch nicht gesetzt ist
                    await conn.execute("""
                        UPDATE library 
                        SET sitestoscrape_id = $1 
                        WHERE sitestoscrape_id IS NULL
                    """, sitestoscrape_id)

            logger.info("Fremdschlüssel-Zuordnung abgeschlossen.")
        except Exception as e:
            logger.error(f"Fehler in set_foreignkey: {e}")

    @staticmethod
    async def prefill_db_with_static_data(db_pool):
        """
        Füllt die `library`-Tabelle mit Standardwerten für bestimmte Spalten.
        Bezieht `CategoryName` als Eingabe vom Benutzer.
        """
        try:
            # Benutzereingabe für die Kategorie
            category_name = input("Bitte geben Sie den Category Name ein: ").strip()

            # Falls leer, Standardwert setzen
            if not category_name:
                category_name = "/Bücher & Zeitschriften/Bücher"

            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE library
                    SET
                        Action = 'Add',
                        CategoryID = 261186,
                        CategoryName = $1,
                        Duration = 'GTC',
                        Format = 'FixedPrice',
                        Location = 78567,
                        Shipping_profile_name = 'Standardversand Bücher Deutschland',
                        Return_profile_name = 'Rückgabe für Bücher',
                        Payment_profile_name = 'Zahlung für Bücher',
                        Quantity = 1,
                        Best_Offer_Enabled = 1
                """, category_name)
                logger.info(f"Statische Standardwerte wurden erfolgreich eingefügt. Category Name: {category_name}")
        except Exception as e:
            logger.error(f"Fehler in prefill_db_with_static_data: {e}")
