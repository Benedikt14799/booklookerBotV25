import logging
logger = logging.getLogger(__name__)

async def create_table(db_pool):
    async with db_pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS sitesToScrape CASCADE")
        await conn.execute("DROP TABLE IF EXISTS library CASCADE")

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
                sitestoscrape_id INTEGER REFERENCES Sitestoscrape(id),
                Action VARCHAR(255),
                Custom_label_SKU VARCHAR(255),
                CategoryName INTEGER,
                Title VARCHAR(255),
                Relationship VARCHAR(255),
                RelationshipDetails VARCHAR(255),
                ISBN VARCHAR(255),
                EPID VARCHAR(255),
                Start_price NUMERIC,
                Margin NUMERIC,
                Quantity INTEGER,
                photo TEXT,
                VideoID VARCHAR(255),
                Condition_ID VARCHAR(255),
                Description TEXT,
                Format VARCHAR(255),
                Duration VARCHAR(255),
                Buy_It_Now_price NUMERIC,
                Best_Offer_Enabled INTEGER,
                Best_Offer_Auto_Accept_Price NUMERIC,
                Minimum_Best_Offer_Price NUMERIC,
                VAT_percent NUMERIC,
                Immediate_pay_required BOOLEAN,
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
                Ausgabe  TEXT,
                LinkToBL TEXT  
            );
        """)


"""
Funktion: prefill_db_with_static_data
-------------------------------------
- Aktualisiert die Tabelle `library` mit standardisierten statischen Daten.
- Setzt Werte wie `Action`, `CategoryName`, `Duration`, `Format`, etc. für alle Einträge.
- Verwendet eine direkte SQL-Update-Anweisung für eine effiziente Verarbeitung.
"""
async def prefill_db_with_static_data(db_pool):
    try:
        # Aktualisiert alle Einträge in der Tabelle `library` mit festen Werten
        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE library
                SET
                    Action = 'Add',
                    CategoryName = 261186,
                    Duration = 'GTC',
                    Format = 'FixedPrice',
                    Location = 78567,
                    Shipping_profile_name = 'Standartversand Bücher Deutschland',
                    Return_profile_name = 'Rückgabe für Bücher',
                    Payment_profile_name = 'Zahlung für Bücher',
                    Quantity = 1,
                    Best_Offer_Enabled = 1
            """)
    except Exception as e:
        # Fehlerbehandlung und Logging
        logger.error(f"Ein Fehler ist aufgetreten in prefill_db_with_static_data: {e}")


"""
Funktion: set_foreignkey
------------------------
- Setzt die Fremdschlüssel-Beziehung zwischen `sitetoscrape` und `library`.
- Ordnet Buch-Links aus `library` den entsprechenden Einträgen in `sitetoscrape` zu.
"""
async def set_foreignkey(db_pool):
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT numbersOfBooks, id FROM sitestoscrape")
        test = 0

        async with db_pool.acquire() as conn:
            for row in rows:
                counter = row['numbersofbooks']
                sitestoscrape_id = row['id']
                await conn.execute(
                    "UPDATE library SET sitestoscrape_id = $1 WHERE id BETWEEN $2 AND $3",
                    sitestoscrape_id, test + 1, test + counter
                )
                test += counter
    except Exception as e:
        logger.error(f"Ein Fehler ist aufgetreten in set_foreignkey: {e}")
